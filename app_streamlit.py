"""
Streamlit UI for Policy Reviewer Textract Pipeline (Lambda-only)

What it does
------------
1) Uploads a PDF to s3://{S3_BUCKET}/{POLICY_PDF_PREFIX}<filename>.
2) Your Lambda pipeline (S3 event -> Ingest Lambda -> Textract -> SNS -> Callback Lambda)
   persists a manifest at {POLICY_OUTPUT_PREFIX}/<timestamp>/<job_id>/index.json
   plus per-page Textract responses under /pages/page_*.json.
3) This app polls for that manifest (matching 'source_key') and renders:
   - Combined text (LINE blocks)
   - Key-Value pairs (FORMS)
   - Tables (TABLES), each as a DataFrame with CSV download

Environment (set in shell or .env for local dev)
-------------------------------------------------
AWS_REGION=us-east-1
S3_BUCKET=policyreviewer-bucket
POLICY_PDF_PREFIX=policy/pdf/
POLICY_OUTPUT_PREFIX=policy/textract-output/

Local run
---------
uv add streamlit boto3 python-dotenv pandas
uv run streamlit run app_streamlit.py
"""

from __future__ import annotations

import io
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
import botocore
import pandas as pd
import streamlit as st


# ---------- Configuration ----------

def get_cfg() -> Dict[str, str]:
    # (Optional) from dotenv import load_dotenv; load_dotenv()
    cfg = {
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
        # FIXED DEFAULT: your bucket name is lowercase "policyreviewer-bucket"
        "S3_BUCKET": os.getenv("S3_BUCKET", "policyreviewer-bucket"),
        "POLICY_PDF_PREFIX": os.getenv("POLICY_PDF_PREFIX", "policy/pdf/"),
        "POLICY_OUTPUT_PREFIX": os.getenv("POLICY_OUTPUT_PREFIX", "policy/textract-output/"),
    }
    if not cfg["POLICY_PDF_PREFIX"].endswith("/"):
        cfg["POLICY_PDF_PREFIX"] += "/"
    if not cfg["POLICY_OUTPUT_PREFIX"].endswith("/"):
        cfg["POLICY_OUTPUT_PREFIX"] += "/"
    return cfg


def s3_client(region: str):
    return boto3.client("s3", region_name=region)


def bucket_exists(s3, bucket: str) -> bool:
    try:
        s3.head_bucket(Bucket=bucket)
        return True
    except botocore.exceptions.ClientError as e:
        # 404 / 403 / region mismatch will land here
        return False


# ---------- S3 helpers ----------

def upload_pdf_to_s3(s3, bucket: str, key: str, file_bytes: bytes) -> str:
    s3.put_object(Bucket=bucket, Key=key, Body=file_bytes, ContentType="application/pdf")
    return f"s3://{bucket}/{key}"


def list_recent_objects(s3, bucket: str, prefix: str, max_keys: int = 400) -> List[Dict[str, Any]]:
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)
    contents = resp.get("Contents", [])
    contents.sort(key=lambda x: x["LastModified"], reverse=True)
    return contents


def get_object_text(s3, bucket: str, key: str) -> str:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")


def get_object_bytes(s3, bucket: str, key: str) -> bytes:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


# ---------- Manifest lookup ----------

def find_matching_manifest(
    s3,
    bucket: str,
    output_prefix: str,
    source_key: str,
    scan_limit: int = 800,
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Scan recent index.json objects under the output prefix and return the first whose
    'source_key' matches the just-uploaded source_key.
    """
    objects = list_recent_objects(s3, bucket, output_prefix, max_keys=scan_limit)
    for obj in objects:
        key = obj["Key"]
        if not key.endswith("index.json"):
            continue
        try:
            manifest_json = json.loads(get_object_text(s3, bucket, key))
            if manifest_json.get("source_key") == source_key:
                return key, manifest_json
        except Exception:
            pass
    return None


# ---------- Textract parsing ----------

def _block_map(blocks: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {b["Id"]: b for b in blocks if "Id" in b}


def _text_from_block(block: Dict[str, Any], id_to_block: Dict[str, Dict[str, Any]]) -> str:
    """
    Collect text from a block by walking CHILD relationships to WORD/SELECTION_ELEMENT.
    """
    texts: List[str] = []
    for rel in block.get("Relationships", []) or []:
        if rel.get("Type") == "CHILD":
            for cid in rel.get("Ids", []):
                child = id_to_block.get(cid)
                if not child:
                    continue
                bt = child.get("BlockType")
                if bt == "WORD" and child.get("Text"):
                    texts.append(child["Text"])
                elif bt == "SELECTION_ELEMENT":
                    # Textract marks selected checkboxes
                    if child.get("SelectionStatus") == "SELECTED":
                        texts.append("[X]")
                    else:
                        texts.append("[ ]")
    return " ".join(texts).strip()


def parse_kv_pairs(all_blocks: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    """
    Extract (key, value) pairs from KEY_VALUE_SET blocks (Analysis mode).
    """
    id_map = _block_map(all_blocks)
    keys: List[Dict[str, Any]] = [
        b for b in all_blocks if b.get("BlockType") == "KEY_VALUE_SET" and "KEY" in (b.get("EntityTypes") or [])
    ]
    values: List[Dict[str, Any]] = [
        b for b in all_blocks if b.get("BlockType") == "KEY_VALUE_SET" and "VALUE" in (b.get("EntityTypes") or [])
    ]
    # Index VALUE blocks so we can map from KEY via VALUE relationships
    value_by_id = {b["Id"]: b for b in values if "Id" in b}

    pairs: List[Tuple[str, str]] = []
    for k in keys:
        key_text = _text_from_block(k, id_map)
        # find the VALUE block linked from this KEY
        val_text = ""
        for rel in k.get("Relationships", []) or []:
            if rel.get("Type") == "VALUE":
                for vid in rel.get("Ids", []):
                    vblock = value_by_id.get(vid)
                    if vblock:
                        val_text = _text_from_block(vblock, id_map)
                        break
        if key_text or val_text:
            pairs.append((key_text, val_text))
    # de-dup while preserving order (some docs repeat)
    seen = set()
    uniq: List[Tuple[str, str]] = []
    for k, v in pairs:
        sig = (k, v)
        if sig in seen:
            continue
        seen.add(sig)
        uniq.append((k, v))
    return uniq


def parse_tables(all_blocks: List[Dict[str, Any]]) -> List[pd.DataFrame]:
    """
    Build table DataFrames from TABLE/CELL hierarchy.
    """
    id_map = _block_map(all_blocks)
    tables = [b for b in all_blocks if b.get("BlockType") == "TABLE"]
    dataframes: List[pd.DataFrame] = []

    for tbl in tables:
        # gather CELL ids from TABLE's CHILD rels
        cell_ids: List[str] = []
        for rel in tbl.get("Relationships", []) or []:
            if rel.get("Type") == "CHILD":
                cell_ids.extend(rel.get("Ids", []))

        cells = [id_map[cid] for cid in cell_ids if id_map.get(cid) and id_map[cid].get("BlockType") == "CELL"]
        if not cells:
            continue

        max_row = max(c.get("RowIndex", 0) for c in cells)
        max_col = max(c.get("ColumnIndex", 0) for c in cells)

        # Initialize grid with empty strings
        grid: List[List[str]] = [["" for _ in range(max_col)] for _ in range(max_row)]

        for c in cells:
            r = c.get("RowIndex", 1) - 1
            cidx = c.get("ColumnIndex", 1) - 1
            txt = _text_from_block(c, id_map)
            # rudimentary merge handling: if the spot already has text, append
            if grid[r][cidx]:
                grid[r][cidx] = (grid[r][cidx] + " | " + txt).strip()
            else:
                grid[r][cidx] = txt

        df = pd.DataFrame(grid)
        # Optional: treat first row as header if it looks like one
        if df.shape[0] > 1 and any(len(str(x)) > 0 for x in df.iloc[0].tolist()):
            df.columns = [c if c != "" else f"Col{i+1}" for i, c in enumerate(df.iloc[0])]
            df = df[1:].reset_index(drop=True)

        dataframes.append(df)

    return dataframes


def extract_lines(all_blocks: List[Dict[str, Any]]) -> str:
    """
    Combined text from LINE blocks (works for both TextDetection and Analysis).
    """
    lines = [b.get("Text", "") for b in all_blocks if b.get("BlockType") == "LINE" and b.get("Text")]
    return "\n".join(lines)


# ---------- Loader for all page JSONs ----------

def load_all_blocks_from_pages(s3, page_uris: List[str]) -> List[Dict[str, Any]]:
    """
    Given s3:// URIs for per-call Textract responses, merge all `Blocks` arrays.
    """
    blocks: List[Dict[str, Any]] = []
    for uri in page_uris:
        if not uri.startswith("s3://"):
            continue
        _, _, rest = uri.partition("s3://")
        bkt, _, key = rest.partition("/")
        raw = get_object_text(s3, bkt, key)
        js = json.loads(raw)
        for b in js.get("Blocks", []) or []:
            blocks.append(b)
    return blocks


# ---------- UI ----------

st.set_page_config(page_title="Policy Reviewer - Textract Ingestion", layout="wide")
st.title("📄 Policy Reviewer – Textract Ingestion (Lambda-only)")
st.caption("Upload a PDF → S3. Your Lambda pipeline runs automatically. Results will appear below.")

cfg = get_cfg()
s3 = s3_client(cfg["AWS_REGION"])

with st.sidebar:
    st.header("Configuration")
    st.text_input("AWS Region", value=cfg["AWS_REGION"], disabled=True)
    st.text_input("S3 Bucket", value=cfg["S3_BUCKET"], disabled=True)
    st.text_input("PDF Prefix", value=cfg["POLICY_PDF_PREFIX"], disabled=True)
    st.text_input("Output Prefix", value=cfg["POLICY_OUTPUT_PREFIX"], disabled=True)
    # Pre-flight bucket check
    ok = bucket_exists(s3, cfg["S3_BUCKET"])
    if ok:
        st.success("Bucket reachable ✓")
    else:
        st.error(
            "Bucket not found or not reachable. "
            "Verify the name/region and your credentials. "
            f"Expected: s3://{cfg['S3_BUCKET']}"
        )

st.subheader("1) Upload PDF → S3")
uploaded = st.file_uploader("Upload policy PDF", type=["pdf"])

if "upload_info" not in st.session_state:
    st.session_state["upload_info"] = None  # dict with {key, s3_uri, started_at}

col1, col2 = st.columns([1, 1], vertical_alignment="center")

with col1:
    if uploaded is not None and st.button("Upload to S3 & Start Pipeline", type="primary", use_container_width=True):
        if not bucket_exists(s3, cfg["S3_BUCKET"]):
            st.error(f"Bucket '{cfg['S3_BUCKET']}' not found. Fix the bucket name/region and retry.")
        else:
            filename = uploaded.name
            object_key = f"{cfg['POLICY_PDF_PREFIX']}{filename}"
            try:
                file_bytes = uploaded.read()
                s3_uri = upload_pdf_to_s3(s3, cfg["S3_BUCKET"], object_key, file_bytes)
                st.session_state["upload_info"] = {
                    "key": object_key,
                    "s3_uri": s3_uri,
                    "started_at": datetime.now(timezone.utc).isoformat(),
                }
                st.success(f"Uploaded to {s3_uri}")
                st.info("Your Lambda ingest will now run automatically (via S3 event).")
            except botocore.exceptions.ClientError as e:
                code = e.response.get("Error", {}).get("Code")
                if code == "NoSuchBucket":
                    st.error(
                        "S3 PutObject failed: NoSuchBucket. "
                        f"Check that '{cfg['S3_BUCKET']}' exists in region {cfg['AWS_REGION']} "
                        "and your AWS credentials are correct."
                    )
                else:
                    st.exception(e)

with col2:
    if st.session_state["upload_info"]:
        if st.button("Reset", use_container_width=True):
            st.session_state["upload_info"] = None

st.divider()
st.subheader("2) Wait for Results → View Output")

if st.session_state["upload_info"]:
    source_key = st.session_state["upload_info"]["key"]
    st.write(f"Watching for **index.json** with `source_key={source_key}`")

    max_wait = st.slider("Max wait (seconds)", 10, 600, 180, 10)
    poll_interval = st.slider("Poll interval (seconds)", 2, 20, 5, 1)

    placeholder = st.empty()
    result_container = st.container()

    start_ts = time.time()
    found: Optional[Tuple[str, Dict[str, Any]]] = None

    with st.spinner("Polling for Textract results…"):
        while time.time() - start_ts < max_wait:
            elapsed = int(time.time() - start_ts)
            placeholder.info(f"⏳ Elapsed: {elapsed}s — checking for manifest…")
            found = find_matching_manifest(
                s3=s3,
                bucket=cfg["S3_BUCKET"],
                output_prefix=cfg["POLICY_OUTPUT_PREFIX"],
                source_key=source_key,
                scan_limit=800,
            )
            if found:
                break
            time.sleep(poll_interval)

    if not found:
        placeholder.warning(
            "No manifest found yet. Textract may still be processing. "
            "Try increasing the wait time or check CloudWatch logs for the Lambdas."
        )
    else:
        manifest_key, manifest = found
        placeholder.empty()
        with result_container:
            st.success("Textract output found!")
            st.caption(f"Manifest: `s3://{cfg['S3_BUCKET']}/{manifest_key}`")
            st.code(json.dumps(manifest, indent=2), language="json")

            page_uris: List[str] = manifest.get("pages", [])
            if not page_uris:
                st.warning("Manifest has no 'pages'. Check your callback Lambda persistence.")
            else:
                # Load all blocks across pages once
                with st.spinner("Loading and parsing Textract pages…"):
                    all_blocks = load_all_blocks_from_pages(s3, page_uris)
                st.write(
                    f"**Blocks loaded:** {len(all_blocks)} "
                    f"(LINES, WORDS, TABLES, FORMS, etc.)"
                )

                # Tabs for different views
                tab_text, tab_kv, tab_tables, tab_raw = st.tabs(
                    ["📝 Combined Text", "🔑 Key–Value Pairs", "📊 Tables", "🧩 Raw Page JSON"]
                )

                with tab_text:
                    text = extract_lines(all_blocks)
                    st.text_area("Detected Text (LINE blocks)", value=text, height=420)
                    st.download_button(
                        "Download Text (.txt)",
                        data=text.encode("utf-8"),
                        file_name="textract_text.txt",
                        mime="text/plain",
                    )

                with tab_kv:
                    kv_pairs = parse_kv_pairs(all_blocks)
                    if not kv_pairs:
                        st.info("No KEY_VALUE_SET blocks detected (this is expected for Text Detection jobs).")
                    else:
                        df_kv = pd.DataFrame(kv_pairs, columns=["Key", "Value"])
                        st.dataframe(df_kv, use_container_width=True)
                        csv = df_kv.to_csv(index=False).encode("utf-8")
                        st.download_button("Download KV CSV", data=csv, file_name="textract_kv.csv", mime="text/csv")

                with tab_tables:
                    tables = parse_tables(all_blocks)
                    if not tables:
                        st.info("No TABLES detected. Use StartDocumentAnalysis(FORMS,TABLES) to extract tables.")
                    else:
                        for i, df in enumerate(tables, start=1):
                            with st.expander(f"Table {i}", expanded=(i == 1)):
                                st.dataframe(df, use_container_width=True, height=420)
                                csv = df.to_csv(index=False).encode("utf-8")
                                st.download_button(
                                    f"Download Table {i} CSV",
                                    data=csv,
                                    file_name=f"textract_table_{i}.csv",
                                    mime="text/csv",
                                )

                with tab_raw:
                    for i, uri in enumerate(page_uris, start=1):
                        with st.expander(f"Page JSON {i} – {uri}", expanded=False):
                            _, _, rest = uri.partition("s3://")
                            bkt, _, key = rest.partition("/")
                            raw = get_object_text(s3, bkt, key)
                            st.code(raw, language="json")
else:
    st.info("Upload a PDF and click **Upload to S3 & Start Pipeline** to begin.")

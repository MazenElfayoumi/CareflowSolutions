"""
Unified Streamlit reconciliation hub.

Run with:
    streamlit run streamlit_app.py

Users can choose either Transaction Matcher or Bill Reconciler.
"""

from __future__ import annotations

from pathlib import Path
import tempfile

import streamlit as st

from streamlit_bill_app import render_bill_reconciler_ui
from transaction_matcher_app import TransactionMatcher


SUPPORTED_TYPES = ["csv", "xlsx", "xls"]
APP_PASSWORD = "admin123"


def _require_password() -> bool:
    if st.session_state.get("app_authenticated", False):
        return True

    st.title("Careflow Reconciliation Hub")
    st.caption("Enter password to continue.")
    password = st.text_input("Password", type="password", key="app_password_input")
    login_clicked = st.button("Unlock", type="primary", key="app_password_button")

    if login_clicked:
        if password == APP_PASSWORD:
            st.session_state["app_authenticated"] = True
            st.success("Access granted.")
            st.rerun()
        else:
            st.error("Invalid password.")

    return False


def _save_uploaded_file(uploaded_file, target_dir: Path, fallback_stem: str) -> Path:
    suffix = Path(uploaded_file.name).suffix or ".csv"
    safe_name = Path(uploaded_file.name).name or f"{fallback_stem}{suffix}"
    path = target_dir / safe_name
    path.write_bytes(uploaded_file.getvalue())
    return path


def _run_match(
    chase1_file,
    collections_file,
    chase2_file=None,
    name_mapping_file=None,
    output_dir: str = "",
):
    with tempfile.TemporaryDirectory(prefix="transaction_matcher_") as tmp:
        tmp_dir = Path(tmp)

        chase1_path = _save_uploaded_file(chase1_file, tmp_dir, "chase1")
        collections_path = _save_uploaded_file(collections_file, tmp_dir, "collections")
        chase2_path = _save_uploaded_file(chase2_file, tmp_dir, "chase2") if chase2_file else None
        name_map_path = (
            _save_uploaded_file(name_mapping_file, tmp_dir, "name_mapping") if name_mapping_file else None
        )

        if output_dir.strip():
            out_dir_path = Path(output_dir.strip()).expanduser()
            out_dir_path.mkdir(parents=True, exist_ok=True)
        else:
            out_dir_path = tmp_dir / "reports"
            out_dir_path.mkdir(parents=True, exist_ok=True)

        matcher = TransactionMatcher(
            chase_file=str(chase1_path),
            collections_file=str(collections_path),
            second_chase_file=str(chase2_path) if chase2_path else None,
            name_mapping_file=str(name_map_path) if name_map_path else None,
        )
        matcher.load_files()
        matcher.match_transactions()
        report_path = matcher.generate_report(str(out_dir_path))
        unmatched_path = out_dir_path / "unmatched_transactions.xlsx"

        result = {
            "matched": len(matcher.matches),
            "unmatched_chase": len(matcher.unmatched_chase),
            "unmatched_collections": len(matcher.unmatched_collections),
            "total_chase": len(matcher.chase_df),
            "total_collections": len(matcher.collections_df),
            "output_dir": str(report_path.parent),
            "report_bytes": report_path.read_bytes(),
            "unmatched_bytes": unmatched_path.read_bytes(),
        }
        return result


def render_transaction_matcher_ui() -> None:
    st.title("Transaction Matcher")
    st.caption("Chase to Collections reconciliation with the existing matching engine.")

    with st.sidebar:
        st.header("Inputs")
        chase1_file = st.file_uploader("Chase File 1 (required)", type=SUPPORTED_TYPES, key="tm_chase1")
        chase2_file = st.file_uploader("Chase File 2 (optional)", type=SUPPORTED_TYPES, key="tm_chase2")
        collections_file = st.file_uploader(
            "Collections File (required)",
            type=SUPPORTED_TYPES,
            key="tm_collections",
        )
        name_mapping_file = st.file_uploader(
            "Name Mapping CSV (optional)",
            type=["csv"],
            key="tm_mapping",
        )
        output_dir = st.text_input(
            "Output Folder (optional)",
            value="",
            key="tm_output_dir",
            help="If provided, reports are also saved to this server path.",
        )

        run_clicked = st.button("Run Matching", type="primary", use_container_width=True, key="tm_run")

    if run_clicked:
        if not chase1_file or not collections_file:
            st.error("Chase File 1 and Collections File are required.")
        else:
            with st.spinner("Matching transactions..."):
                try:
                    st.session_state["tm_match_result"] = _run_match(
                        chase1_file=chase1_file,
                        collections_file=collections_file,
                        chase2_file=chase2_file,
                        name_mapping_file=name_mapping_file,
                        output_dir=output_dir,
                    )
                    st.success("Matching complete.")
                except Exception as exc:
                    st.error(f"Error: {exc}")

    result = st.session_state.get("tm_match_result")
    if not result:
        st.info("Upload files and click Run Matching to generate reports.")
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("Matched", result["matched"])
    col2.metric("Unmatched Chase", result["unmatched_chase"])
    col3.metric("Unmatched Collections", result["unmatched_collections"])

    total_collections = result["total_collections"]
    if total_collections > 0:
        match_rate = (result["matched"] / total_collections) * 100
        st.metric("Match Rate", f"{match_rate:.2f}%")
    else:
        st.metric("Match Rate", "N/A")

    st.write(f"Reports saved to: `{result['output_dir']}`")

    dl_col1, dl_col2 = st.columns(2)
    dl_col1.download_button(
        "Download transaction_matching_report.xlsx",
        data=result["report_bytes"],
        file_name="transaction_matching_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key="tm_download_report",
    )
    dl_col2.download_button(
        "Download unmatched_transactions.xlsx",
        data=result["unmatched_bytes"],
        file_name="unmatched_transactions.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key="tm_download_unmatched",
    )


def main() -> None:
    st.set_page_config(page_title="Careflow Reconciliation Hub", layout="wide")

    if not _require_password():
        return

    selected_tool = st.radio(
        "Select Tool",
        options=["Transaction Matcher", "Bill Reconciler"],
        horizontal=True,
        index=0,
    )

    if selected_tool == "Transaction Matcher":
        render_transaction_matcher_ui()
    else:
        render_bill_reconciler_ui()


if __name__ == "__main__":
    main()

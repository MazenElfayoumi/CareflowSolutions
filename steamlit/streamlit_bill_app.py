import streamlit as st
import tempfile
from pathlib import Path
from bill_reconciler_app import reconcile_files, save_report

st.set_page_config(page_title="Bill Reconciler", layout="wide")

st.title("Bill Reconciler")
st.caption("Reconcile bills between Bank (Check Details) PDF and Website (Electronic Remittance Advice) PDF.")

with st.sidebar:
    st.header("Inputs")
    bank_file = st.file_uploader("Insurance File ", type=["pdf"], key="bank")
    web_file = st.file_uploader("Prompt File ", type=["pdf"], key="web")
    
    if "folder_path" not in st.session_state:
        st.session_state["folder_path"] = ""
        
    output_dir = st.text_input(
        "Output Folder (server path)",
        value=st.session_state["folder_path"],
        help="Optional. Saves report on the app server. Leave blank and use Download to save on your device."
    )
    st.caption("You could optionally put selected folder path here before running the reconciliation or simply run without it and use Download.")

    run_btn = st.button("Reconcile Files", type="primary", use_container_width=True)

if run_btn:
    if not bank_file or not web_file:
        st.error("Both Bank and Website files are required.")
    else:
        with st.spinner("Extracting and reconciling data..."):
            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir)
                
                # Save uploads to temp files
                bank_tmp = tmp_path / bank_file.name
                web_tmp = tmp_path / web_file.name
                
                bank_tmp.write_bytes(bank_file.getvalue())
                web_tmp.write_bytes(web_file.getvalue())
                
                try:
                    # Logging function for Streamlit
                    log_msgs = []
                    def st_log(msg):
                        log_msgs.append(msg)
                    
                    df_report, stats = reconcile_files(str(web_tmp), str(bank_tmp), log_fn=st_log)
                    
                    st.success("Reconciliation complete!")
                    
                    # Prepare output directory
                    if output_dir.strip():
                        out_path = Path(output_dir.strip()).expanduser()
                        out_path.mkdir(parents=True, exist_ok=True)
                        target_file = out_path / "Reconciliation_Final.xlsx"
                    else:
                        target_file = tmp_path / "Reconciliation_Final.xlsx"
                    
                    # Save local
                    saved_path = save_report(df_report, str(target_file))
                    
                    st.session_state["br_report_bytes"] = Path(saved_path).read_bytes()
                    st.session_state["br_stats"] = stats
                    st.session_state["br_report_path"] = saved_path
                    
                except Exception as e:
                    st.error(f"Error during reconciliation: {e}")

if "br_stats" in st.session_state:
    stats = st.session_state["br_stats"]
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Bank Count", stats.get("bank_count", 0))
    col2.metric("Web Count", stats.get("web_count", 0))
    col3.metric("Matched", stats.get("matched", 0))
    col4.metric("Mismatched", stats.get("mismatched", 0))
    
    st.write(f"Missing in Bank: **{stats.get('missing_bank', 0)}** | Missing in Website: **{stats.get('missing_web', 0)}**")
    
    st.download_button(
        label="Download Reconciliation Report (Excel)",
        data=st.session_state["br_report_bytes"],
        file_name="Reconciliation_Final.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )
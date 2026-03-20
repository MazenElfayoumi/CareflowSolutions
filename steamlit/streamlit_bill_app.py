import streamlit as st
import tempfile
from pathlib import Path
from bill_reconciler_app import reconcile_files, save_report

APP_PASSWORD = "admin123"


def _require_password() -> bool:
    if st.session_state.get("app_authenticated", False):
        return True

    st.title("Bill Reconciler")
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

def render_bill_reconciler_ui() -> None:
    st.title("Bill Reconciler")
    st.caption(
        "Reconcile bills between Bank (Check Details) PDF and Website "
        "(Electronic Remittance Advice) PDF."
    )

    if "br_folder_path" not in st.session_state:
        st.session_state["br_folder_path"] = ""

    with st.sidebar:
        st.header("Inputs")
        bank_file = st.file_uploader(
            "Bank File (Check Details PDF)",
            type=["pdf"],
            key="br_bank",
        )
        web_file = st.file_uploader(
            "Website File (Remittance Advice PDF)",
            type=["pdf"],
            key="br_web",
        )

        output_dir = st.text_input(
            "Output Folder (server path)",
            value=st.session_state["br_folder_path"],
            key="br_output_dir",
            help=(
                "Optional. Saves report on the app server. Leave blank and use "
                "Download to save on your device."
            ),
        )
        st.session_state["br_folder_path"] = output_dir
        st.caption(
            "You can provide a server path before running or leave it blank "
            "and download the file."
        )

        run_btn = st.button("Reconcile Files", type="primary", use_container_width=True, key="br_run")

    if run_btn:
        if not bank_file or not web_file:
            st.error("Both Bank and Website files are required.")
        else:
            with st.spinner("Extracting and reconciling data..."):
                with tempfile.TemporaryDirectory() as tmp_dir:
                    tmp_path = Path(tmp_dir)

                    bank_tmp = tmp_path / bank_file.name
                    web_tmp = tmp_path / web_file.name

                    bank_tmp.write_bytes(bank_file.getvalue())
                    web_tmp.write_bytes(web_file.getvalue())

                    try:
                        log_msgs = []

                        def st_log(msg):
                            log_msgs.append(msg)

                        df_report, stats = reconcile_files(str(web_tmp), str(bank_tmp), log_fn=st_log)

                        st.success("Reconciliation complete!")

                        if output_dir.strip():
                            out_path = Path(output_dir.strip()).expanduser()
                            out_path.mkdir(parents=True, exist_ok=True)
                            target_file = out_path / "Reconciliation_Final.xlsx"
                        else:
                            target_file = tmp_path / "Reconciliation_Final.xlsx"

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

        st.write(
            f"Missing in Bank: **{stats.get('missing_bank', 0)}** | "
            f"Missing in Website: **{stats.get('missing_web', 0)}**"
        )

        st.download_button(
            label="Download Reconciliation Report (Excel)",
            data=st.session_state["br_report_bytes"],
            file_name="Reconciliation_Final.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="br_download",
        )


def main() -> None:
    st.set_page_config(page_title="Bill Reconciler", layout="wide")

    if not _require_password():
        return

    render_bill_reconciler_ui()


if __name__ == "__main__":
    main()
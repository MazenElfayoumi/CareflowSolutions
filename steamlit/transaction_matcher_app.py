"""
Transaction Matcher - Core matcher and CLI entrypoint.

Use Streamlit UI via:
    streamlit run streamlit_app.py

Use CLI via:
    python transaction_matcher_app.py --chase1 <path> --collections <path> [--chase2 <path>] [--name-mapping <path>] [--output <path>]
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import List
import argparse
import difflib
import logging
import re

import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class TransactionMatcher:
    """Matches transactions between one or two Chase bank files and the Collections report."""

    def __init__(
        self,
        chase_file: str,
        collections_file: str,
        second_chase_file: str | None = None,
        name_mapping_file: str | None = None,
    ):
        self.chase_file = Path(chase_file)
        self.collections_file = Path(collections_file)
        self.report_file = Path(second_chase_file) if second_chase_file else None
        self.name_mapping_file = Path(name_mapping_file) if name_mapping_file else None
        self.chase_df = None
        self.collections_df = None
        self.matches = []
        self.unmatched_chase = []
        self.unmatched_report = []
        self.unmatched_collections = []
        self.name_mapping = {}

    def load_files(self) -> None:
        try:
            logger.info("Loading Chase file: %s", self.chase_file)
            chase_ext = self.chase_file.suffix.lower()
            if chase_ext in [".xlsx", ".xls"]:
                self.chase_df = pd.read_excel(self.chase_file, header=1)
            else:
                self.chase_df = pd.read_csv(self.chase_file, skipinitialspace=True, index_col=False)
            self.chase_df.columns = self.chase_df.columns.str.strip()
            if "Description" in self.chase_df.columns:
                drop_keywords = ["ORIG CO NAME:MERCHANT BANKCD", "Zelle payment"]
                mask = self.chase_df["Description"].astype(str).str.contains(
                    "|".join(drop_keywords), case=False, na=False
                )
                self.chase_df = self.chase_df[~mask]
            logger.info("Loaded %s Chase transactions", len(self.chase_df))
            self.chase_df["Source"] = "Chase"

            if self.report_file and self.report_file.exists():
                logger.info("Loading second Chase file: %s", self.report_file)
                report_df = pd.read_excel(self.report_file, header=4)
                if "Org Name" in report_df.columns:
                    report_df["Org Name"] = report_df["Org Name"].ffill()
                if "Date" in report_df.columns and "Amount" in report_df.columns:
                    report_df = report_df[~(report_df["Date"].isna() & report_df["Amount"].isna())]
                amount_list, ref_list = [], []
                for _, row in report_df.iterrows():
                    raw = row.get("Amount", "")
                    if pd.isna(raw):
                        amount_list.append(None)
                        ref_list.append("")
                        continue
                    s = str(raw).strip()
                    parts = s.split()
                    if len(parts) >= 2:
                        try:
                            amount_list.append(float(parts[0].replace(",", "").replace("$", "")))
                            ref_list.append(parts[1])
                        except (ValueError, TypeError):
                            amount_list.append(None)
                            ref_list.append("")
                    else:
                        try:
                            amount_list.append(float(s.replace(",", "").replace("$", "")))
                            ref_list.append(str(row.get("Memo/Description", "") or ""))
                        except (ValueError, TypeError):
                            amount_list.append(None)
                            ref_list.append("")
                report_df = report_df.copy()
                report_df["Amount"] = amount_list
                report_df["_ref"] = ref_list
                report_df["Posting Date"] = report_df["Date"]
                report_df["Description"] = report_df.apply(
                    lambda r: f"ORG: {r.get('Org Name', '')} REF: {r.get('_ref', '')}", axis=1
                )
                report_df["Source"] = "Chase 2"
                
                # Report filter intentionally removed to allow matching of XXXXXX references
                report_df = report_df[["Posting Date", "Amount", "Description", "Source"]]
                
                self.chase_df = pd.concat(
                    [self.chase_df[["Posting Date", "Amount", "Description", "Source"]], report_df],
                    ignore_index=True,
                )
                before_dedup = len(self.chase_df)
                self.chase_df["_date_norm"] = pd.to_datetime(
                    self.chase_df["Posting Date"], errors="coerce"
                ).dt.normalize()
                self.chase_df["_amount_norm"] = self.chase_df["Amount"].apply(
                    lambda x: abs(float(str(x).replace(",", "").replace("$", "")))
                    if pd.notna(x) and str(x).strip()
                    else None
                )
                self.chase_df = self.chase_df.drop_duplicates(
                    subset=["_date_norm", "_amount_norm", "Description"], keep="first"
                )
                self.chase_df = self.chase_df.drop(columns=["_date_norm", "_amount_norm"])
                if before_dedup - len(self.chase_df) > 0:
                    logger.info(
                        "Dropped %s duplicate Chase transactions", before_dedup - len(self.chase_df)
                    )
                logger.info("Combined Chase transactions total: %s", len(self.chase_df))
            elif self.report_file:
                logger.warning("Second Chase file not found: %s", self.report_file)

            logger.info("Loading Collections file: %s", self.collections_file)
            collections_ext = self.collections_file.suffix.lower()

            if collections_ext in [".xlsx", ".xls"]:
                excel_file = pd.ExcelFile(self.collections_file)
                target_sheet = "Total Insurance Collections"

                if target_sheet in excel_file.sheet_names:
                    logger.info("Detected main file. Reading sheet: '%s'", target_sheet)
                    self.collections_df = pd.read_excel(excel_file, sheet_name=target_sheet)
                else:
                    logger.info("Sheet 'Total Insurance Collections' not found. Reading first sheet.")
                    self.collections_df = pd.read_excel(excel_file, sheet_name=0)
            else:
                self.collections_df = pd.read_csv(self.collections_file)

            # --- CRITICAL FIX: Strip invisible trailing spaces from Collections References ---
            if "Payment Reference" in self.collections_df.columns:
                self.collections_df["Payment Reference"] = (
                    self.collections_df["Payment Reference"].astype(str).str.strip()
                )

            if "Payment Method" in self.collections_df.columns:
                self.collections_df = self.collections_df[self.collections_df["Payment Method"] != "NON"]
            if "Payer Name" in self.collections_df.columns:
                self.collections_df = self.collections_df[
                    ~self.collections_df["Payer Name"].astype(str).str.contains("self pay", case=False, na=False)
                ]
            logger.info("Loaded %s Collections records", len(self.collections_df))

            if self.name_mapping_file and self.name_mapping_file.exists():
                mapping_df = pd.read_csv(self.name_mapping_file)
                for _, row in mapping_df.iterrows():
                    chase_name = str(row["Chase Name"]).strip().upper()
                    collections_name = str(row["Collections Name"]).strip().upper()
                    self.name_mapping[chase_name] = collections_name
                logger.info("Loaded %s name mappings", len(self.name_mapping))
        except Exception as e:
            logger.error("Error loading files: %s", e)
            raise

    def extract_payment_reference(self, description: str) -> List[str]:
        if pd.isna(description) or not isinstance(description, str):
            return []
        
        found = []
        
        # --- 1. THE MEDICAID ASTERISK FIX ---
        # Extracts blocks like TRN*1*1044761*1141505378~ and splits them by asterisks
        trn_blob = re.search(r"TRN\*1\*([^~\\/]+)", description, re.IGNORECASE)
        if trn_blob:
            segments = trn_blob.group(1).split('*')
            found.extend([s.strip() for s in segments if s.strip()])

        # --- 2. THE CORVEL HYPHEN FIX ---
        # Chops the whole description by spaces, hyphens, and slashes to isolate 7-digit numbers
        segments = re.split(r'[-\s\*\/~]+', description)
        for segment in segments:
            clean_seg = segment.strip().upper()
            # Looks for any isolated segment that is 6-15 characters and contains numbers
            if 6 <= len(clean_seg) <= 15 and any(char.isdigit() for char in clean_seg):
                found.append(clean_seg)

        # --- 3. STANDARD PATTERNS ---
        patterns = [
            r"TRN[:\s]+([0-9A-Z]+)",
            r"TRACE#[:\s]*([0-9A-Z]+)",
            r"REF[:\s]*([0-9A-Z]+)",
            r"payment\s+(?:from|to).*?([0-9A-Z]{6,})",
            r"\b([0-9A-Z]{6,})\b", # Lowered from 8 to 6 to catch shorter references
        ]
        
        seen = set(found)
        result = list(found)
        for pattern in patterns:
            for match in re.findall(pattern, description, re.IGNORECASE):
                if match not in seen:
                    seen.add(match)
                    result.append(match)
        return result

    def extract_orig_co_name(self, description: str) -> str:
        if pd.isna(description) or not isinstance(description, str):
            return ""
        match = re.search(r"ORIG CO NAME:\s*([^/]+?)(?:\s*/|$)", description, re.IGNORECASE)
        if match:
            return match.group(1).strip().upper()
        match = re.search(r"ORG:\s*(.+?)\s*REF:", description, re.IGNORECASE)
        if match:
            return match.group(1).strip().upper()
        return ""

    def get_mapped_payer_name(self, chase_name: str) -> str:
        if not chase_name:
            return ""
        chase_upper = chase_name.upper()
        if chase_upper in self.name_mapping and self.name_mapping[chase_upper] != "NO MATCH FOUND":
            return self.name_mapping[chase_upper]
        for key, value in self.name_mapping.items():
            if value == "NO MATCH FOUND":
                continue
            if chase_upper.startswith(key[:10]) or key.startswith(chase_upper[:10]):
                return value
        return ""

    def _normalize_name(self, name: str) -> str:
        if not isinstance(name, str):
            return ""
        normalized = re.sub(r"[^A-Z0-9\s]", " ", name.upper())
        for suffix in [" INC", " LLC", " CORP", " CORPORATION", " CO", " LTD", " LLP"]:
            normalized = normalized.replace(suffix, " ")
        return re.sub(r"\s+", " ", normalized).strip()

    def fuzzy_match_payer_name(self, chase_name: str, min_ratio: float = 0.9) -> str:
        if not chase_name or self.collections_df is None:
            return ""
        normalized_chase = self._normalize_name(chase_name)
        if not normalized_chase:
            return ""
        if not hasattr(self, "_collections_payer_cache"):
            self._collections_payer_cache = []
            seen = set()
            if "Payer Name" in self.collections_df.columns:
                for raw in self.collections_df["Payer Name"].dropna().unique():
                    normalized = self._normalize_name(str(raw))
                    if normalized and normalized not in seen:
                        seen.add(normalized)
                        self._collections_payer_cache.append((normalized, str(raw)))
        best_name, best_ratio = "", 0.0
        for normalized, raw in self._collections_payer_cache:
            ratio = difflib.SequenceMatcher(None, normalized_chase, normalized).ratio()
            if ratio > best_ratio:
                best_ratio, best_name = ratio, raw
        return best_name.upper() if best_ratio >= min_ratio else ""

    def clean_payment_reference(self, ref: str) -> str:
        if pd.isna(ref):
            return ""
        
        cleaned = re.sub(r"[^0-9A-Za-z]", "", str(ref).strip()).upper()
        
        # --- CRITICAL FIX: Strip common banking suffixes ---
        if cleaned.endswith("TC") and len(cleaned) > 2:
            cleaned = cleaned[:-2]
        if cleaned.endswith("EFT") and len(cleaned) > 3:
            cleaned = cleaned[:-3]
            
        # --- CRITICAL FIX: Avoid NON Reference Trap ---
        if cleaned in ["NON", "NONE", "NULL", "NA", ""]:
            return ""
            
        return cleaned

    def match_transactions(self) -> None:
        def _norm_date(value):
            dt = pd.to_datetime(value, errors="coerce")
            return dt.normalize().date().isoformat() if not pd.isna(dt) else ""

        # --- CRITICAL FIX: Group Collections into Buckets for Split Payments ---
        collections_refs = {}
        collections_by_date_amount = {}
        collections_by_payer_date_amount = {}
        
        for idx, row in self.collections_df.iterrows():
            ref = self.clean_payment_reference(row.get("Payment Reference", ""))
            amount = row.get("Payment Amount")
            date = row.get("Payment Date")
            payer = str(row.get("Payer Name", "")).strip().upper() if pd.notna(row.get("Payer Name")) else ""
            info = {
                "index": idx,
                "original_ref": row.get("Payment Reference"),
                "payer": row.get("Payer Name"),
                "payer_upper": payer,
                "amount": amount,
                "date": date,
            }
            
            try:
                normalized_amount = abs(float(str(amount).replace(",", "").replace("$", "")))
            except (ValueError, TypeError):
                normalized_amount = None

            # Add to Bucket
            if ref:
                if ref not in collections_refs:
                    collections_refs[ref] = {"items": [], "total_amount": 0.0, "matched": False}
                collections_refs[ref]["items"].append(info)
                if normalized_amount is not None:
                    collections_refs[ref]["total_amount"] += normalized_amount

            # Date/Amount Fallbacks (Keep as lists of single items to avoid altering core logic too much)
            date_str = _norm_date(date)
            info_matched_flagged = info.copy()
            info_matched_flagged["matched"] = False
            
            if normalized_amount is not None:
                key = (date_str, normalized_amount)
                if key not in collections_by_date_amount:
                    collections_by_date_amount[key] = []
                collections_by_date_amount[key].append(info_matched_flagged)
                
            if payer and date_str and normalized_amount is not None:
                key = (payer, date_str, normalized_amount)
                if key not in collections_by_payer_date_amount:
                    collections_by_payer_date_amount[key] = []
                collections_by_payer_date_amount[key].append(info_matched_flagged)

        pending_chase = []
        for idx, row in self.chase_df.iterrows():
            description = row.get("Description", "")
            amount = row.get("Amount")
            date = row.get("Posting Date")
            source = row.get("Source", "Chase")
            refs = self.extract_payment_reference(description)
            matched = False
            
            try:
                chase_amount = abs(float(str(amount).replace(",", "").replace("$", "")))
            except (ValueError, TypeError):
                chase_amount = None

            for ref in refs:
                cleaned_ref = self.clean_payment_reference(ref)
                if not cleaned_ref:
                    continue
                
                # 1. Standard Exact Match (with Split Sum support)
                if cleaned_ref in collections_refs:
                    collection_bucket = collections_refs[cleaned_ref]
                    if not collection_bucket["matched"]:
                        bucket_total = collection_bucket["total_amount"]
                        amount_matches = False
                        
                        if chase_amount is not None:
                            # Allow a tiny margin for float rounding errors
                            if abs(bucket_total - chase_amount) <= 0.05:
                                amount_matches = True
                        else:
                            amount_matches = True 
                        
                        is_split = len(collection_bucket["items"]) > 1
                        
                        # Only strictly enforce amount verification if it's a split payment
                        if amount_matches or not is_split:
                            for info in collection_bucket["items"]:
                                self.matches.append(
                                    {
                                        "Source": source,
                                        "Chase_Index": idx,
                                        "Chase_Date": date,
                                        "Chase_Description": description,
                                        "Chase_Amount": amount,
                                        "Collections_Index": info["index"],
                                        "Collections_Date": info["date"],
                                        "Collections_Payer": info["payer"],
                                        "Collections_Amount": info["amount"],
                                        "Payment_Reference": info["original_ref"],
                                        "Matched_Number": cleaned_ref,
                                        "Match_Type": "Reference (Exact - Split Sum)" if is_split else "Reference (Exact)",
                                    }
                                )
                            collection_bucket["matched"] = True
                            matched = True
                            break

                # 2. Masked Suffix + Amount Match (Handles the XXXXXX format)
                if "XXXX" in cleaned_ref and chase_amount is not None:
                    suffix = cleaned_ref.replace("X", "")
                    
                    if len(suffix) >= 3:
                        for col_ref, collection_bucket in collections_refs.items():
                            if not collection_bucket["matched"] and col_ref.endswith(suffix):
                                bucket_total = collection_bucket["total_amount"]
                                
                                if abs(bucket_total - chase_amount) <= 0.05:
                                    is_split = len(collection_bucket["items"]) > 1
                                    for info in collection_bucket["items"]:
                                        self.matches.append(
                                            {
                                                "Source": source,
                                                "Chase_Index": idx,
                                                "Chase_Date": date,
                                                "Chase_Description": description,
                                                "Chase_Amount": amount,
                                                "Collections_Index": info["index"],
                                                "Collections_Date": info["date"],
                                                "Collections_Payer": info["payer"],
                                                "Collections_Amount": info["amount"],
                                                "Payment_Reference": info["original_ref"],
                                                "Matched_Number": f"Masked: {cleaned_ref} -> {col_ref}",
                                                "Match_Type": "Masked Suffix + Amount (Split Sum)" if is_split else "Masked Suffix + Amount",
                                            }
                                        )
                                    collection_bucket["matched"] = True
                                    matched = True
                                    break
                
                if matched:
                    break

            if not matched:
                pending_chase.append(
                    {
                        "index": idx,
                        "date": date,
                        "description": description,
                        "amount": amount,
                        "potential_refs": refs,
                        "source": source,
                    }
                )

        still_unmatched = []
        for item in pending_chase:
            origin_name = self.extract_orig_co_name(item["description"])
            mapped = self.get_mapped_payer_name(origin_name) or self.fuzzy_match_payer_name(origin_name)
            date_str = _norm_date(item["date"])
            try:
                chase_amount = abs(float(str(item["amount"]).replace(",", "").replace("$", "")))
            except (ValueError, TypeError):
                chase_amount = None
            matched = False
            if mapped and date_str and chase_amount is not None:
                key = (mapped, date_str, chase_amount)
                if key in collections_by_payer_date_amount:
                    for collection_info in collections_by_payer_date_amount[key]:
                        if not collection_info["matched"]:
                            self.matches.append(
                                {
                                    "Source": item.get("source", "Chase"),
                                    "Chase_Index": item["index"],
                                    "Chase_Date": item["date"],
                                    "Chase_Description": item["description"],
                                    "Chase_Amount": item["amount"],
                                    "Collections_Index": collection_info["index"],
                                    "Collections_Date": collection_info["date"],
                                    "Collections_Payer": collection_info["payer"],
                                    "Collections_Amount": collection_info["amount"],
                                    "Payment_Reference": collection_info["original_ref"],
                                    "Matched_Number": f"N/A (Payer+Date+Amount: {origin_name})",
                                    "Match_Type": "Payer+Date+Amount",
                                }
                            )
                            collection_info["matched"] = True
                            matched = True
                            break
            if not matched:
                still_unmatched.append(item)

        final_unmatched = []
        for item in still_unmatched:
            try:
                chase_amount = abs(float(str(item["amount"]).replace(",", "").replace("$", "")))
            except (ValueError, TypeError):
                chase_amount = None
            matched = False
            if chase_amount is not None and pd.notna(item["date"]):
                dt = pd.to_datetime(item["date"], errors="coerce")
                if not pd.isna(dt):
                    for delta in range(-4, 5):
                        date_key = (dt + timedelta(days=delta)).normalize().date().isoformat()
                        key = (date_key, chase_amount)
                        if key in collections_by_date_amount:
                            for collection_info in collections_by_date_amount[key]:
                                if not collection_info["matched"]:
                                    self.matches.append(
                                        {
                                            "Source": item.get("source", "Chase"),
                                            "Chase_Index": item["index"],
                                            "Chase_Date": item["date"],
                                            "Chase_Description": item["description"],
                                            "Chase_Amount": item["amount"],
                                            "Collections_Index": collection_info["index"],
                                            "Collections_Date": collection_info["date"],
                                            "Collections_Payer": collection_info["payer"],
                                            "Collections_Amount": collection_info["amount"],
                                            "Payment_Reference": collection_info["original_ref"],
                                            "Matched_Number": "N/A (Date+/-4+Amount match)",
                                            "Match_Type": "Date+/-4+Amount",
                                        }
                                    )
                                    collection_info["matched"] = True
                                    matched = True
                                    break
                        if matched:
                            break
            if not matched:
                final_unmatched.append(item)

        for item in final_unmatched:
            self.unmatched_chase.append(
                {
                    "Index": item["index"],
                    "Date": item["date"],
                    "Description": item["description"],
                    "Amount": item["amount"],
                    "Potential_Refs": ", ".join(item["potential_refs"]) if item["potential_refs"] else "None found",
                }
            )

        # Iterate over buckets for unmatched collections
        for _, bucket in collections_refs.items():
            if not bucket["matched"]:
                for info in bucket["items"]:
                    self.unmatched_collections.append(
                        {
                            "Index": info["index"],
                            "Date": info["date"],
                            "Payer": info["payer"],
                            "Amount": info["amount"],
                            "Payment_Reference": info["original_ref"],
                        }
                    )
                    
        existing = {item["Index"] for item in self.unmatched_collections}
        for _, items in collections_by_date_amount.items():
            for info in items:
                if not info["matched"] and info["index"] not in existing:
                    self.unmatched_collections.append(
                        {
                            "Index": info["index"],
                            "Date": info["date"],
                            "Payer": info["payer"],
                            "Amount": info["amount"],
                            "Payment_Reference": info["original_ref"],
                        }
                    )

    def generate_report(self, output_dir: str | None = None) -> Path:
        output_dir_path = Path(output_dir) if output_dir else self.chase_file.parent
        output_dir_path.mkdir(parents=True, exist_ok=True)

        report_file = output_dir_path / "transaction_matching_report.xlsx"
        with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
            if self.matches:
                pd.DataFrame(self.matches).to_excel(writer, sheet_name="Matched", index=False)
            summary = pd.DataFrame(
                {
                    "Metric": [
                        "Total Chase Transactions",
                        "Total Collections",
                        "Matched",
                        "Unmatched Chase",
                        "Unmatched Collections",
                        "Match Rate (%)",
                    ],
                    "Count": [
                        len(self.chase_df),
                        len(self.collections_df),
                        len(self.matches),
                        len(self.unmatched_chase),
                        len(self.unmatched_collections),
                        f"{(len(self.matches) / len(self.collections_df) * 100):.2f}%"
                        if len(self.collections_df) > 0
                        else "N/A",
                    ],
                }
            )
            summary.to_excel(writer, sheet_name="Summary", index=False)

        unmatched_file = output_dir_path / "unmatched_transactions.xlsx"
        with pd.ExcelWriter(unmatched_file, engine="openpyxl") as writer:
            if self.unmatched_chase:
                pd.DataFrame(self.unmatched_chase).to_excel(
                    writer,
                    sheet_name="Unmatched_Chase",
                    index=False,
                )
            else:
                pd.DataFrame(columns=["Index", "Date", "Description", "Amount", "Potential_Refs"]).to_excel(
                    writer,
                    sheet_name="Unmatched_Chase",
                    index=False,
                )

            if self.unmatched_collections:
                pd.DataFrame(self.unmatched_collections).to_excel(
                    writer,
                    sheet_name="Unmatched_Collections",
                    index=False,
                )
            else:
                pd.DataFrame(columns=["Index", "Date", "Payer", "Amount", "Payment_Reference"]).to_excel(
                    writer,
                    sheet_name="Unmatched_Collections",
                    index=False,
                )

            def _sum(items, key="Amount"):
                total = 0.0
                for item in items:
                    try:
                        value = str(item.get(key, "")).replace(",", "").replace("$", "")
                        if value and value.replace("-", "").replace(".", "").isdigit():
                            total += abs(float(value))
                    except (ValueError, TypeError):
                        pass
                return total

            pd.DataFrame(
                {
                    "Category": ["Unmatched Chase", "Unmatched Collections"],
                    "Count": [len(self.unmatched_chase), len(self.unmatched_collections)],
                    "Total Amount": [_sum(self.unmatched_chase), _sum(self.unmatched_collections)],
                }
            ).to_excel(writer, sheet_name="Summary", index=False)
        return report_file

    def run(self, output_dir: str | None = None) -> Path:
        self.load_files()
        self.match_transactions()
        return self.generate_report(output_dir)


def run_matcher(
    chase1: str,
    chase2: str | None,
    collections: str,
    name_mapping: str | None,
    output_dir: str | None,
) -> str:
    try:
        matcher = TransactionMatcher(
            chase_file=chase1,
            collections_file=collections,
            second_chase_file=chase2 if chase2 else None,
            name_mapping_file=name_mapping if name_mapping else None,
        )
        out = matcher.run(output_dir)
        return (
            "Success!\n\n"
            f"Reports saved to:\n{out.parent}\n\n"
            "- transaction_matching_report.xlsx\n"
            "- unmatched_transactions.xlsx\n\n"
            f"Matched: {len(matcher.matches)}\n"
            f"Unmatched Chase: {len(matcher.unmatched_chase)}\n"
            f"Unmatched Collections: {len(matcher.unmatched_collections)}"
        )
    except Exception as e:
        return f"Error: {str(e)}"


def main_cli() -> None:
    parser = argparse.ArgumentParser(description="Transaction Matcher")
    parser.add_argument("--chase1", required=True, help="Chase file 1 (required)")
    parser.add_argument("--chase2", help="Chase file 2 (optional)")
    parser.add_argument("--collections", required=True, help="Collections file (required)")
    parser.add_argument("--name-mapping", help="Name mapping CSV (optional)")
    parser.add_argument("--output", help="Output directory (default: same as Chase 1)")
    args = parser.parse_args()
    message = run_matcher(args.chase1, args.chase2, args.collections, args.name_mapping, args.output)
    print(message)


if __name__ == "__main__":
    if len(__import__("sys").argv) > 1:
        main_cli()
    else:
        print("Run Streamlit UI with: streamlit run streamlit_app.py")
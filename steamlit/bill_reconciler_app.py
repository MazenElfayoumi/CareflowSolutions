"""
Bill Reconciler - Standalone Application
Reconciles bills between Bank (Check Details) PDF and Website (Electronic Remittance Advice) PDF.
Run directly or build to .exe with: pyinstaller BillReconciler.spec
"""

import pdfplumber
import pandas as pd
import re
import sys
from pathlib import Path
from thefuzz import fuzz

# ═══════════════════════════════════════════════════════════════════
# PDF EXTRACTION LOGIC
# ═══════════════════════════════════════════════════════════════════

def extract_bank_data(file_path):
    """Extract bills from the Bank (Check Details) PDF.
    
    Bank file text has NO spaces between field labels, e.g.:
      PatientName: ABREUPAULINO,CENIAM
      PatientControlNumber: HLR-020068
      ClaimPaymentAmount: $142.60
    
    Each bill section starts with 'ClaimSummary'.
    """
    data = []
    with pdfplumber.open(file_path) as pdf:
        full_text = "\n".join([page.extract_text() or "" for page in pdf.pages])

    # Many bank PDFs list each claim starting with 'PatientName:' (or variants).
    # Split on each patient occurrence so we reliably get one bill per section.
    sections = re.split(r'(?=PatientName:)', full_text)

    for section in sections:
        if not section.strip().startswith('PatientName:'):
            continue
        # HLR can appear as 'PatientCtrlNmbr' or 'PatientControlNumber'
        hlr_match = re.search(r'Patient(?:CtrlNmbr|ControlNumber):\s*(HLR[-–—]?\s*\d+)', section)
        # PatientName: capture up to the next field label or 2+ spaces
        name_match = re.search(
            r'PatientName:\s*(.+?)(?:\s{2,}|ClaimChargeAmount|ClaimNumber|ClaimDate|PatientCtrlNmbr|PatientControlNumber|$)',
            section
        )
        if not name_match:
            # Fallback: grab everything after PatientName: up to the end of the line
            name_match = re.search(r'PatientName:\s*(.+)', section)
        # Amount label varies: 'ClaimPayment', 'ClaimPaymentAmount', or 'PaymentAmount'
        amt_match = re.search(r'(?:ClaimPaymentAmount|ClaimPayment|PaymentAmount):\s*\$?([\d,]+\.\d{2})', section)
        # ClaimDate: e.g. "ClaimDate: 04/28/2025-04/28/2025" — take the first date
        date_match = re.search(r'ClaimDate:\s*(\d{2}/\d{2}/\d{4})', section)

        if hlr_match and amt_match:
            raw_name = name_match.group(1).strip() if name_match else "Unknown"
            # Clean trailing field labels that might have been caught
            raw_name = re.split(r'(?:ClaimCharge|ClaimNumber|ClaimDate|Patient(?:Ctrl|Control|ID|Responsibility))', raw_name)[0].strip()
            # Normalize bank name: "ABREUPAULINO,CENIAM" -> "ABREU PAULINO, CENIA M"
            # 1. Add space after comma
            raw_name = re.sub(r',\s*', ', ', raw_name)
            # 2. Insert spaces between stuck-together capitalized words
            parts = raw_name.split(', ')
            cleaned_parts = []
            for part in parts:
                spaced = re.sub(r'([A-Z])([A-Z][a-z])', r'\1 \2', part)
                spaced = re.sub(r'([a-z])([A-Z])', r'\1 \2', spaced)
                cleaned_parts.append(spaced)
            raw_name = ', '.join(cleaned_parts)
            
            amount = float(amt_match.group(1).replace(',', ''))
            claim_date = date_match.group(1) if date_match else ""
            data.append({
                'HLR_ID': hlr_match.group(1).strip(),
                'Patient_Name': raw_name,
                'Amount': amount,
                'Claim_Date': claim_date
            })

    return pd.DataFrame(data)


def extract_web_data(file_path):
    """Extract bills from the Website (Electronic Remittance Advice) PDF.
    
    Website file structure per bill:
      Visit Details  Billed Amount $500.00  Paid Amount $74.63
      Patient Name        Rendering Provider   Payer Control #   Clinic Name
      URGYEN PALMO        Elshazly Nadim       044313215000      Jackson Heights
      Patient DOB         Prompt Claim #       Date of Service   Clinic Tax ID
      12/15/1958          HLR-019984           04/25/2025        464894775
    """
    data = []
    with pdfplumber.open(file_path) as pdf:
        # Use layout=True to preserve column spacing — critical for separating
        # Patient Name from Rendering Provider (they share single-space words)
        full_text = "\n".join([page.extract_text(layout=True) or "" for page in pdf.pages])
        # Also get non-layout text for fields that don't need column alignment
        plain_text = "\n".join([page.extract_text() or "" for page in pdf.pages])

    # Split on 'Visit Details' — each section is one bill
    layout_sections = re.split(r'Visit Details', full_text)
    plain_sections = re.split(r'Visit Details', plain_text)

    for idx in range(1, min(len(layout_sections), len(plain_sections))):
        section = layout_sections[idx]
        plain_section = plain_sections[idx] if idx < len(plain_sections) else section

        # Paid Amount (use plain text — more reliable for single-line fields)
        amt_match = re.search(r'Paid Amount\s*\$?([\d,]+\.\d{2})', plain_section)
        # HLR ID
        hlr_match = re.search(r'HLR-\d+', plain_section)
        # Date of Service
        date_match = re.search(
            r'Date of Service.*?\n.*?HLR-\d+\s+(\d{2}/\d{2}/\d{4})', plain_section
        )

        # Patient Name: use layout text and split by 3+ spaces for clean columns
        raw_name = "Unknown"
        header_match = re.search(r'Patient Name\s+Rendering Provider.*', section)
        if header_match:
            # Get the line after the header
            after_header = section[header_match.end():]
            data_lines = after_header.strip().split('\n')
            if data_lines and data_lines[0].strip():
                # Split the data line by 3+ spaces to get columns
                columns = re.split(r'\s{3,}', data_lines[0])
                # First non-empty column is the patient name
                for col in columns:
                    col = col.strip()
                    if col:
                        raw_name = col
                        break

        # Clean up: remove rendering provider and payer control # that may have leaked in
        # 1. Remove long numeric payer control numbers (8+ digits) and everything after
        raw_name = re.split(r'\s+\d{8,}', raw_name)[0].strip()
        # 2. Remove known rendering provider name patterns from the end
        #    Common patterns: "Nadim Elshazly", "Elshazly Nadim", "Nadim M Elshazly", etc.
        raw_name = re.sub(r'\s+(?:Nadim\s+)?Elshazly(?:\s+Nadim)?(?:\s+M)?$', '', raw_name, flags=re.IGNORECASE).strip()
        raw_name = re.sub(r'\s+Nadim(?:\s+Elshazly)?$', '', raw_name, flags=re.IGNORECASE).strip()
        raw_name = re.sub(r'\s+Elshazly$', '', raw_name, flags=re.IGNORECASE).strip()

        if hlr_match and amt_match:
            amount = float(amt_match.group(1).replace(',', ''))
            claim_date = date_match.group(1) if date_match else ""
            data.append({
                'HLR_ID': hlr_match.group(0).strip(),
                'Patient_Name': raw_name,
                'Amount': amount,
                'Claim_Date': claim_date
            })

    return pd.DataFrame(data)


def reconcile_files(web_path, bank_path, log_fn=None):
    """Reconcile two PDF files and return (DataFrame, stats_dict)."""
    if log_fn is None:
        log_fn = print

    log_fn("Extracting data from Bank file...")
    df_bank = extract_bank_data(bank_path)
    log_fn(f"  → Found {len(df_bank)} bills in Bank file")

    log_fn("Extracting data from Website file...")
    df_web = extract_web_data(web_path)
    log_fn(f"  → Found {len(df_web)} bills in Website file")

    stats = {
        'bank_count': len(df_bank),
        'web_count': len(df_web),
        'matched': 0,
        'mismatched': 0,
        'missing_bank': 0,
        'missing_web': 0,
    }

    if df_web.empty and df_bank.empty:
        return pd.DataFrame(columns=[
            'HLR_ID', 'patient_insurance_portal', 'patient prompt',
            'claim_date_insurance_portal', 'claim_date_prompt',
            'insurance portal', 'prompt', 'Status'
        ]), stats

    if df_bank.empty:
        result = pd.DataFrame({
            'HLR_ID': df_web['HLR_ID'],
            'patient_insurance_portal': 'N/A',
            'patient prompt': df_web['Patient_Name'],
            'claim_date_insurance_portal': '',
            'claim_date_prompt': df_web['Claim_Date'],
            'insurance portal': None,
            'prompt': df_web['Amount'],
            'Status': '❌ Missing in Bank File'
        })
        stats['missing_bank'] = len(result)
        return result, stats

    if df_web.empty:
        result = pd.DataFrame({
            'HLR_ID': df_bank['HLR_ID'],
            'patient_insurance_portal': df_bank['Patient_Name'],
            'patient prompt': 'N/A',
            'claim_date_insurance_portal': df_bank['Claim_Date'],
            'claim_date_prompt': '',
            'insurance portal': df_bank['Amount'],
            'prompt': None,
            'Status': '❌ Missing in Website File'
        })
        stats['missing_web'] = len(result)
        return result, stats

    log_fn("Merging and comparing bills...")

    # Merge using HLR_ID as the anchor
    results = pd.merge(df_bank, df_web, on='HLR_ID', how='outer', suffixes=('_Bank', '_Web'))

    audit_log = []
    for _, row in results.iterrows():
        b_name = str(row['Patient_Name_Bank']) if pd.notna(row.get('Patient_Name_Bank')) else "N/A"
        w_name = str(row['Patient_Name_Web']) if pd.notna(row.get('Patient_Name_Web')) else "N/A"
        b_amt = row.get('Amount_Bank')
        w_amt = row.get('Amount_Web')
        b_date = str(row.get('Claim_Date_Bank', '')) if pd.notna(row.get('Claim_Date_Bank')) else ''
        w_date = str(row.get('Claim_Date_Web', '')) if pd.notna(row.get('Claim_Date_Web')) else ''

        # Fuzzy Name Comparison
        name_score = fuzz.token_sort_ratio(b_name, w_name)

        status = "✅ Match"
        if pd.isna(b_amt):
            status = "❌ Missing in Bank File"
            stats['missing_bank'] += 1
        elif pd.isna(w_amt):
            status = "Not posted on prompt"
            stats['missing_web'] += 1
        elif abs(b_amt - w_amt) > 0.01:
            status = f"💰 Amount Mismatch (Diff: ${round(b_amt - w_amt, 2)})"
            stats['mismatched'] += 1
        elif name_score < 75:
            status = "❓ Name Variance"
            stats['matched'] += 1  # amounts match, just name differs
        else:
            stats['matched'] += 1

        audit_log.append({
            'HLR_ID': row['HLR_ID'],
            'patient_insurance_portal': b_name,
            'patient prompt': w_name,
            'claim_date_insurance_portal': b_date,
            'claim_date_prompt': w_date,
            'insurance portal': b_amt,
            'prompt': w_amt,
            'Status': status
        })

    return pd.DataFrame(audit_log), stats


def save_report(df, output_path):
    """Save the reconciliation report to Excel with a totals row."""
    def _sum_col(dataframe, col):
        if col in dataframe.columns:
            return pd.to_numeric(dataframe[col], errors='coerce').sum()
        return 0

    total_insurance = _sum_col(df, 'insurance portal')
    total_prompt = _sum_col(df, 'prompt')

    totals_row = {
        'HLR_ID': 'Totals',
        'patient_insurance_portal': '',
        'patient prompt': '',
        'claim_date_insurance_portal': '',
        'claim_date_prompt': '',
        'insurance portal': total_insurance,
        'prompt': total_prompt,
        'Status': ''
    }

    final_with_totals = pd.concat([df, pd.DataFrame([totals_row])], ignore_index=True)

    target = Path(output_path)
    try:
        final_with_totals.to_excel(str(target), index=False)
        return str(target)
    except PermissionError:
        alt = str(target).replace('.xlsx', '_temp.xlsx')
        final_with_totals.to_excel(alt, index=False)
        return alt


# ═══════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════

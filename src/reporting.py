import io
import re
from datetime import datetime, time
from typing import Tuple, Dict, Any

import pandas as pd


def clean_amount(amount_str):
    """Clean and convert amount strings to float"""
    try:
        cleaned = str(amount_str).replace(',', '').replace(' ', '').strip()
        if cleaned == '' or cleaned.lower() == 'nan':
            return 0.0
        return float(cleaned)
    except:
        return 0.0



def parse_time_flexible(value):
    """Parse date/time to time object from various formats"""
    if pd.isna(value):
        return None
    try:
        dt = pd.to_datetime(value, errors='coerce')
        if pd.isna(dt):
            return None
        return dt.time()
    except Exception:
        return None



def categorize_meal_period(sale_time):
    """Categorize sale time into meal periods"""
    if sale_time is None:
        return "Unknown"

    breakfast_start = time(6, 0)
    breakfast_end = time(12, 30)
    lunch_end = time(17, 0)

    if sale_time < breakfast_start:
        return "Dinner"
    elif breakfast_start <= sale_time < breakfast_end:
        return "Breakfast"
    elif breakfast_end <= sale_time <= lunch_end:
        return "Lunch"
    else:
        return "Dinner"



def extract_date_from_metadata(metadata_line: str) -> str:
    """Extract and format date from metadata line"""
    try:
        date_pattern = r'\d{2}-\d{2}-\d{4}'
        dates_found = re.findall(date_pattern, metadata_line)
        if dates_found:
            date_str = dates_found[0]
            return date_str.replace('-', '/')
        return datetime.now().strftime('%d/%m/%Y')
    except:
        return datetime.now().strftime('%d/%m/%Y')



def format_report_new_style(report_data: Dict[str, Any], metadata_line: str) -> str:
    lahore_period_totals = report_data['lahore_period_totals']
    lahore_ordertype_totals = report_data['lahore_ordertype_totals']
    lahore_total_sales = report_data['lahore_total_sales']
    santorini_period_totals = report_data['santorini_period_totals']
    santorini_ordertype_totals = report_data['santorini_ordertype_totals']
    santorini_total_sales = report_data['santorini_total_sales']

    formatted_date = extract_date_from_metadata(metadata_line)

    report_lines = []
    report_lines.append(f"{formatted_date};;")
    report_lines.append("Location;Lahore;Santorini")

    def getv(d, k):
        return d.get(k, 0.0)

    report_lines.append(f"Lunch sales;{getv(lahore_period_totals, 'Lunch'):,.2f};{getv(santorini_period_totals, 'Lunch'):,.2f}")
    report_lines.append(f"Dinner sales;{getv(lahore_period_totals, 'Dinner'):,.2f};{getv(santorini_period_totals, 'Dinner'):,.2f}")
    report_lines.append(f"Breakfast (weekend);{getv(lahore_period_totals, 'Breakfast'):,.2f};{getv(santorini_period_totals, 'Breakfast'):,.2f}")

    def ordertype_sums(ordertype_totals):
        delivery = eatin = takeaway = 0.0
        for order_type, amount in ordertype_totals.items():
            order_type_lower = str(order_type).lower()
            if 'delivery' in order_type_lower:
                delivery += amount
            elif 'eat in' in order_type_lower or 'dine' in order_type_lower:
                eatin += amount
            elif 'take away' in order_type_lower or 'takeaway' in order_type_lower:
                takeaway += amount
            else:
                eatin += amount
        return eatin, delivery, takeaway

    lahore_eatin, lahore_delivery, lahore_takeaway = ordertype_sums(lahore_ordertype_totals)
    santorini_eatin, santorini_delivery, santorini_takeaway = ordertype_sums(santorini_ordertype_totals)

    report_lines.append(f"Total Eat in;{lahore_eatin:,.2f};{santorini_eatin:,.2f}")
    report_lines.append(f"Total Delivery;{lahore_delivery:,.2f};{santorini_delivery:,.2f}")
    report_lines.append(f"Total Take away;{lahore_takeaway:,.2f};{santorini_takeaway:,.2f}")

    report_lines.append(f"TOTAL SALES;{lahore_total_sales:,.2f};{santorini_total_sales:,.2f}")

    return '\n'.join(report_lines)



def process_ikitchen_data(uploaded_file) -> Tuple[Dict[str, Any], pd.DataFrame, str]:
    """Process the iKitchen CSV and generate report using receipt-level totals."""
    try:
        content = uploaded_file.getvalue().decode('utf-8')
        lines = content.split('\n')

        if len(lines) < 2:
            return None, None, "File format is incorrect."

        metadata_line = lines[1].strip()

        date_pattern = r'\d{2}-\d{2}-\d{4}'
        dates_found = re.findall(date_pattern, metadata_line)
        sales_date = str(dates_found[0]) if dates_found else ""

        # Try reading from line 4 like before, but fall back to auto-detect header row
        try:
            csv_content = '\n'.join(lines[3:])
            raw_df = pd.read_csv(io.StringIO(csv_content))
        except Exception:
            raw_df = pd.read_csv(io.StringIO(content))

        # Normalize column names
        raw_df.columns = raw_df.columns.str.strip()

        # Amount column handling: prefer 'Item amount', fallback to 'Amount'
        amount_col = 'Item amount' if 'Item amount' in raw_df.columns else ('Amount' if 'Amount' in raw_df.columns else None)
        if amount_col is None:
            return None, None, "Expected amount column not found ('Item amount' or 'Amount')."

        # Clean amounts
        df = raw_df.copy()
        df['Amount_clean'] = df[amount_col].astype(str).str.replace(',', '').str.replace(' ', '')
        df['Amount_clean'] = pd.to_numeric(df['Amount_clean'], errors='coerce').fillna(0.0)

        # Optional status filter if present
        if 'Status' in df.columns:
            df = df[df['Status'].astype(str).str.strip().str.lower() == 'ordered']

        # Build receipt-level table
        if 'Receipt no' not in df.columns:
            return None, None, "Missing 'Receipt no' column."

        agg_spec = {
            'Amount_clean': 'sum'
        }
        # Bring along representative fields per receipt
        take_first_cols = [c for c in ['Ordertype name', 'Register name', 'Sale date'] if c in df.columns]
        grouped = df.groupby('Receipt no', as_index=False).agg({**agg_spec, **{c: 'first' for c in take_first_cols}})
        grouped = grouped.rename(columns={'Amount_clean': 'Order_total'})

        # Compute time + meal period
        if 'Sale date' in grouped.columns:
            grouped['Sale_time'] = grouped['Sale date'].apply(parse_time_flexible)
        else:
            grouped['Sale_time'] = None
        grouped['Meal_period'] = grouped['Sale_time'].apply(categorize_meal_period)

        # Split by location
        if 'Register name' in grouped.columns:
            lahore_mask = grouped['Register name'] != 'CO-50010'
            santorini_mask = grouped['Register name'] == 'CO-50010'
        else:
            lahore_mask = pd.Series([True] * len(grouped))
            santorini_mask = pd.Series([False] * len(grouped))

        lahore_orders = grouped[lahore_mask]
        santorini_orders = grouped[santorini_mask]

        def get_metrics(orders_df: pd.DataFrame):
            period_totals = orders_df.groupby('Meal_period')['Order_total'].sum().to_dict() if not orders_df.empty else {}
            ordertype_col = 'Ordertype name' if 'Ordertype name' in orders_df.columns else None
            if ordertype_col:
                ordertype_totals = orders_df.groupby(ordertype_col)['Order_total'].sum().to_dict()
            else:
                ordertype_totals = {}
            total_sales = float(orders_df['Order_total'].sum()) if not orders_df.empty else 0.0
            return period_totals, ordertype_totals, total_sales

        lahore_period_totals, lahore_ordertype_totals, lahore_total_sales = get_metrics(lahore_orders)
        santorini_period_totals, santorini_ordertype_totals, santorini_total_sales = get_metrics(santorini_orders)

        report_data = {
            'metadata_line': metadata_line,
            'sales_date': sales_date,
            'valid_sales': grouped,
            'lahore_period_totals': lahore_period_totals,
            'lahore_ordertype_totals': lahore_ordertype_totals,
            'lahore_total_sales': lahore_total_sales,
            'santorini_period_totals': santorini_period_totals,
            'santorini_ordertype_totals': santorini_ordertype_totals,
            'santorini_total_sales': santorini_total_sales
        }

        final_report = format_report_new_style(report_data, metadata_line)
        report_data['final_report'] = final_report

        return report_data, grouped, None

    except Exception as e:
        return None, None, f"Error processing file: {str(e)}" 
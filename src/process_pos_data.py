import pandas as pd
import uuid

from typing import List, Dict
from src.models import Customer, Order, OrderItem

from src.db import supabase, get_table, BATCH_SIZE, batch_insert_orders, get_existing_receipts_ids, get_existing_customers, refresh_views_analytics
from src.utils import standardize_phone_number, get_spreadsheet_data, validate_spreadsheet_columns


order_type_mapping = {
    "Take away": "Take away",
    "Eat in": "Dine-In",
    "Delivery": "Delivery"
}


def batch_insert_customers(customers: List[Customer]) -> Dict[str, str]:
    customer_id_map = {}
    existing_customers = {}

    # Lookup existing customers
    phone_numbers = [customer.phone_number for customer in customers]
    existing_customers = get_existing_customers(phone_numbers)
    for phone_number in existing_customers:
        customer_id_map[phone_number] = existing_customers[phone_number]["customer_id"]

    # Insert new customers
    new_customers = [
        customer for customer in customers 
        if customer.phone_number not in existing_customers
    ]

    for customer in new_customers:
        customer.customer_id = str(uuid.uuid4())
        customer_id_map[customer.phone_number] = customer.customer_id

    for i in range(0, len(new_customers), BATCH_SIZE):
        batch = [customer.model_dump() for customer in new_customers[i:i + BATCH_SIZE]]
        supabase.table(get_table("customers")).insert(batch).execute()

    return customer_id_map



def process_pos_data(file_path, logger=None):
    data = get_spreadsheet_data(file_path)
    validate_spreadsheet_columns(data, "servquick_columns")

    data = data.dropna(subset=["Receipt no"])

    # ---------- Data Cleaning ----------
    data["Item quantity"] = pd.to_numeric(data["Item quantity"], errors="coerce")

    # Clean money helper (commas -> numeric)
    def clean_money(series):
        return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False), errors="coerce")

    data["Item amount"] = clean_money(data["Item amount"])

    # Optional debug for bad rows
    if data["Item amount"].isna().any() and logger:
        logger("Invalid 'Item amount' detected:")
        logger(data[data["Item amount"].isna()])

    # ---------- Taxes (line-level) ----------
    # Preferred single tax column
    tax_cols = []
    if "Tax amount" in data.columns:
        tax_cols.append("Tax amount")
    else:
        # fallback: sum the usual constituents if they exist
        for col in ["SGST amount", "CGST amount", "IGST amount", "CESS amount", "GST amount"]:
            if col in data.columns:
                tax_cols.append(col)

    if tax_cols:
        for col in tax_cols:
            data[col] = clean_money(data[col])
        # row-wise sum of whichever tax columns exist
        data["__Tax_line_total__"] = data[tax_cols].sum(axis=1, min_count=1).fillna(0.0)
    else:
        data["__Tax_line_total__"] = 0.0  # no tax info available in this export

    # ---------- Group Items by Receipt Number (unchanged for items) ----------
    grouped = data.groupby("Receipt no").apply(lambda group: {
        "order_items": group.apply(lambda row: OrderItem(
            item_name=row["Item name"],
            quantity=row["Item quantity"],
            amount=row["Item amount"]  # keep item rows pre-tax
        ), axis=1).tolist(),
        "order_items_text": "; ".join(
            f'{row["Item name"]} (x{row["Item quantity"]})' for _, row in group.iterrows()
        )
    }).reset_index(name="grouped_data")

    # ---------- Per-receipt monetary totals ----------
    # items_total (pre-tax) and tax_total (line-level sum), then combined
    receipt_totals = (
        data.groupby("Receipt no", as_index=False)
            .agg(items_total=("Item amount", "sum"),
                 tax_total=("__Tax_line_total__", "sum"))
    )
    receipt_totals["total_with_tax"] = receipt_totals["items_total"] + receipt_totals["tax_total"]

    # Merge grouped items + bring one representative row per receipt
    final_data = pd.merge(
        data.drop_duplicates("Receipt no"),
        grouped, on="Receipt no", how="left"
    )
    # Attach the totals
    final_data = pd.merge(final_data, receipt_totals, on="Receipt no", how="left")

    # ---------- Process all Customers ----------
    customers = []
    for _, row in final_data.iterrows():
        phone_number = standardize_phone_number(row.get("Customer mobile"))
        if pd.isna(phone_number) or not phone_number:
            continue  # Skip if no phone number
        if phone_number in [cust.phone_number for cust in customers]:
            continue  # Already added

        email = row.get("Customer email")
        address = row.get("Customer address")

        customer = Customer(
            name=row.get("Customer name"),
            phone_number=phone_number,
            email=email if not pd.isna(email) else None,
            address=address if not pd.isna(address) else None
        )
        customers.append(customer)

    customer_id_map = batch_insert_customers(customers)
    if logger:
        logger(f"Processing {len(customers)} customers ...")

    # ---------- Existing receipts ----------
    def format_receipt_id(row):
        receipt_no = row["Receipt no"]
        order_date = row["Sale date"]
        if isinstance(order_date, pd.Timestamp):
            formatted_date = order_date.strftime("%d_%m_%Y")
        else:
            parsed_date = pd.to_datetime(order_date, errors="coerce")
            if pd.isna(parsed_date):
                return None
            formatted_date = parsed_date.strftime("%d_%m_%Y")
        return f"{receipt_no}_{formatted_date}"

    receipt_ids = final_data.apply(format_receipt_id, axis=1).dropna().unique().tolist()
    existing_receipt_ids = get_existing_receipts_ids(receipt_ids)

    # Fast lookup for totals per receipt
    totals_map = receipt_totals.set_index("Receipt no")[["items_total", "tax_total", "total_with_tax"]].to_dict("index")

    # ---------- Build Orders ----------
    orders = []
    for _, row in final_data.iterrows():
        receipt_no = row["Receipt no"]

        # Parse date
        order_date = row["Sale date"]
        if isinstance(order_date, pd.Timestamp):
            formatted_date = order_date.strftime("%d_%m_%Y")
            order_date_str = order_date.isoformat()
        else:
            parsed_date = pd.to_datetime(order_date, errors="coerce")
            if pd.isna(parsed_date):
                if logger:
                    logger(f"Skipping order with missing or invalid date for receipt {receipt_no}")
                continue
            formatted_date = parsed_date.strftime("%d_%m_%Y")
            order_date_str = parsed_date.isoformat()

        formatted_receipt_id = f"{receipt_no}_{formatted_date}"

        # Skip duplicates
        if formatted_receipt_id in existing_receipt_ids:
            if logger:
                logger(f"Skipping order with receipt ID: {formatted_receipt_id} - already in the database")
            continue

        customer_id = customer_id_map.get(standardize_phone_number(row.get("Customer mobile")))
        location_name = 'Santorini' if row.get('Register name') == 'CO-50010' else 'Lahore'

        # Totals with tax (fees intentionally excluded)
        totals = totals_map.get(receipt_no, {"items_total": 0.0, "tax_total": 0.0, "total_with_tax": 0.0})

        order = Order(
            order_id=str(uuid.uuid4()),
            customer_id=customer_id,
            order_date=order_date_str,
            order_items=row['grouped_data']["order_items"],          # still pre-tax per line
            order_items_text=row['grouped_data']["order_items_text"],
            total_amount=float(totals["total_with_tax"]),            # <-- items + taxes
            order_type=order_type_mapping.get(row["Ordertype name"]),
            receipt_id=formatted_receipt_id,
            location=location_name
        )
        orders.append(order)

    batch_insert_orders(orders)

    refresh_views_analytics()

    if logger:
        logger(f"Processing complete. {len(final_data)} receipts processed.")

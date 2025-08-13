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

    # Data Cleaning
    data["Item quantity"] = pd.to_numeric(data["Item quantity"], errors="coerce")
    data["Item amount"] = data["Item amount"].astype(str).str.replace(",", "")
    data["Item amount"] = pd.to_numeric(data["Item amount"], errors="coerce")

    # Handle any invalid data
    if data["Item amount"].isna().any():
        if logger:
            logger("Invalid 'Item amount' detected:")
            logger(data[data["Item amount"].isna()])


    # Group Items by Receipt Number
    grouped = data.groupby("Receipt no").apply(lambda group: {
        "order_items": group.apply(lambda row: OrderItem(
            item_name=row["Item name"],
            quantity=row["Item quantity"],
            amount=row["Item amount"]
        ), axis=1).tolist(),
        "order_items_text": "; ".join(
            f'{row["Item name"]} (x{row["Item quantity"]})' for _, row in group.iterrows()
        )
    }).reset_index(name="grouped_data")


    final_data = pd.merge(data.drop_duplicates("Receipt no"), grouped, on="Receipt no", how="left")

    # Process all Customers
    customers = []
    for _, row in final_data.iterrows():
        phone_number = standardize_phone_number(row.get("Customer mobile"))
        if pd.isna(phone_number) or not phone_number:
            continue  # Skip if no phone number

        if phone_number in [cust.phone_number for cust in customers]:
            continue # Skip if Customer was already processed

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

    # Process all Orders

    # First, fetch existing receipt IDs from the database
    # Format all receipt IDs first using your existing date logic
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

    # Apply the formatting to all rows
    receipt_ids = final_data.apply(format_receipt_id, axis=1).dropna().unique().tolist()

    # Now fetch existing formatted receipt IDs from the database
    existing_receipt_ids = get_existing_receipts_ids(receipt_ids)

    orders = []

    # Proceed with order processing using the same logic
    for _, row in final_data.iterrows():
        receipt_no = row["Receipt no"]

        # Format order date
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

        # Skip if already in database
        if formatted_receipt_id in existing_receipt_ids:
            if logger:
                logger(f"Skipping order with receipt ID: {formatted_receipt_id} - already in the database")
            continue

        customer_id = customer_id_map.get(standardize_phone_number(row["Customer mobile"]))

        # When processing orders, add logic like for location name
        location_name = 'Santorini' if row.get('Register name') == 'CO-50010' else 'Lahore'

        order = Order(
            order_id=str(uuid.uuid4()),
            customer_id=customer_id,
            order_date=order_date_str,
            order_items=row['grouped_data']["order_items"],
            order_items_text=row['grouped_data']["order_items_text"],
            total_amount=sum(item.amount for item in row['grouped_data']["order_items"]),
            order_type=order_type_mapping.get(row["Ordertype name"]),
            receipt_id=formatted_receipt_id,
            location=location_name
        )
        orders.append(order)

    batch_insert_orders(orders)

    refresh_views_analytics()

    if logger:
        logger(f"Processing complete. {len(final_data)} receipts processed.")

import os
from supabase import create_client, Client
from dotenv import load_dotenv
from typing import List, Dict

from src.models import Order

# Load environment variables
load_dotenv(".env")

# Optionally load from Streamlit secrets if available
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = None

try:
	import streamlit as st  # type: ignore
	if not SUPABASE_URL or not SUPABASE_KEY:
		secrets = st.secrets.get("supabase", {}) if hasattr(st, "secrets") else {}
		SUPABASE_URL = SUPABASE_URL or secrets.get("url")
		SUPABASE_KEY = SUPABASE_KEY or secrets.get("key")
except Exception:
	# Streamlit may not be present in some contexts
	pass

if SUPABASE_URL and SUPABASE_KEY:
	supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

PROD_TABLES = {
	"customers": "customers",
	"orders": "orders"
}


BATCH_SIZE = 1000

def get_table(name: str):
	return PROD_TABLES.get(name)


def _ensure_client():
	if supabase is None:
		raise ValueError("Supabase client is not configured. Set SUPABASE_URL and SUPABASE_KEY env vars or provide them in Streamlit secrets under 'supabase'.")


def get_existing_customers(phone_numbers: List[str]) -> Dict[str, dict]:
	_ensure_client()
	existing_customers_data = supabase.table(get_table("customers")).select("*").in_("phone_number", phone_numbers).execute()
	return {cust['phone_number']: cust for cust in existing_customers_data.data}


def get_existing_orders(receipt_numbers: List[str]) -> Dict[str, dict]:
	_ensure_client()
	existing_orders_data = supabase.table(get_table("orders")).select("*").in_("receipt_id", receipt_numbers).execute()
	return {order['receipt_id']: order for order in existing_orders_data.data}


def get_existing_receipts_ids(receipt_numbers: List[str], batch_size: int = 100):
	_ensure_client()
	existing_receipts = set()
	table = supabase.table(get_table("orders"))

	for i in range(0, len(receipt_numbers), batch_size):
		batch = receipt_numbers[i:i + batch_size]
		try:
			response = table.select("*").in_("receipt_id", batch).execute()
			if response.data:
				existing_receipts.update([item["receipt_id"] for item in response.data])
		except Exception as e:
			print(f"Error fetching receipts batch {i}-{i+len(batch)}: {e}")
			continue

	return existing_receipts


def batch_insert_orders(orders: List[Order]):
	_ensure_client()
	for i in range(0, len(orders), BATCH_SIZE):
		batch = [order.model_dump() for order in orders[i:i + BATCH_SIZE]]
		supabase.table(get_table("orders")).insert(batch).execute()


def refresh_views_analytics():
	_ensure_client()
	supabase.rpc("refresh_all_views").execute()
import pandas as pd
import logging
import yaml
import pandas as pd
import os


columns_config = {}
config_path = 'spreadsheets_config.yaml'
if os.path.exists(config_path):
	with open(config_path, 'r') as file:
		columns_config = yaml.safe_load(file) or {}
else:
	logging.warning("spreadsheets_config.yaml not found. Column validation will be skipped or minimal.")


def get_spreadsheet_data(file_path: str):
	# Function to read the file and dynamically skip rows
	def dynamic_skip(file_reader, file_path):
		skip = 0
		while True:
			# Read the file with the current number of skipped rows
			df = file_reader(file_path, skiprows=skip)
			
			# Check if the DataFrame has the expected column names
			if "Customer name" in df.columns or "Contact Number" in df.columns:
				return df  # Return the valid DataFrame
			
			# Increment the number of rows to skip and try again
			skip += 1

	if file_path.endswith(".csv"):
		# Handle CSV files with dynamic skipping
		return dynamic_skip(pd.read_csv, file_path)
	else:
		# Handle Excel files with dynamic skipping
		return dynamic_skip(pd.read_excel, file_path)


# Function to standardize phone numbers
def standardize_phone_number(phone_number):
	if pd.isna(phone_number):
		return None

	if isinstance(phone_number, (int, float)):
        # If float, cast to int first to remove .0
		phone_number = str(int(phone_number))
	else:
		phone_number = str(phone_number)

	# Remove spaces, dashes, and other non-numeric characters
	phone_number = "".join(filter(str.isdigit, phone_number))

	# Add the country code +880 if not already present
	if not phone_number.startswith("+880"):
		if phone_number.startswith("0"):
			phone_number = "880" + phone_number[1:]  # Remove leading zero
		elif phone_number.startswith("880"):
			phone_number = phone_number
		else:
			phone_number = "880" + phone_number
			
	# Remove the country code and check the length
	local_number = phone_number[3:]  # Exclude the '880' country code
	if len(local_number) < 8 or len(local_number) > 15:
		logging.warning(f"Invalid phone number length for: {phone_number}")
		return None # Remove the number if it doesn't meet the length requirement

	return f"+{phone_number}"  # Add the '+' prefix

def convert_rating(value):
	"""
	Convert rating strings to integers.
	"""
	rating_map = {
		'poor': 1,
		'fair': 2,
		'good': 3,
		'great': 4
	}

	if pd.isna(value):
		return 0  # Default to 0 for missing values

	cleaned_value = str(value).lower().strip()
	return rating_map.get(cleaned_value, 1)

def is_valid_email(email):
	"""
	Check if an email is valid (simple validation to exclude placeholders).
	"""
	return email not in ["-", "--", "---", None, ""] and not pd.isna(email)


def validate_spreadsheet_columns(data: pd.DataFrame, data_source: str):
	# Support both dict and list formats
	if isinstance(columns_config, dict):
		expected_columns = columns_config.get(data_source, [])
	elif isinstance(columns_config, list):
		expected_columns = columns_config
	else:
		expected_columns = []

	# If no expected columns configured, skip strict validation
	if not expected_columns:
		logging.warning("No expected columns configured; skipping strict column validation.")
		return

	missing_columns = [col for col in expected_columns if col not in data.columns]
	if missing_columns:
		raise ValueError(f"The spreadsheet is missing the following required columns: {', '.join(missing_columns)}")
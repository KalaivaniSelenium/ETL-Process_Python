import pandas as pd
import logging
import json
import os
import re  # Import the regular expression module

# Set up logging
logging.basicConfig(
    filename="Logs/etl_process.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load configuration
with open("Config/config.json", "r") as config_file:
    config = json.load(config_file)

INPUT_FILE = config["input_file"]
OUTPUT_FILE = config["output_file"]

# Function to categorize region based on country
def categorize_region(country):
    region_mapping = {
        "USA": "North America",
        "Canada": "North America",
        "UK": "Europe",
        "India": "Asia",
        "China": "Asia",
        "Germany": "Europe",
        "Australia":"Australia"
    }
    return region_mapping.get(country, "Other")

# Email validation function
def is_valid_email(email):
    # Define the regex pattern for a valid email
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(email_pattern, email) is not None

def is_numeric(value):
    try:
        # Try converting the value to a numeric type
        pd.to_numeric(value)
        return True
    except ValueError:
        # If conversion fails, it's not numeric
        return False

try:
    # Step 1: Extract - Read data from the CSV file
    logging.info("Starting data extraction.")
    data = pd.read_csv(INPUT_FILE)
    logging.info(f"Data extracted successfully from {INPUT_FILE}.")

    # Step 2: Transform - Clean and process the data
    logging.info("Starting data transformation.")

    # Remove rows with missing or invalid data
    cleaned_data = data.dropna(subset=["Name", "Email", "Age", "Country"])
    
    # Validate Email column and filter out invalid emails
    cleaned_data = cleaned_data[cleaned_data["Email"].apply(is_valid_email)]

    # Convert Name to title case
    cleaned_data.loc[:, "Name"] = cleaned_data["Name"].str.title()

    # Reject rows where Age is not numeric
    cleaned_data = cleaned_data[cleaned_data["Age"].apply(is_numeric)]

    # Filter out customers under 18
    cleaned_data = cleaned_data[cleaned_data["Age"] >= 18]

    # Add a new column for region categorization
    cleaned_data["Region"] = cleaned_data["Country"].apply(categorize_region)

    logging.info("Data transformation completed successfully.")

    # Step 3: Load - Save the transformed data into a new CSV file
    logging.info("Starting data loading.")
    cleaned_data.to_csv(OUTPUT_FILE, index=False)
    logging.info(f"Transformed data saved to {OUTPUT_FILE}.")
    logging.info(f"Number of records processed: {len(cleaned_data)}")

except Exception as e:
    logging.error(f"An error occurred during the ETL process: {e}")
    print(f"An error occurred: {e}")

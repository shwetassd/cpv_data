###########################################################################################################
# python 3.11
# category_language.py
# This script reads a CSV file containing level 3 category IDs from an S3 bucket, retrieves the corresponding
# category names in multiple languages from an API, and then writes the results back to another CSV file in the S3 bucket.
# 2024/04/23
##########################################################################################################

import concurrent
import csv
import io
import boto3
import requests
import logging
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

# Hardcoded variables
S3_BUCKET_NAME = "bi-visable"
S3_FOLDER_PATH = "cpv_data/categories/"
LEVEL3_CSV_FILE_KEY = "level3_cpv_category.csv"
CATEGORY_NAMES_FOLDER = "cpv_data/categoryNames/"
CATEGORY_NAMES_CSV_FILE = "cpv_categoryName.csv"
LANGUAGE_CODES = ['en', 'de', 'fr']
MAX_RETRIES = 5
RETRY_INTERVAL_SECONDS = 300  # 5 minutes in seconds

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Initialize S3 client
s3_client = boto3.client('s3')

def fetch_category_name(category_id, language):
    logging.info(f"Fetching category name for category ID {category_id} and language {language}")
    base_url = "https://deapi.alibaba.com/openapi/param2/1/com.alibaba.v.business/category.info.findCategoryById/195284"
    params = {
        "siteId": "wlw",
        "language": language,
        "categoryId": category_id
    }
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json().get("data", {})
        category_name = data.get("categoryName")

        if category_name is not None:
            return category_name
        else:
            logging.warning(f"Category name not found for category ID {category_id}.")
            return None
    else:
        logging.error(f"Failed to fetch category data for category ID {category_id} and language {language}.")
        return None

def fetch_category_names_in_parallel(category_ids, languages):
    category_info = {}
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_category = {executor.submit(fetch_category_name, category_id, language): (category_id, language) for category_id in category_ids for language in languages}
        for future in concurrent.futures.as_completed(future_to_category):
            category_id, language = future_to_category[future]
            try:
                category_name = future.result()
                if category_name is not None:
                    if category_id not in category_info:
                        category_info[category_id] = {}
                    category_info[category_id][f'categoryName_{language}'] = category_name
            except Exception as exc:
                logging.error(f"Error fetching category name for category ID {category_id} and language {language}: {exc}")

    return category_info

def main():
    start_time = time.time()  # Record start time

    # Retry fetching the file for a maximum of MAX_RETRIES times with an interval of RETRY_INTERVAL_SECONDS each
    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_FOLDER_PATH + LEVEL3_CSV_FILE_KEY)
            lines = response['Body'].read().decode('utf-8').splitlines()
            reader = csv.DictReader(lines, delimiter=';')
            category_ids = []
            for row in reader:
                category_ids.append(row['level1_ID'])
                category_ids.append(row['level2_ID'])
                category_ids.append(row['level3_ID'])
                category_ids.append(row['level4_ID'])
            # Remove duplicates and empty strings
            category_ids = list(filter(None, set(category_ids)))
            logging.info(f"Found {len(category_ids)} unique category IDs.")
            break  # Exit the retry loop if successful
        except Exception as e:
            logging.error(f"Failed to read level3 CSV file from S3: {e}")
            retry_count += 1
            if retry_count < MAX_RETRIES:
                logging.info(f"Retrying in {RETRY_INTERVAL_SECONDS / 60} minutes...")
                time.sleep(RETRY_INTERVAL_SECONDS)
    else:
        logging.error("Maximum retry attempts reached. Exiting.")
        return

    # Fetch category names in parallel
    category_info = fetch_category_names_in_parallel(category_ids, LANGUAGE_CODES)

    # Convert fetched data to DataFrame
    df = pd.DataFrame(category_info).transpose()

    # Define the desired column order
    desired_columns_order = ['categoryName_en', 'categoryName_de', 'categoryName_fr']

    # Reorder the columns
    df_reordered = df.reindex(columns=desired_columns_order)

    # Write the DataFrame to a CSV file
    try:
        df_reordered.to_csv(CATEGORY_NAMES_CSV_FILE, sep=';', index_label='categoryId')
        # Upload the CSV file to S3 bucket
        with open(CATEGORY_NAMES_CSV_FILE, 'rb') as file:
            s3_client.put_object(Body=file, Bucket=S3_BUCKET_NAME, Key= CATEGORY_NAMES_FOLDER + CATEGORY_NAMES_CSV_FILE)
    except Exception as e:
        logging.error(f"Failed to upload CSV file to S3 bucket: {e}")
        return

    end_time = time.time()  # Record end time
    execution_time = (end_time - start_time) / 60  # Calculate execution time in minutes
    logging.info(f"Total execution time: {execution_time:.2f} minutes")

    logging.info("Data has been written to cpv_categoryName.csv file and uploaded to S3 bucket.")

if __name__ == "__main__":
    main()
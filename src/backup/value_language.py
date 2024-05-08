###########################################################################################################
# python 3.11
# value_language.py
# This script fetches values in multiple languages from an API using leaf category IDs
# obtained from an S3 CSV file and writes the results back to another CSV file in the same S3 bucket.
# 2024/04/23
##########################################################################################################

import boto3
import csv
from io import StringIO
import requests
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from requests.exceptions import Timeout

# Define S3 bucket and file details
bucket_name = 'bi-visable'
file_key = 'cpv_data/properties/category_properties.csv'
output_csv_key = 'cpv_data/valueTexts/cpv_property_valuetexts.csv'  # Key for the output CSV file in S3
base_url = 'https://deapi.alibaba.com/openapi/param2/1/com.alibaba.v.business/category.info.listProp/195284?siteId=wlw&language={}&categoryId={}'
# Supported languages
languages = ['en', 'de', 'fr']

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_csv_from_s3(bucket_name, file_key):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')
        csv_reader = csv.reader(StringIO(csv_content), delimiter=';')
        return list(csv_reader)
    except Exception as e:
        logger.error("Error reading CSV from S3: %s", e)
        return []

def fetch_category_ids(csv_rows):
    """
    Extract category IDs from the CSV rows and return distinct category IDs as a list.
    """
    category_ids = set()  # Using a set to collect unique category IDs
    try:
        for row in csv_rows:
            category_ids.add(row[0])  # Add each category ID to the set
        return list(category_ids)  # Convert the set to a list and return
    except Exception as e:
        logger.error("Error fetching category IDs: %s", e)
        return []

def main():
    # Read CSV from S3
    csv_rows = read_csv_from_s3(bucket_name, file_key)
    if not csv_rows:
        print("No data found in the CSV file.")
        return

    # Fetch category IDs
    category_ids = fetch_category_ids(csv_rows)
    #category_ids = ["600093","600012"]
    if not category_ids:
        print("No category IDs found in the CSV file.")
        return

    # Sort category IDs
    category_ids.sort()

    # Total number of category IDs
    total_category_ids = len(category_ids)

    # Calculate batch size dynamically based on the total number of category IDs
    max_batch_size = 500  # Maximum batch size to ensure API limits are not exceeded
    # Adjust the batch size calculation based on the total number of category IDs
    batch_size = max(1, min(max_batch_size, total_category_ids // 10))  # Adjust the denominator as needed

    # Split category IDs into batches
    category_batches = [category_ids[i:i+batch_size] for i in range(0, len(category_ids), batch_size)]

    # Initialize dictionaries to store property texts for each language
    value_texts = {lang: {} for lang in languages}

    start_time = time.time()  # Record start time

    # Process each batch sequentially
    for batch in category_batches:
        process_batch(batch, value_texts)

    end_time = time.time()  # Record end time
    total_time = end_time - start_time # Calculate total time taken
    total_time_min = total_time // 60

    # Generate CSV content
    csv_content = generate_csv_content(value_texts)

    # Write the CSV content to S3 bucket
    write_csv_to_s3(bucket_name, output_csv_key, csv_content)

    logger.info("CSV file generated and uploaded to S3 successfully.")
    logger.info("Total time taken: %.2f minutes", total_time_min)  # Log total time taken

def process_batch(category_batch, value_texts):
    with ThreadPoolExecutor() as executor:
        for category_id in category_batch:
            logger.info("Processing category ID: %s", category_id)
            for lang in languages:
                url = base_url.format(lang, category_id)
                try:
                    response = requests.get(url, timeout=10)  # Set a timeout value (in seconds)
                    if response.status_code == 200:
                        data = response.json()
                        for property_data in data['data']:
                            if 'propertyValues' in property_data:
                                for value in property_data['propertyValues']:
                                    value_id = value.get('valueId')
                                    value_text = value.get('valueText')
                                    value_texts[lang][value_id] = value_text
                            else:
                                logger.warning("No property values found for category ID %s and language %s",category_id, lang)
                                continue  # Skip to the next iteration
                    else:
                        logger.error("Error fetching data for category ID %s and language %s. Status code: %s", category_id, lang, response.status_code)
                except Timeout:
                    logger.error("Timeout while fetching data for category ID %s and language %s", category_id, lang)
                    continue  # Continue with the next request

def generate_csv_content(value_texts):
    # Generate CSV content
    csv_content = StringIO()
    writer = csv.writer(csv_content, delimiter=';')
    # Write header
    writer.writerow(['valueId', 'valueText_en', 'valueText_de', 'valueText_fr'])
    # Write data
    for value_id in value_texts['en']:
        value_text_en = value_texts['en'].get(value_id, 'NA')
        value_text_de = value_texts['de'].get(value_id, 'NA')
        value_text_fr = value_texts['fr'].get(value_id, 'NA')
        writer.writerow([value_id, value_text_en, value_text_de, value_text_fr])
    return csv_content.getvalue()

def write_csv_to_s3(bucket_name, file_key, csv_content):
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_content.encode('utf-8'))
    except Exception as e:
        logger.error("Error writing CSV to S3: %s", e)

if __name__ == "__main__":
    main()

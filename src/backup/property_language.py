###########################################################################################################
# python 3.11
# property_language.py
# This script fetches properties in multiple languages from an API using leaf category IDs
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

# Define S3 bucket and file details
bucket_name = 'bi-visable'
file_key = 'cpv_data/properties/category_properties.csv'
output_csv_path = 'cpv_data/propertyNames/cpv_propertyNames.csv'
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

def write_csv_to_s3(bucket_name, file_key, data):
    s3_client = boto3.client('s3')
    try:
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer, delimiter=';')
        for row in data:
            csv_writer.writerow(row)

        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())
        logger.info("CSV file written to S3 successfully.")
    except Exception as e:
        logger.error("Error writing CSV to S3: %s", e)

def main():
    # Read CSV from S3
    csv_rows = read_csv_from_s3(bucket_name, file_key)
    if not csv_rows:
        logger.error("No data found in the CSV file.")
        return

    # Fetch category IDs
    category_ids = fetch_category_ids(csv_rows)
    if not category_ids:
        logger.error("No category IDs found in the CSV file.")
        return

    # Sort category IDs
    category_ids.sort()

    # Initialize dictionaries to store property texts for each language
    property_texts = {lang: {} for lang in languages}

    start_time = time.time()  # Record start time

    with ThreadPoolExecutor() as executor:
        for category_id in category_ids:
            logger.info("Processing category ID: %s", category_id)
            for lang in languages:
                url = base_url.format(lang, category_id)
                try:
                    response = requests.get(url)
                    if response.status_code == 200:
                        data = response.json()
                        for property_data in data['data']:
                            property_id = property_data.get('propertyId')
                            property_text = property_data.get('propertyText')
                            property_texts[lang][property_id] = property_text
                    else:
                        logger.error("Error fetching data for category ID %s and language %s. Status code: %s",
                                     category_id, lang, response.status_code)
                except Exception as e:
                    logger.error("Error fetching data for category ID %s and language %s: %s", category_id, lang, e)

    end_time = time.time()  # Record end time
    total_time = end_time - start_time  # Calculate total time taken
    total_time_min = total_time // 60

    # Prepare CSV data
    csv_data = [['propertyId', 'propertyText_en', 'propertyText_de', 'propertyText_fr']]
    for property_id in property_texts['en']:
        property_text_en = property_texts['en'].get(property_id, 'NA')
        property_text_de = property_texts['de'].get(property_id, 'NA')
        property_text_fr = property_texts['fr'].get(property_id, 'NA')
        csv_data.append([property_id, property_text_en, property_text_de, property_text_fr])

    # Write CSV data to S3
    write_csv_to_s3(bucket_name, output_csv_path, csv_data)

    logger.info("CSV file generated successfully.")
    logger.info("Total time taken: %.2f minutes", total_time_min)  # Log total time taken

if __name__ == "__main__":
    main()

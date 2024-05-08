###########################################################################################################
# python 3.11
# cpv_properties.py
# This script reads a CSV file containing level 3 category IDs from an S3 bucket and pass into api to fetch corresponding properties of the id.
# It uses concurrent requests and thread pooling for efficient execution.
# The fetched data includes category properties, which are then flattened and stored in a csv file format.
# 2024/04/23
##########################################################################################################
import pandas as pd
import boto3
import requests
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO, StringIO

# Parameters for API request
base_url = "https://deapi.alibaba.com/openapi/param2/1/com.alibaba.v.business"
site_id = "wlw"
language = "en"

# S3 bucket and keys
bucket_name = "bi-visable"
category_key = "cpv_data/categories/level3_cpv_category.csv"
property_key = "cpv_data/properties/category_properties.csv"

# Configure ThreadPool
max_workers = 30

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_leaf_category_info(category_id, base_url, site_id, language):
    url = f"{base_url}/category.info.listProp/195284"
    params = {
        "siteId": site_id,
        "language": language,
        "categoryId": category_id
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise exception for 4xx and 5xx status codes
        data = response.json()

        properties_data = []
        for item in data['data']:
            if 'propertyValues' in item:  # Check if propertyValues exists
                for value in item['propertyValues']:
                    prop_data = {
                        'categoryId': value['categoryId'],  # categoryId inside propertyValues
                        'propertyId': value['propertyId'],  # propertyId inside propertyValues
                        'propertyText': item['propertyText'],  # propertyText outside propertyValues
                        'showType': item['showType'],
                        'valueId': value['valueId'],  # valueId inside propertyValues
                        'valueText': value['valueText'],  # valueText inside propertyValues
                        'saleProp': item['saleProp'],
                        'required': item['required'],
                        'inputProp': item['inputProp'],
                        'hasUnit': item['hasUnit'],
                        'enumProp': item['enumProp'],
                    }
                    properties_data.append(prop_data)
            else:  # If propertyValues does not exist, append None values for valueId and valueText
                prop_data = {
                    'categoryId': item["categoryId"],
                    'propertyId': item['propertyId'],
                    'propertyText': item['propertyText'],
                    'showType': item['showType'],
                    'valueId': None,
                    'valueText': None,
                    'saleProp': item['saleProp'],
                    'required': item['required'],
                    'inputProp': item['inputProp'],
                    'hasUnit': item['hasUnit'],
                    'enumProp': item['enumProp'],
                }
                properties_data.append(prop_data)

        return properties_data
    except Exception as e:
        logger.error(f"Failed to fetch data for category ID {category_id}: {e}")
        return None

# Function to fetch data from S3
def fetch_data_from_s3(bucket, key, column):
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_data = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_data), delimiter=';')  # Read CSV data into DataFrame
        if column in df.columns:
            return df[column].tolist()  # Extract column values and return as list
        else:
            logger.error(f"Column '{column}' not found in the CSV file.")
            return None
    except Exception as e:
        logger.error(f"Failed to fetch data from S3: {e}")
        return None

# Function to write data to S3
def write_data_to_s3(data, bucket, key):
    try:
        s3 = boto3.client('s3')
        csv_buffer = data.to_csv(index=False, sep=';')  # Convert DataFrame to CSV format
        s3.put_object(Body=csv_buffer, Bucket=bucket, Key=key)
        logger.info("Data has been successfully written to S3.")
    except Exception as e:
        logger.error(f"Failed to write data to S3: {e}")


start_time = time.time()

# Read leaf category IDs from Parquet file in S3
leaf_category_ids = fetch_data_from_s3(bucket_name, category_key, "level4_ID")

if leaf_category_ids is not None:
    with ThreadPoolExecutor() as executor:
        # Function to execute API requests concurrently
        def execute_request(category_id):
            logger.info(f"Fetching data for category ID {category_id}")
            return fetch_leaf_category_info(category_id, base_url, site_id, language)

        # Concurrently fetch data for all leaf category IDs
        all_properties_data = list(executor.map(execute_request, leaf_category_ids))

    # Filter out None values (failed requests)
    all_properties_data = [data for sublist in all_properties_data if sublist for data in sublist]

    # Convert list of dictionaries to DataFrame
    flattened_df = pd.DataFrame(all_properties_data)

    # Write data to S3 as Parquet file
    write_data_to_s3(flattened_df, bucket_name, property_key)

    # Calculate and log the total time of execution
    end_time = time.time()
    total_time_seconds = end_time - start_time
    total_time_minutes = total_time_seconds / 60  # Convert seconds to minutes
    logger.info(f"Total time taken: {total_time_minutes:.2f} minutes")
else:
    logger.error("Failed to fetch leaf category IDs from S3.")

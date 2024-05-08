###########################################################################################################
# python 3.11
# cpv_categories.py
# This code fetches category data from the Alibaba API, processes it into hierarchical levels, and uploads the data to an S3 bucket.
# 2024/04/23
##########################################################################################################
import pandas as pd
import requests
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor
import boto3

# Alibaba API base URL and site ID
base_url = "https://deapi.alibaba.com/openapi/param2/1/com.alibaba.v.business"
site_id = "wlw"
language = "en"  # Default language

# S3 bucket details
s3_bucket_name = "bi-visable"
s3_folder_path = "cpv_data/categories/"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize S3 client
s3_client = boto3.client('s3')

def fetch_level1_data():
    """
        Fetches level 1 category data from API.
    """
    url = f"{base_url}/category.info.listTopCategories/195284"
    params = {"siteId": site_id, "language": language}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()["data"]
    except requests.exceptions.RequestException as e:
        logging.error("Error fetching level 1 data: %s", e)
        return []

def fetch_level2_data(level1_data):
    """
        Fetches and processes level 2 category data.
    """
    level2_rows = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for _, row in level1_data.iterrows():
            url = f"{base_url}/category.info.listSubCategories/195284"
            params = {"siteId": site_id, "language": language, "categoryId": row['categoryId']}
            futures.append(executor.submit(fetch_level2_row, url, params, row))
        for future in futures:
            level2_rows.extend(future.result())
    return level2_rows

def fetch_level2_row(url, params, row):
    """
        Fetches data for each row of level 2 categories.
    """
    level2_rows = []
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        level2_data = response.json()["data"]
        for category in level2_data:
            subcategories_response = requests.get(url, params={"siteId": site_id, "language": language,
                                                               "categoryId": category['categoryId']})
            subcategories_response.raise_for_status()
            subcategories_data = subcategories_response.json()["data"]
            level2_id = category['categoryId']
            if subcategories_data:
                # If level2_id has subcategories, it remains as level2_id
                level2_rows.append(
                    (row['categoryId'], row['categoryName'], level2_id, category['categoryName'], ''))
            else:
                # If level2_id doesn't have subcategories, it goes to level4_id
                level2_rows.append(
                    (row['categoryId'], row['categoryName'], '', '', level2_id, category['categoryName']))
    except requests.exceptions.RequestException as e:
        logging.error("Error fetching level 2 data for Category ID %s: %s", row['categoryId'], e)
    return level2_rows


def fetch_level3_data(level2_data):
    """
        Fetches and processes level 3 category data.
    """
    level3_rows = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for _, row in level2_data.iterrows():
            if row['level2_ID']:
                url = f"{base_url}/category.info.listSubCategories/195284"
                params = {"siteId": site_id, "language": language, "categoryId": row['level2_ID']}
                futures.append(executor.submit(fetch_level3_row, url, params, row))
            else:
                # If level2_ID is not present, directly include level1 and level4 information
                level3_rows.append((row['level1_ID'], row['level1_Name'], '', '', '', '', row['level4_ID'], row['level4_Name']))
                print("Warning: Found row without level2 data. Added level1 and level4 data directly to level3.")
        for future in futures:
            level3_rows.extend(future.result())

    # Sort level3_rows based on level1 ID
    level3_rows.sort(key=lambda x: x[0])
    return level3_rows

def fetch_level3_row(url, params, row):
    """
        Fetches data for each row of level 3 categories.
    """
    level3_rows = []
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        level3_data = response.json()["data"]
        for category in level3_data:
            subcategories_response = requests.get(url, params={"siteId": site_id, "language": language,
                                                               "categoryId": category['categoryId']})
            subcategories_response.raise_for_status()
            subcategories_data = subcategories_response.json()["data"]
            level3_id = category['categoryId']
            if subcategories_data:
                # If level3_id has further subcategories
                for subcategory in subcategories_data:
                    level4_id = subcategory['categoryId']
                    level3_rows.append((row['level1_ID'], row['level1_Name'], row['level2_ID'], row['level2_Name'],
                                        level3_id, category['categoryName'], level4_id, subcategory['categoryName']))
            else:
                # If level3_id doesn't have subcategories
                level3_rows.append((row['level1_ID'], row['level1_Name'], row['level2_ID'], row['level2_Name'],'', '',
                                    level3_id, category['categoryName']))
    except requests.exceptions.RequestException as e:
        logging.error("Error fetching level 3 data for Category ID %s: %s", row['level2_ID'], e)
    return level3_rows


if __name__ == "__main__":
    start_time = time.time()

    logging.info("Fetching level 1 data...")
    # Fetch level 1 data
    level1_data = fetch_level1_data()
    if level1_data:
        logging.info("Level 1 data fetched successfully.")
        # Convert level 1 data to DataFrame
        df_level1 = pd.DataFrame(level1_data, columns=["categoryId", "categoryName"])

        logging.info("Fetching and processing level 2 data...")
        # Fetch and process level 2 data
        level2_rows = fetch_level2_data(df_level1)
        df_level2 = pd.DataFrame(level2_rows,
                                 columns=["level1_ID", "level1_Name", "level2_ID", "level2_Name", "level4_ID",
                                          "level4_Name"])

        logging.info("Fetching and processing level 3 data...")
        # Fetch and process level 3 data
        level3_rows = fetch_level3_data(df_level2)
        df_level3 = pd.DataFrame(level3_rows,
                                 columns=["level1_ID", "level1_Name", "level2_ID", "level2_Name", "level3_ID",
                                          "level3_Name", "level4_ID", "level4_Name"])

        logging.info("Uploading DataFrames to S3...")
        # Upload DataFrames to S3
        for df, filename in zip([df_level1, df_level2, df_level3], ["level1_cpv_category.csv", "level2_cpv_category.csv", "level3_cpv_category.csv"]):
            csv_buffer = df.to_csv(index=False, sep=";")
            s3_key = s3_folder_path + filename
            s3_client.put_object(Body=csv_buffer, Bucket=s3_bucket_name, Key=s3_key)
            logging.info(f"{filename} uploaded successfully to S3.")

    end_time = time.time()
    total_time_seconds = end_time - start_time
    total_time_minutes = total_time_seconds / 60  # Convert seconds to minutes
    logging.info(f"Total time taken: {total_time_minutes:.2f} minutes")

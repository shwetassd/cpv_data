import csv
from io import StringIO

import boto3
import requests
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time
from requests.exceptions import RequestException

# Hardcoded variables
BASE_URL = "https://deapi.alibaba.com/openapi/param2/1/com.alibaba.v.business/category.info.listProp/195284"
SITE_ID = "wlw"
LANGUAGES = ['en', 'de', 'fr']
S3_BUCKET_NAME = 'bi-visable'
S3_FILE_PATH = 'cpv_data/properties/category_properties.csv'
OUTPUT_CSV_PATH = 'cpv_data/propertyNames/cpv_propertyNames.csv'
#OUTPUT_CSV_PATH = 'output/cpv_propertiesName.csv'
DELIMITER = ';'
THREAD_POOL_SIZE = 20  # Adjust as needed
MAX_RETRIES = 3

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

s3_client = boto3.client('s3')

# Function to fetch property values for a category ID and language with retry mechanism
def fetch_property_values_with_retry(category_id, language):
    for _ in range(MAX_RETRIES):
        try:
            return fetch_property_values(category_id, language)
        except RequestException as e:
            logging.warning(f"Failed to fetch property values for category ID {category_id} and language {language}. Retrying...")
    raise Exception(f"Failed to fetch property values for category ID {category_id} and language {language} after {MAX_RETRIES} retries.")

# Function to fetch property values for a category ID and language
def fetch_property_values(category_id, language):
    logging.info(f"Fetching property values for category ID {category_id} in language {language}")
    params = {
        "siteId": SITE_ID,
        "language": language,
        "categoryId": category_id
    }
    response = requests.get(BASE_URL, params=params)

    if response.status_code == 200:
        data = response.json().get("data", [])
        property_values = []
        for item in data:
            if item.get("enumProp", False):
                for value in item.get("propertyValues", []):
                    property_values.append({
                        'categoryId': category_id,
                        'valueId': value.get("valueId"),  # Check if valueId is present
                        'language': language,
                        'propertyText': item["propertyText"],
                        'valueText': value.get("valueText")  # Check if valueText is present
                    })
            else:
                # Check if propertyValues exist before appending
                if item.get("propertyValues"):
                    for value in item.get("propertyValues", []):
                        property_values.append({
                            'categoryId': category_id,
                            'valueId': value.get("valueId"),  # Check if valueId is present
                            'language': language,
                            'propertyText': item["propertyText"],
                            'valueText': value.get("valueText")  # Check if valueText is present
                        })
                else:
                    property_values.append({
                        'categoryId': category_id,
                        'language': language,
                        'propertyText': item["propertyText"],
                        'valueId': None,
                        'valueText': None
                    })
        return property_values
    else:
        logging.error(f"Failed to fetch property values for category ID {category_id} and language {language}")
        raise RequestException(f"Failed to fetch property values for category ID {category_id} and language {language}")

# Function to fetch property values for all category IDs and languages
def fetch_all_property_values(category_ids, languages):
    all_property_values = []
    with ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE) as executor:
        futures = []
        for category_id in category_ids:
            for language in languages:
                futures.append(executor.submit(fetch_property_values, category_id, language))
        for future in futures:
            all_property_values.extend(future.result())
    return all_property_values

def main():
    try:
        start_time = time.time()

        # Read category IDs from the CSV file in the S3 bucket
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_FILE_PATH)
        df_category = pd.read_csv(response['Body'], delimiter=DELIMITER)

        # Extract unique category IDs
        category_ids = df_category['categoryId'].unique().tolist()

        #category_ids = ["604835"]

        # Fetch property values for all category IDs and languages
        all_property_values = fetch_all_property_values(category_ids, LANGUAGES)

        # Convert to DataFrame
        df = pd.DataFrame(all_property_values)

        # Filter out rows where valueId is not null
        df_filtered = df[df['valueId'].notnull()]

        # Filter out rows where valueId is null for any category ID
        df_filtered = df_filtered.groupby('categoryId').filter(lambda x: x['valueId'].notnull().all())

        if not df_filtered.empty:
            # Pivot DataFrame to have separate columns for each language
            df_pivot = df_filtered.pivot_table(index=['categoryId', 'valueId'],
                                               columns='language',
                                               values=['propertyText', 'valueText'],
                                               aggfunc='first').reset_index()

            # Flatten column names
            df_pivot.columns = [col[0] + '_' + col[1] if col[1] != '' else col[0] for col in df_pivot.columns]

            # Reorder columns
            columns_order = ['categoryId', 'valueId',
                             'propertyText_en', 'valueText_en',
                             'propertyText_de', 'valueText_de',
                             'propertyText_fr', 'valueText_fr']
            df_pivot = df_pivot[columns_order]
        else:
            # Create placeholder rows for category IDs and propertyText
            placeholder_rows = []
            for category_id in category_ids:
                for language in LANGUAGES:
                    placeholder_rows.append({
                        'categoryId': category_id,
                        'language': language,
                        'propertyText': f'Placeholder for Category {category_id} in {language}',
                        'valueId': None,
                        'valueText': None
                    })
            df_pivot = pd.DataFrame(placeholder_rows)

        # Save DataFrame to CSV file
        #df_pivot.to_csv(OUTPUT_CSV_PATH, index=False)

        # Save DataFrame to CSV file in S3 bucket
        csv_buffer = StringIO()
        df_pivot.to_csv(csv_buffer, index=False, sep=';')

        # Upload CSV data to S3 bucket
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=OUTPUT_CSV_PATH,
            Body=csv_buffer.getvalue()
        )

        elapsed_time = time.time() - start_time
        logging.info(f"Property values written to {OUTPUT_CSV_PATH}. Total execution time: {elapsed_time} seconds")
    except Exception as e:
        logging.error(f"Failed to process category IDs: {e}")

if __name__ == "__main__":
    main()
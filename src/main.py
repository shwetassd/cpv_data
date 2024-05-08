import requests
import os
import pandas as pd
import json 
import logging
import time
import concurrent.futures
import boto3
from io import StringIO

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variables
BASE_URL = 'https://deapi.alibaba.com/openapi/param2/1/com.alibaba.v.business/category.info.listTopCategories/195284?siteId=wlw&language='
LANGUAGES = ['en', 'de', 'fr']
CATEGORY_OUTPUTFILE = 'categories_data.csv'
PROPERTY_OUTPUTFILE = 'category_properties_data.csv'

# Upload CSV file to S3
s3_client = boto3.client('s3')
s3_bucket_name = 'bi-visable'
s3_category_key = 'cpv_data/category/' + CATEGORY_OUTPUTFILE
s3_property_key = 'cpv_data/property/' + PROPERTY_OUTPUTFILE

def format_category_paths(categoryIdPath, categoryNamePath, finalCategoryId, finalCategoryName):
    """
    Format category paths into a dictionary with specific naming conventions for each level.

    Args:
    - categoryIdPath (list): A list of category IDs.
    - categoryNamePath (list): A list of category names corresponding to the IDs.
    - finalCategoryId (str): The ID of the final category.
    - finalCategoryName (str): The name of the final category.

    Returns:
    - dict: A dictionary representing the category hierarchy.
    """
    # Initialize the output dictionary
    output = {'topCategoryId': categoryIdPath[0], 'topCategoryName': categoryNamePath[0]}

    # Process each category in the path
    for index, (cat_id, cat_name) in enumerate(zip(categoryIdPath[1:], categoryNamePath[1:]), start=2):
        # For the last available subcategory level, check if there are no more subcategories
        if index == len(categoryIdPath):
            # Assign the final category ID and name
            output['leafCategoryId'] = finalCategoryId
            output['leafCategoryName'] = finalCategoryName
        else:
            # For other levels, use the standard naming convention
            output[f'subCategoryLevel{index-1}Id'] = cat_id
            output[f'subCategoryLevel{index-1}Name'] = cat_name

    return output

def fetch_categories(url, language):
    """
    Recursive function to fetch all categories until a leaf category is found.

    Args:
    - url (str): The starting URL for fetching category data.
    - language (str): The language of the categories.

    Returns:
    - list: A list of all categories including leaf categories.
    """
    result = []  # Initialize result list for each language

    def recursive_fetch(url):
        # Send the HTTP request
        response = requests.get(url)

        if response.status_code == 200:
            try:
                # Parse the JSON response
                data = response.json()
                categories = data['data']  # Adjust this depending on the actual key that contains the categories

                for category_data in categories:
                    # If it's not a leaf category, recurse into its subcategories
                    if not category_data['leafCategory']:
                        sub_url = f"https://deapi.alibaba.com/openapi/param2/1/com.alibaba.v.business/category.info.listSubCategories/195284?siteId=wlw&language={language}&categoryId={category_data['categoryId']}"
                        recursive_fetch(sub_url)  # Recursively call with the same language
                    else:
                        final_category_id = category_data['categoryId']
                        final_category_name = category_data['categoryName']
                        formatted_output = format_category_paths(category_data['categoryIdPath'],
                                                                 category_data['categoryNamePath'],
                                                                 final_category_id,
                                                                 final_category_name)
                        result.append(formatted_output)
            except KeyError as e:
                logging.error(f"Error parsing data: Missing key {e}")
            except json.JSONDecodeError:
                logging.error("Error decoding JSON response from the API")
        else:
            logging.error(f"Failed to retrieve data: HTTP {response.status_code}")

    # Start recursive fetching
    recursive_fetch(url)

    return result

def fetch_category_ids(api_url):
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()
            category_ids = [category['categoryId'] for category in data.get('data', [])]
            return category_ids
        else:
            logging.error(f"Failed to fetch data: Status code {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred: {e}")
    except ValueError:
        logging.error("Failed to parse JSON response")
    return []

def fetch_all_categories(language, category_ids):
    """
    Fetch category data for all category IDs in parallel.

    Args:
    - language (str): The language of the categories.
    - category_ids (list): List of category IDs.

    Returns:
    - list: A list of all categories including leaf categories.
    """
    category_data = []
    
    def fetch_category_data(category_id):
        start_url = f"https://deapi.alibaba.com/openapi/param2/1/com.alibaba.v.business/category.info.listSubCategories/195284?siteId=wlw&language={language}&categoryId={category_id}"
        return fetch_categories(start_url, language)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_category_data, category_id) for category_id in category_ids]
        for future in futures:
            category_data.extend(future.result())

    return category_data

def flatten_properties(data):
    properties = []
    if isinstance(data, list):
        for item in data:
            if 'propertyValues' in item:  # Check if propertyValues exists
                for value in item['propertyValues']:
                    prop_data = {
                        column: item[column] for column in item if column != 'propertyValues'
                    }
                    prop_data.update({
                        column: value[column] for column in value
                    })
                    properties.append(prop_data)
            else:  # If propertyValues does not exist, append None values for valueId and valueText
                prop_data = {
                    column: item[column] for column in item
                }
                properties.append(prop_data)
    elif isinstance(data, dict):
        if 'propertyValues' in data:  # Check if propertyValues exists
            for value in data['propertyValues']:
                prop_data = {
                    column: data[column] for column in data if column != 'propertyValues'
                }
                prop_data.update({
                    column: value[column] for column in value
                })
                properties.append(prop_data)
        else:  # If propertyValues does not exist, append None values for valueId and valueText
            prop_data = {
                column: data[column] for column in data
            }
            properties.append(prop_data)
    return properties

def fetch_properties_for_language(language, leaf_category_ids):
    base_url = 'https://deapi.alibaba.com/openapi/param2/1/com.alibaba.v.business'
    site_id = 'wlw'
    properties = []
    for leaf_category_id in leaf_category_ids:
        url = f"{base_url}/category.info.listProp/195284"
        params = {
            "siteId": site_id,
            "language": language,
            "categoryId": leaf_category_id
        }
        logging.info(f"Fetching properties for leaf category ID {leaf_category_id} in language {language}")
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            flattened_properties = flatten_properties(data.get('data', []))
            properties.extend(flattened_properties)
        else:
            logging.error(f"Failed to fetch properties for leaf category ID {leaf_category_id} in language {language}")
    return properties

def fetch_category_properties(leaf_category_ids):
    prop_language_dfs = {}
    total_time_start = time.time()
    retry_count = 3  # Number of retries in case of timeout
    retry_delay = 5  # Delay between retries in seconds
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Fetch category properties for each language concurrently
        future_to_language = {executor.submit(fetch_properties_for_language, language, leaf_category_ids): language for language in LANGUAGES}
        for future in concurrent.futures.as_completed(future_to_language):
            language = future_to_language[future]
            try:
                properties = future.result()
                # Create DataFrame for the language
                properties_df = pd.DataFrame(properties)
                for column in properties_df.columns:
                    if column.endswith('Text'):
                        properties_df.rename(columns={column: f"{column}_{language}"}, inplace=True)
                prop_language_dfs[language] = properties_df
            except Exception as exc:
                logging.error(f"Exception occurred while fetching properties for language {language}: {exc}")
                if retry_count > 0:
                    logging.info(f"Retrying for language {language} after {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_count -= 1
                    future_to_language[executor.submit(fetch_properties_for_language, language, leaf_category_ids)] = language
                else:
                    logging.error(f"Retry limit exceeded for language {language}.")
    total_time_end = time.time()
    total_time_taken = total_time_end - total_time_start
    logging.info(f"Total time taken: {total_time_taken} seconds")
    return prop_language_dfs

def merge_category_properties(prop_language_dfs):
    # List to store DataFrames for each language
    dfs = []

    # Read CSV files for each language and append to the list
    for language in prop_language_dfs:
        dfs.append(prop_language_dfs[language])

    # Merge DataFrames based on 'categoryId', 'valueId', and 'propertyId'
    merged_df = dfs[0]  # Start with the first DataFrame
    for df in dfs[1:]:
        # Merge on common columns
        merged_df = pd.merge(merged_df, df, on=['categoryId', 'valueId', 'propertyId'], how='outer', suffixes=('', f'_{df.columns[1]}'))

    # Reorder columns
    column_order = ['categoryId', 'valueId', 'propertyId', 'saleProp', 'required', 'inputProp', 'enumProp', 'hasUnit', 'showType', 'multiSelect']
    for df in dfs:
        for col in df.columns:
            if col.endswith(('_en', '_de', '_fr')):
                lang_code = col[-2:]
                prefix = col[:-3]  # Remove the language suffix
                prefix_col_name = f'{prefix}_'
                if prefix_col_name not in column_order:
                    if prefix_col_name in merged_df.columns:
                        column_order.append(prefix_col_name)  # Add the prefix without language code
                    else:
                        logging.warning(f"Prefix column '{prefix_col_name}' not found in DataFrame.")
                column_order.append(col)  # Add the language-specific column

    merged_df = merged_df[column_order]

    # Convert DataFrame to CSV format with ';' delimiter
    csv_buffer = merged_df.to_csv(sep=';', index=False)
    s3_client.put_object(Body=csv_buffer, Bucket=s3_bucket_name, Key=s3_property_key)
    logging.info(f"Merged Category file uploaded to S3 bucket: s3://{s3_bucket_name}/{s3_property_key}")

def main():
    start_time = time.time()  # Record the start time

    base_url = BASE_URL
    languages = LANGUAGES
    
    # Initialize a dictionary to store category DataFrames for each language
    language_dfs = {}

    for language in languages:
        url = base_url + language
        category_ids = fetch_category_ids(url)
        language_categories = fetch_all_categories(language, category_ids)
        
        # Store category data in a DataFrame for each language
        df = pd.DataFrame(language_categories)
        
        # Rename columns ending with 'Name' to have a suffix with the language code
        for col in df.columns:
            if col.endswith('Name'):
                df.rename(columns={col: f"{col}_{language}"}, inplace=True)
        
        language_dfs[language] = df

    # Merge DataFrames for all languages
    merged_df = pd.DataFrame()
    for language, df in language_dfs.items():
        if merged_df.empty:
            merged_df = df
        else:
            # Merge on 'topCategoryId', 'subCategoryLevel1Id', 'subCategoryLevel2Id', and 'leafCategoryId'
            merged_df = pd.merge(merged_df, df, on=['topCategoryId', 'subCategoryLevel1Id', 'subCategoryLevel2Id', 'leafCategoryId'], how='outer', suffixes=('', f'_{language}'))

    # Remove duplicate columns
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

    # Convert DataFrame to CSV format with ';' delimiter
    csv_buffer = merged_df.to_csv(sep=';', index=False)
    s3_client.put_object(Body=csv_buffer, Bucket=s3_bucket_name, Key=s3_category_key)
            
    logging.info(f"Merged Category file uploaded to S3 bucket: s3://{s3_bucket_name}/{s3_category_key}")

    # Extract leaf category IDs from merged DataFrame and sort them
    leaf_category_ids = sorted(merged_df['leafCategoryId'].unique().tolist())
    
    # Fetch category properties for each leaf category ID
    prop_language_dfs = fetch_category_properties(leaf_category_ids)
    
    # Merge category properties for different languages
    merge_category_properties(prop_language_dfs)

    end_time = time.time()  # Record the end time
    total_time = end_time - start_time
    logging.info(f"Total time taken: {total_time:.2f} seconds")

if __name__ == '__main__':
   main()
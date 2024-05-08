import logging
import pandas as pd
import requests
import concurrent.futures
import time

# Configure logging
logging.basicConfig(level=logging.INFO)

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
    languages = ['en', 'de', 'fr']
    language_dfs = {}
    total_time_start = time.time()
    retry_count = 3  # Number of retries in case of timeout
    retry_delay = 5  # Delay between retries in seconds
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Fetch category properties for each language concurrently
        future_to_language = {executor.submit(fetch_properties_for_language, language, leaf_category_ids): language for language in languages}
        for future in concurrent.futures.as_completed(future_to_language):
            language = future_to_language[future]
            try:
                properties = future.result()
                # Create DataFrame for the language
                properties_df = pd.DataFrame(properties)
                for column in properties_df.columns:
                    if column.endswith('Text'):
                        properties_df.rename(columns={column: f"{column}_{language}"}, inplace=True)
                language_dfs[language] = properties_df
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
    return language_dfs

def merge_category_properties(language_dfs):
    # List to store DataFrames for each language
    dfs = []

    # Read CSV files for each language and append to the list
    for language in language_dfs:
        dfs.append(language_dfs[language])

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

    # Write the merged DataFrame to a CSV file
    merged_csv_file_path = './merged_category_properties.csv'
    merged_df.to_csv(merged_csv_file_path, index=False)
    logging.info(f"Merged category properties CSV file created at: {merged_csv_file_path}")

def main():
    merged_df = pd.read_csv('./categories_data.csv')

    # Extract leaf category IDs from merged DataFrame and sort them
    leaf_category_ids = sorted(merged_df['leafCategoryId'].unique().tolist())
    
    # Fetch category properties for each leaf category ID
    language_dfs = fetch_category_properties(leaf_category_ids)
    
    # Merge category properties for different languages
    merge_category_properties(language_dfs)

if __name__ == '__main__':
    main()

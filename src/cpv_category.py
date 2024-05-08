import requests
import pandas as pd
import json 
from concurrent.futures import ThreadPoolExecutor

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
                print(f"Error parsing data: Missing key {e}")
            except json.JSONDecodeError:
                print("Error decoding JSON response from the API")
        else:
            print(f"Failed to retrieve data: HTTP {response.status_code}")

    # Start recursive fetching
    recursive_fetch(url)

    return result

def export_to_csv(category_data, language):
    """
    Export category data to a CSV file for a specific language.

    Args:
    - category_data (list): List of category dictionaries.
    - language (str): Language of the categories.
    """
    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(category_data)

    # Rename columns ending with 'Name' to have a suffix with the language code
    for col in df.columns:
        if col.endswith('Name'):
            df.rename(columns={col: f"{col}_{language}"}, inplace=True)

    # Reorder columns to move 'leafCategoryId' and 'leafCategoryName' to the end
    columns = df.columns.tolist()
    df = df[[col for col in columns if col not in ['leafCategoryId', f"leafCategoryName_{language}"]] + ['leafCategoryId', f"leafCategoryName_{language}"]]

    # Write the DataFrame to a CSV file
    csv_file_path = f'./categories_{language}_data_output.csv'
    df.to_csv(csv_file_path, index=False)
    print(f"CSV file for language '{language}' created at: {csv_file_path}")

def fetch_category_ids(api_url):
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()
            category_ids = [category['categoryId'] for category in data.get('data', [])]
            return category_ids
        else:
            print(f"Failed to fetch data: Status code {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    except ValueError:
        print("Failed to parse JSON response")
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

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_category_data, category_id) for category_id in category_ids]
        for future in futures:
            category_data.extend(future.result())

    return category_data

def merge_csv_files(languages):
    """
    Merge CSV files for different languages into one.

    Args:
    - languages (list): List of languages.

    Returns:
    - DataFrame: Merged DataFrame containing data for all languages.
    """
    # Initialize an empty list to store DataFrames for each language
    dfs = []

    # Read CSV files for each language and append to the list
    for language in languages:
        file_path = f'./categories_{language}_data_output.csv'
        df = pd.read_csv(file_path)
        dfs.append(df)

    # Merge DataFrames based on 'topCategoryId', 'subcategoryLevel1Id', 'subCategoryLevel2Id', and 'leafCategoryId'
    merged_df = dfs[0]  # Start with the first DataFrame
    for df in dfs[1:]:
        merged_df = pd.merge(merged_df, df, on=['topCategoryId', 'subCategoryLevel1Id', 'subCategoryLevel2Id', 'leafCategoryId'], how='outer', suffixes=('', f'_{df.columns[1]}'))

    # Reorder columns
    merged_df = merged_df[['topCategoryId', 'topCategoryName_en', 'topCategoryName_de', 'topCategoryName_fr',
                           'subCategoryLevel1Id', 'subCategoryLevel1Name_en', 'subCategoryLevel1Name_de', 'subCategoryLevel1Name_fr',
                           'subCategoryLevel2Id', 'subCategoryLevel2Name_en', 'subCategoryLevel2Name_de', 'subCategoryLevel2Name_fr',
                           'leafCategoryId', 'leafCategoryName_en', 'leafCategoryName_de', 'leafCategoryName_fr']]

    return merged_df

def main():
    base_url = 'https://deapi.alibaba.com/openapi/param2/1/com.alibaba.v.business/category.info.listTopCategories/195284?siteId=wlw&language='
    languages = ['en','de','fr']
    
    for language in languages:
        url = base_url + language
        category_ids = fetch_category_ids(url)
        language_categories = fetch_all_categories(language, category_ids)
        export_to_csv(language_categories, language)

    # Merge CSV files for all languages
    merged_df = merge_csv_files(languages)
    
    # Write the merged DataFrame to a CSV file
    merged_csv_file_path = './merged_categories_data.csv'
    merged_df.to_csv(merged_csv_file_path, index=False)
    print(f"Merged CSV file created at: {merged_csv_file_path}")

if __name__ == '__main__':
    main()
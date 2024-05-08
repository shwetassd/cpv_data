import requests
import pandas as pd


def format_category_paths(categoryIdPath, categoryNamePath):
    """
    Format category paths into a list of dictionaries with specific naming conventions for each level.

    Args:
    - categoryIdPath (list): A list of category IDs.
    - categoryNamePath (list): A list of category names corresponding to the IDs.

    Returns:
    - list: A formatted list of dictionaries representing the category hierarchy.
    """
    # Initialize the output list
    output = []

    # Ensure both input lists have the same length to avoid indexing errors
    if len(categoryIdPath) != len(categoryNamePath):
        raise ValueError("Category ID path and name path must be of the same length")

    # Process each category in the path
    for index, (cat_id, cat_name) in enumerate(zip(categoryIdPath, categoryNamePath)):
        # For the first item, use "parent_id" and "parent_cat_name"
        if index == 0:
            entry = {'topCategoryId': cat_id, 'topCategoryName': cat_name}

        # For subsequent items, use "leafnode_X_id" and "leafcat_X_name"
        else:
            entry = {f'subCategoryLevel{index+1}Id': cat_id, f'subCategoryLevel{index+1}Name': cat_name}
        # Append the formatted entry to the output list
        output.append(entry)

    return output


def fetch_categories(url, result=[]):
    """
    Recursive function to fetch all categories until a leaf category is found.

    Args:
    - url (str): The starting URL for fetching category data.
    - result (list): Accumulates the categories information.

    Returns:
    - list: A list of all categories including leaf categories.
    """
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
                    sub_url = f"https://deapi.alibaba.com/openapi/param2/1/com.alibaba.v.business/category.info.listSubCategories/195284?siteId=wlw&language=en&categoryId={category_data['categoryId']}"
                    fetch_categories(sub_url, result)
                else:
                    formatted_output = format_category_paths(category_data['categoryIdPath'],
                                                             category_data['categoryNamePath'])
                    result.append(formatted_output)


        except KeyError as e:
            print(f"Error parsing data: Missing key {e}")
        except json.JSONDecodeError:
            print("Error decoding JSON response from the API")
    else:
        print(f"Failed to retrieve data: HTTP {response.status_code}")

    return result

def merge_dicts_and_export_csv(input_list, csv_file_path='./catagories_final_data_output.csv'):
    """
    Merges dictionaries within each sublist of a list of list of dictionaries, converts the result to a DataFrame,
    and then exports it to a CSV file.

    Parameters:
    - input_list: List[List[Dict]]. A list of lists containing dictionaries to be merged.
    - csv_file_path: str. The path where the CSV file will be saved.

    Returns:
    - csv_file_path: str. The path to the saved CSV file.
    """
    # Use a list comprehension to merge dictionaries vertically within each sublist
    output_list = [{k: v for d in sublist for k, v in d.items()} for sublist in input_list]

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(output_list)

    # Write the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=False)

    return csv_file_path


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


def main():
    url = 'https://deapi.alibaba.com/openapi/param2/1/com.alibaba.v.business/category.info.listTopCategories/195284?siteId=wlw&language=en'
    category_ids = fetch_category_ids(url)
    print(category_ids)

    """
    Now shweta you have to use list of top cat url mentioned below and iterate using loop and pass it for all catgories one by one
    and then you can generate the final output do contact me in case of any quries :)


    reference url: https://deapi.alibaba.com/openapi/param2/1/com.alibaba.v.business/category.info.listTopCategories/195284?siteId=wlw&language=en
    """
    # Example usage
    all_categories = []
    for category_id in category_ids[:3]:
        start_url = f"https://deapi.alibaba.com/openapi/param2/1/com.alibaba.v.business/category.info.listSubCategories/195284?siteId=wlw&language=en&categoryId={category_id}"
        all_categories += fetch_categories(start_url)
        print(f'All Category list for {category_id}: ', all_categories)
    merge_dicts_and_export_csv(all_categories)

if __name__ == '__main__':
    main()
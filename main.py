import pandas as pd
import concurrent.futures
import os
import json
import requests
import yaml
import ipaddress
import logging
from urllib.error import HTTPError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_yaml_from_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        yaml_data = yaml.safe_load(response.text)
        return yaml_data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching YAML from {url}: {e}")
        raise

def read_list_from_url(url):
    try:
        df = pd.read_csv(url, header=None, names=['pattern', 'address', 'other'], on_bad_lines='warn')
        return df
    except HTTPError as e:
        logging.error(f"HTTP error when reading CSV from {url}: {e.code}, {e.reason}")
        raise
    except pd.errors.ParserError as e:
        logging.error(f"Parsing error when reading CSV from {url}: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error when reading CSV from {url}: {e}")
        raise

# ... [rest of the functions remain unchanged] ...

def parse_list_file(link, output_directory):
    logging.info(f"Processing link: {link}")
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = list(executor.map(parse_and_convert_to_dataframe, [link]))
        df = pd.concat(results, ignore_index=True)

        # ... [rest of the function remains unchanged] ...

        logging.info(f"Successfully processed {link}")
        return file_name
    except Exception as e:
        logging.error(f"Error processing {link}: {e}")
        raise

# Main execution
if __name__ == "__main__":
    try:
        with open("../links.txt", 'r') as links_file:
            links = links_file.read().splitlines()

        links = [l for l in links if l.strip() and not l.strip().startswith("#")]

        output_dir = "./"
        result_file_names = []

        for link in links:
            try:
                result_file_name = parse_list_file(link, output_directory=output_dir)
                result_file_names.append(result_file_name)
            except Exception as e:
                logging.error(f"Failed to process {link}: {e}")

        logging.info("Processing complete")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

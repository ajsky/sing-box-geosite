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
        response = requests.get(url)
        response.raise_for_status()
        content = response.text
        return content
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching content from {url}: {e}")
        raise

def parse_and_convert_to_dataframe(content, file_type):
    lines = content.strip().split('\n')
    data = []
    
    if file_type == 'bilibili':
        for line in lines:
            if line.startswith('host'):
                parts = line.split(',')
                if len(parts) >= 2:
                    data.append({'type': 'host', 'value': parts[1]})
    elif file_type == 'wechat':
        for line in lines:
            if not line.startswith('#') and line.strip():
                data.append({'rule': line.strip()})
    elif file_type == 'microsoft':
        for line in lines:
            if line.startswith('HOST'):
                parts = line.split(',')
                if len(parts) >= 3:
                    data.append({'type': parts[0], 'host': parts[1], 'group': parts[2]})
    else:
        logging.warning(f"Unknown file type: {file_type}")
    
    return pd.DataFrame(data)

def parse_list_file(link, output_directory):
    logging.info(f"Processing link: {link}")
    try:
        content = read_list_from_url(link)
        
        if 'Bilibili.list' in link:
            file_type = 'bilibili'
        elif 'WeChat.list' in link:
            file_type = 'wechat'
        elif 'Microsoft.list' in link:
            file_type = 'microsoft'
        else:
            file_type = 'unknown'
        
        df = parse_and_convert_to_dataframe(content, file_type)

        file_name = os.path.join(output_directory, os.path.basename(link))
        df.to_csv(file_name, index=False)

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

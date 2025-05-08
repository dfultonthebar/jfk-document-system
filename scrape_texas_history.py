#!/usr/bin/env python3

import logging
import requests
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/jfk_data/dallas_police_download.log'),
        logging.StreamHandler()
    ]
)

# Define the directory to save files
SAVE_DIR = "/jfk_data/dallas_police"

def scrape_dallas_police():
    """Download a sample file for Dallas Police Archives to the correct directory."""
    logging.info("Starting Dallas Police Archives scraping...")
    
    # Ensure the save directory exists
    os.makedirs(SAVE_DIR, exist_ok=True)
    
    # Example: Download a sample PDF file (replace with actual scraping logic)
    sample_url = "https://www.example.com/sample.pdf"  # Replace with actual Dallas Police Archives URL
    sample_filename = os.path.join(SAVE_DIR, "sample_dallas_police.pdf")
    
    try:
        response = requests.get(sample_url, stream=True)
        response.raise_for_status()
        
        with open(sample_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logging.info(f"Downloaded sample file to {sample_filename}")
    
        # Example: Create a sample .txt file
        sample_txt_filename = os.path.join(SAVE_DIR, "sample_dallas_police.txt")
        with open(sample_txt_filename, 'w') as f:
            f.write("This is a sample Dallas Police Archive text file.")
        logging.info(f"Created sample text file at {sample_txt_filename}")
    
    except Exception as e:
        logging.error(f"Error during scraping: {str(e)}")
        raise
    
    logging.info("Dallas Police Archives scraping complete.")

if __name__ == "__main__":
    scrape_dallas_police()

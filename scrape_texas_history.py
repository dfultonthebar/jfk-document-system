#!/usr/bin/env python3

import logging
import requests
import os
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

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
BASE_URL = "https://texashistory.unt.edu/explore/collections/JFKDP/browse/"

def scrape_dallas_police():
    """Scrape and download PDF files from the Dallas Police Archives on the Portal to Texas History."""
    logging.info("Starting Dallas Police Archives scraping...")
    
    # Ensure the save directory exists
    os.makedirs(SAVE_DIR, exist_ok=True)
    
    # Start with the first page of the collection
    page_url = BASE_URL
    page_num = 1
    downloaded_files = 0
    max_pages = 10  # Limit to 10 pages to avoid excessive downloads; adjust as needed
    
    while page_url and page_num <= max_pages:
        try:
            logging.info(f"Scraping page {page_num}: {page_url}")
            # Fetch the collection page
            response = requests.get(page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all document links with 'metapth' identifiers
            doc_links = soup.find_all('a', href=re.compile(r'/ark:/67531/metapth\d+'))
            for link in doc_links:
                doc_url = urljoin(BASE_URL, link['href'])
                metapth_id = doc_url.split('/')[-2]  # Extract the metapth ID (e.g., metapth338772)
                
                # Construct the PDF URL
                pdf_url = f"{doc_url}m1/1/?layout=pdf"
                pdf_filename = os.path.join(SAVE_DIR, f"{metapth_id}.pdf")
                
                # Skip if the file already exists
                if os.path.exists(pdf_filename) and os.path.getsize(pdf_filename) > 0:
                    logging.info(f"Skipping existing file: {pdf_filename}")
                    continue
                
                # Download the PDF
                logging.info(f"Downloading {pdf_url} to {pdf_filename}...")
                try:
                    pdf_response = requests.get(pdf_url, stream=True)
                    pdf_response.raise_for_status()
                    
                    with open(pdf_filename + ".tmp", 'wb') as f:
                        for chunk in pdf_response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    os.rename(pdf_filename + ".tmp", pdf_filename)
                    file_size = os.path.getsize(pdf_filename) / (1024 ** 2)  # Convert to MB
                    logging.info(f"Downloaded {pdf_filename} ({file_size:.2f} MB)")
                    downloaded_files += 1
                except Exception as e:
                    logging.error(f"Failed to download {pdf_url}: {str(e)}")
                    continue
            
            # Find the "Next" page link
            next_link = soup.find('a', string=re.compile(r'Next', re.I))
            if next_link and 'href' in next_link.attrs:
                page_url = urljoin(BASE_URL, next_link['href'])
                page_num += 1
            else:
                page_url = None  # No more pages
        except Exception as e:
            logging.error(f"Error scraping page {page_num}: {str(e)}")
            break
    
    logging.info(f"Dallas Police Archives scraping complete. Downloaded {downloaded_files} files.")

if __name__ == "__main__":
    try:
        scrape_dallas_police()
    except Exception as e:
        logging.error(f"Scraping failed: {str(e)}")
        exit(1)

#!/usr/bin/env python3

import logging
import requests
import os
import re
import time
import json
import shutil
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from PIL import Image
import subprocess

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
SPEED_LOG_FILE = "/jfk_data/dallas_police_download_speed.json"

# Set umask to ensure files are created with the correct permissions
os.umask(0o022)

def update_download_speed(bytes_downloaded, elapsed_time):
    """Update the download speed in a shared file."""
    speed = (bytes_downloaded / 1024) / elapsed_time if elapsed_time > 0 else 0  # KB/s
    try:
        with open(SPEED_LOG_FILE, 'w') as f:
            json.dump({"download_speed": speed}, f)
        os.chmod(SPEED_LOG_FILE, 0o664)
        os.chown(SPEED_LOG_FILE, 1000, 1000)  # Assuming jfk user has UID/GID 1000
    except Exception as e:
        logging.error(f"Failed to update download speed: {str(e)}")

def clear_download_speed():
    """Clear the download speed file."""
    try:
        with open(SPEED_LOG_FILE, 'w') as f:
            json.dump({"download_speed": 0}, f)
        os.chmod(SPEED_LOG_FILE, 0o664)
        os.chown(SPEED_LOG_FILE, 1000, 1000)  # Assuming jfk user has UID/GID 1000
    except Exception as e:
        logging.error(f"Failed to clear download speed: {str(e)}")

def log_file_permissions(file_path):
    """Log the permissions and ownership of a file for debugging."""
    try:
        stat_info = os.stat(file_path)
        perms = oct(stat_info.st_mode & 0o777)[2:]
        uid = stat_info.st_uid
        gid = stat_info.st_gid
        logging.info(f"File {file_path} - Permissions: {perms}, UID: {uid}, GID: {gid}")
    except Exception as e:
        logging.error(f"Failed to log permissions for {file_path}: {str(e)}")

def scrape_dallas_police():
    """Scrape and download PDF files from the Dallas Police Archives on the Portal to Texas History."""
    logging.info("Starting Dallas Police Archives scraping...")
    
    # Ensure the save directory exists and has correct permissions
    os.makedirs(SAVE_DIR, exist_ok=True)
    os.chmod(SAVE_DIR, 0o775)
    os.chown(SAVE_DIR, 1000, 1000)  # Assuming jfk user has UID/GID 1000
    log_file_permissions(SAVE_DIR)
    
    # Clear the download speed at the start
    clear_download_speed()
    
    # Start with the first page of the collection
    page_url = BASE_URL
    page_num = 1
    downloaded_files = 0
    max_pages = 10  # Limit to 10 pages to avoid excessive downloads; adjust as needed
    
    while page_url and page_num <= max_pages:
        try:
            logging.info(f"Scraping page {page_num}: {page_url}")
            # Fetch the collection page
            chunk_size = 4096
            chunk_bytes = 0
            chunk_start_time = time.time()
            response = requests.get(page_url, stream=True)
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    chunk_bytes += len(chunk)
                    elapsed = time.time() - chunk_start_time
                    update_download_speed(chunk_bytes, elapsed)
                    chunk_bytes = 0
                    chunk_start_time = time.time()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all document links with 'metapth' identifiers
            doc_links = soup.find_all('a', href=re.compile(r'/ark:/67531/metapth\d+'))
            for link in doc_links:
                doc_url = urljoin(BASE_URL, link['href'])
                metapth_id = doc_url.split('/')[-2]  # Extract the metapth ID (e.g., metapth338772)
                pdf_filename = os.path.join(SAVE_DIR, f"output_{metapth_id}.pdf")
                
                # Skip if the file already exists
                if os.path.exists(pdf_filename) and os.path.getsize(pdf_filename) > 0:
                    logging.info(f"PDF already exists for {metapth_id} at {pdf_filename}, skipping")
                    # Simulate a small "download" by fetching the page to keep the gauge active
                    chunk_bytes = 0
                    chunk_start_time = time.time()
                    requests.head(doc_url)
                    elapsed = time.time() - chunk_start_time
                    update_download_speed(1024, elapsed)  # Simulate 1 KB download
                    time.sleep(1)  # Delay to keep gauge active
                    continue
                
                # Scrape the document page
                logging.info(f"Scraping URL: {doc_url}")
                try:
                    chunk_bytes = 0
                    chunk_start_time = time.time()
                    page_response = requests.get(doc_url)
                    page_response.raise_for_status()
                    page_soup = BeautifulSoup(page_response.text, 'html.parser')
                    
                    # Check for "View Extracted Text" link
                    text_link = page_soup.find('a', string=re.compile(r'View Extracted Text', re.I))
                    if text_link:
                        logging.info("No 'View Extracted Text' link found")
                    
                    # Look for IIIF manifest or page links
                    iiif_link = page_soup.find('a', href=re.compile(r'/ark:/67531/metapth\d+/iiif'))
                    if iiif_link:
                        iiif_manifest_url = urljoin(BASE_URL, iiif_link['href']) + '/manifest.json'
                        manifest_response = requests.get(iiif_manifest_url)
                        manifest_response.raise_for_status()
                        manifest = manifest_response.json()
                        pages = manifest.get('sequences', [{}])[0].get('canvases', [])
                        logging.info(f"Item has {len(pages)} pages according to IIIF manifest")
                        images = []
                        temp_dir = os.path.join(SAVE_DIR, f"temp_{metapth_id}")
                        os.makedirs(temp_dir, exist_ok=True)
                        os.chmod(temp_dir, 0o775)
                        os.chown(temp_dir, 1000, 1000)  # Assuming jfk user has UID/GID 1000
                        log_file_permissions(temp_dir)
                        try:
                            for i, canvas in enumerate(pages):
                                image_url = canvas['images'][0]['resource']['@id']
                                # Look for high_res_d link
                                high_res_url = image_url.replace('/full/full/0/default.jpg', '/full/high_res_d/')
                                logging.info(f"Found downloadable link (high_res_d): {high_res_url}")
                                image_response = requests.get(high_res_url, stream=True)
                                image_response.raise_for_status()
                                image_path = os.path.join(temp_dir, f"temp_image_{i}.jpg")
                                logging.info(f"Creating image file at {image_path}")
                                with open(image_path, 'wb') as img_f:
                                    os.chmod(image_path, 0o664)
                                    os.chown(image_path, 1000, 1000)  # Set ownership immediately
                                    log_file_permissions(image_path)
                                    for chunk in image_response.iter_content(chunk_size=chunk_size):
                                        if chunk:
                                            img_f.write(chunk)
                                            chunk_bytes += len(chunk)
                                            elapsed = time.time() - chunk_start_time
                                            update_download_speed(chunk_bytes, elapsed)
                                            chunk_bytes = 0
                                            chunk_start_time = time.time()
                                log_file_permissions(image_path)  # Log permissions after writing
                                images.append(image_path)
                                time.sleep(0.5)
                            logging.info("Using /high_res_d/ links, skipping other sources")
                            # Convert images to PDF
                            if images:
                                logging.info(f"Converting images to PDF: {images}")
                                cmd = ["img2pdf"] + images + ["-o", pdf_filename]
                                subprocess.run(cmd, check=True)
                                os.chmod(pdf_filename, 0o664)
                                os.chown(pdf_filename, 1000, 1000)  # Assuming jfk user has UID/GID 1000
                                log_file_permissions(pdf_filename)
                                file_size = os.path.getsize(pdf_filename) / (1024 ** 2)
                                logging.info(f"Created PDF {pdf_filename} from images ({file_size:.2f} MB)")
                                downloaded_files += 1
                        except Exception as e:
                            logging.error(f"Error converting images to PDF: {str(e)}")
                        finally:
                            time.sleep(1)
                            shutil.rmtree(temp_dir, ignore_errors=True)
                    else:
                        logging.warning(f"No IIIF manifest found for {doc_url}, skipping")
                        continue
                except Exception as e:
                    logging.error(f"Fallback download failed for {doc_url}: {str(e)}")
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
    
    # Clear the download speed at the end
    clear_download_speed()
    logging.info(f"Dallas Police Archives scraping complete. Downloaded {downloaded_files} files.")

if __name__ == "__main__":
    try:
        scrape_dallas_police()
    except Exception as e:
        logging.error(f"Scraping failed: {str(e)}")
        exit(1)

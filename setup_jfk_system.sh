#!/bin/bash

# Script to set up the JFK document management system environment,
# including dependencies, directory structure, database, Python environment,
# scripts, and initial GitHub setup.

# Exit on error
set -e

# Define paths
BASE_DIR="/jfk_data"
VENV_DIR="$BASE_DIR/venv"
JFK_MANAGER_PATH="$BASE_DIR/jfk_manager.py"
SCRAPER_PATH="$BASE_DIR/scrape_texas_history.py"
LOG_PATH="$BASE_DIR/indexing.log"
MANAGER_LOG_PATH="$BASE_DIR/jfk_manager.log"
STATUS_FILE="$BASE_DIR/indexing_status.json"
REPO_DIR="$BASE_DIR/jfk-document-system"
REPO_URL="git@github.com:dfultonthebar/jfk-document-system.git"
DALLAS_POLICE_DIR="$BASE_DIR/dallas_police"
TEMPLATES_DIR="$BASE_DIR/templates"

# Store the absolute path of this script
SCRIPT_PATH="$(realpath "$0")"

# Step 1: Install system dependencies
echo "Step 1: Installing system dependencies..."
apt-get update
apt-get install -y python3 python3-venv python3-pip mysql-server poppler-utils git img2pdf

# Step 2: Set up MySQL database and user
echo "Step 2: Setting up MySQL database and user..."
mysql -u root -e "CREATE DATABASE IF NOT EXISTS jfk_db;" || {
    echo "Failed to create database. If MySQL root requires a password, please modify this script to include it (e.g., mysql -u root -p<your-password>)."
    exit 1
}
mysql -u root -e "CREATE USER IF NOT EXISTS 'jfk_search'@'localhost' IDENTIFIED BY 'JFKUserSecure123!';"
mysql -u root -e "GRANT ALL PRIVILEGES ON jfk_db.* TO 'jfk_search'@'localhost';"
mysql -u root -e "FLUSH PRIVILEGES;"
mysql -u root jfk_db << 'EOF'
CREATE TABLE IF NOT EXISTS files (
    id VARCHAR(512) PRIMARY KEY,  -- Increased size to handle longer IDs
    filename TEXT NOT NULL,
    content TEXT,
    index_time DATETIME,
    date VARCHAR(255),
    time VARCHAR(255),
    location TEXT,
    mission_names TEXT
);
EOF
echo "MySQL database setup complete."

# Step 3: Create base directory and subdirectories
echo "Step 3: Creating base directory at $BASE_DIR..."
mkdir -p "$BASE_DIR"
mkdir -p "$DALLAS_POLICE_DIR"
mkdir -p "$BASE_DIR/national_archives"
# Set permissions for the base directory
chown -R jfk:jfk "$BASE_DIR"
chmod -R 775 "$BASE_DIR"

# Step 4: Set up virtual environment
echo "Step 4: Setting up virtual environment..."
if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"
chown -R jfk:jfk "$VENV_DIR"
chmod -R 775 "$VENV_DIR"

# Step 5: Install Python dependencies
echo "Step 5: Installing Python dependencies..."
pip install --upgrade pip
pip install flask beautifulsoup4 requests Pillow easyocr PyPDF2 mysql-connector-python psutil gputil

# Step 6: Create the jfk_manager.py script with new endpoints for total and new files
echo "Step 6: Creating jfk_manager.py with stabilized CPU-only OCR and continuous indexing..."
cat > "$JFK_MANAGER_PATH" << 'EOF'
#!/usr/bin/env python3

import os
import logging
import requests
import subprocess
import zipfile
import time
import psutil
import GPUtil
import shutil
import re
import json
import threading
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from flask import Flask, render_template, request, jsonify, send_from_directory
from PIL import Image
import easyocr
import mysql.connector
from datetime import datetime
import PyPDF2
import signal

# Configure logging with more detail for Flask
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/jfk_data/jfk_manager.log'),
        logging.StreamHandler()
    ]
)

app = Flask(__name__)
logging.info("Flask application initialized")

# Configuration
BASE_DIR = "/jfk_data"
NATIONAL_ARCHIVES_URL = "https://www.archives.gov/research/jfk/jfkbulkdownload"
MYSQL_CONFIG = {
    'user': 'jfk_search',
    'password': 'JFKUserSecure123!',
    'host': 'localhost',
    'database': 'jfk_db'
}
STATUS_FILE = os.path.join(BASE_DIR, "indexing_status.json")
DALLAS_POLICE_DIR = os.path.join(BASE_DIR, "dallas_police")
SPEED_LOG_FILE = "/jfk_data/dallas_police_download_speed.json"

# Initialize the download speed file
if not os.path.exists(SPEED_LOG_FILE):
    with open(SPEED_LOG_FILE, 'w') as f:
        json.dump({"download_speed": 0}, f)
    os.chmod(SPEED_LOG_FILE, 0o664)
    os.chown(SPEED_LOG_FILE, 1000, 1000)  # Assuming jfk user has UID/GID 1000

# Global dictionaries to track download and indexing status with thread safety
download_status_dict = {
    "in_progress": False,
    "bytes_downloaded": 0,
    "start_time": 0,
    "download_speed": 0  # KB/s
}
download_status_lock = threading.Lock()
download_in_progress = threading.Event()

indexing_status_dict = {
    "in_progress": False,
    "total_files": 0,
    "files_processed": 0,
    "progress": 0  # Percentage
}

# Ensure directories exist
os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "national_archives"), exist_ok=True)
os.makedirs(DALLAS_POLICE_DIR, exist_ok=True)

# Common locations and mission-related keywords for metadata extraction
COMMON_LOCATIONS = [
    "Dallas", "Washington", "New Orleans", "Miami", "Chicago", "Los Angeles",
    "Texas", "Louisiana", "Florida", "Illinois", "California", "Cuba", "Mexico",
    "Havana", "Arlington", "Philadelphia", "New York", "New Hampshire", "Bruxelles",
    "Caracas", "Reno", "Nevada", "Scarsdale"
]
MISSION_KEYWORDS = [
    "Operation", "Mission", "Project", "Plan", "Enigma", "Mongoose", "ZRRIFLE",
    "AMWORLD", "Cryptonym", "Program", "Civic Resistance", "KUDESK", "KUDARK"
]

# Initialize EasyOCR reader with CPU only
try:
    reader = easyocr.Reader(['en'], gpu=False)
    logging.info("EasyOCR initialized with CPU support")
except Exception as e:
    logging.error(f"Failed to initialize EasyOCR with CPU support: {str(e)}. Exiting...")
    raise

# Track initial file count for newly added files
initial_file_count = 0
def count_files():
    """Count the total number of PDF files in national_archives and dallas_police directories."""
    total = 0
    for subdir in ["national_archives", "dallas_police"]:
        subdir_path = os.path.join(BASE_DIR, subdir)
        if os.path.exists(subdir_path):
            total += len([f for f in os.listdir(subdir_path) if f.endswith('.pdf')])
    return total

# Set initial file count at startup
initial_file_count = count_files()

def save_indexing_status():
    """Save the indexing status to a file."""
    try:
        with open(STATUS_FILE, 'w') as f:
            json.dump(indexing_status_dict, f)
    except Exception as e:
        logging.error(f"Failed to save indexing status: {str(e)}")

def load_indexing_status():
    """Load the indexing status from a file."""
    try:
        with open(STATUS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load indexing status: {str(e)}")
        return {"in_progress": False, "total_files": 0, "files_processed": 0, "progress": 0}

def log_system_resources():
    """Log system resource usage for debugging."""
    try:
        # CPU usage
        cpu_usage = psutil.cpu_percent(interval=1)
        # Memory usage
        memory = psutil.virtual_memory()
        memory_total = memory.total / (1024 ** 3)  # Convert to GB
        memory_used = memory.used / (1024 ** 3)    # Convert to GB
        memory_free = memory.available / (1024 ** 3)  # Convert to GB
        logging.info(f"System Resources - CPU Usage: {cpu_usage}%, Memory Total: {memory_total:.2f}GB, Used: {memory_used:.2f}GB, Free: {memory_free:.2f}GB")
    except Exception as e:
        logging.error(f"Error logging system resources: {str(e)}")

def validate_image(image_path, max_size_mb=10):
    """Validate an image file to ensure it exists and isn't too large."""
    try:
        # Check if the file exists
        if not os.path.exists(image_path):
            return False, "Image file does not exist"
        # Check file size
        file_size_mb = os.path.getsize(image_path) / (1024 ** 2)  # Convert to MB
        if file_size_mb > max_size_mb:
            return False, f"Image file too large ({file_size_mb:.2f}MB, max {max_size_mb}MB)"
        # Check if the image can be opened and get dimensions
        with Image.open(image_path) as img:
            img.verify()  # Verify the image is valid
            img = Image.open(image_path)  # Re-open after verify
            width, height = img.size
            if width <= 0 or height <= 0:
                return False, "Image dimensions are invalid"
            logging.info(f"Image {image_path} - Size: {file_size_mb:.2f}MB, Dimensions: {width}x{height}")
            # Check resolution (downscale if too large)
            max_dimension = 2000
            if width > max_dimension or height > max_dimension:
                scale_factor = min(max_dimension / width, max_dimension / height)
                new_width = int(width * scale_factor)
                new_height = int(height * scale_factor)
                img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                img.save(image_path, "JPEG")
                logging.info(f"Downscaled image {image_path} to {new_width}x{new_height}")
        return True, ""
    except Exception as e:
        return False, f"Invalid image: {str(e)}"

def normalize_text(text):
    """Normalize OCR'd text by fixing common issues like extra spaces."""
    # Replace multiple spaces with a single space
    text = re.sub(r'\s+', ' ', text)
    # Fix common OCR errors (e.g., "Novem ber" -> "November")
    text = re.sub(r'Novem\s*ber', 'November', text, flags=re.IGNORECASE)
    text = re.sub(r'Septem\s*ber', 'September', text, flags=re.IGNORECASE)
    text = re.sub(r'Febru\s*ary', 'February', text, flags=re.IGNORECASE)
    return text.strip()

def extract_metadata(pdf_path, max_retries=2):
    """Extract metadata (date, extracted_time, location, mission names) from a PDF using PyPDF2."""
    # Initialize metadata variables to avoid undefined variable errors
    date = None
    extracted_time = None
    location = None
    mission_names = None

    for attempt in range(max_retries):
        try:
            # First try to extract text using PyPDF2
            with open(pdf_path, 'rb') as f:
                pdf_reader = PyPDF2.PdfReader(f)
                full_text = ""
                for page in pdf_reader.pages:
                    text = page.extract_text()
                    if text:
                        full_text += text + "\n"
            
            # If PyPDF2 fails to extract meaningful text, fall back to OCR
            if not full_text.strip():
                logging.warning(f"PyPDF2 extracted no text from {pdf_path}. Falling back to OCR (attempt {attempt+1}/{max_retries}).")
                # Log system resources before OCR
                log_system_resources()
                # Convert PDF to images and use OCR
                temp_dir = os.path.join(BASE_DIR, f"temp_metadata_{os.path.basename(pdf_path).replace('.pdf', '')}")
                os.makedirs(temp_dir, exist_ok=True)
                os.chmod(temp_dir, 0o775)
                os.chown(temp_dir, 1000, 1000)  # Assuming jfk user has UID/GID 1000
                try:
                    output_prefix = os.path.join(temp_dir, "page")
                    subprocess.run(
                        ["pdftoppm", "-jpeg", pdf_path, output_prefix],
                        check=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True
                    )
                    page_images = []
                    for f in os.listdir(temp_dir):
                        if f.endswith('.jpg'):
                            page_num = f.split('-')[1].split('.jpg')[0]
                            page_num = int(page_num.lstrip('0') or '0')
                            page_images.append((f, page_num))
                    page_images.sort(key=lambda x: x[1])
                    page_images = [f[0] for f in page_images]
                    
                    # Limit the number of pages to reduce memory usage (first 2 pages only)
                    page_images = page_images[:2]
                    
                    for page_image in page_images:
                        image_path = os.path.join(temp_dir, page_image)
                        os.chmod(image_path, 0o664)
                        os.chown(image_path, 1000, 1000)  # Assuming jfk user has UID/GID 1000
                        # Validate the image
                        is_valid, reason = validate_image(image_path)
                        if not is_valid:
                            logging.warning(f"Skipping image {image_path}: {reason}")
                            continue
                        try:
                            # Add timeout for OCR operation
                            def timeout_handler(signum, frame):
                                raise TimeoutError("OCR operation timed out")
                            signal.signal(signal.SIGALRM, timeout_handler)
                            signal.alarm(60)  # Set a 60-second timeout
                            result = reader.readtext(image_path, detail=0)
                            signal.alarm(0)  # Disable the alarm
                            text = " ".join(result).strip()
                            if text:
                                full_text += text + "\n"
                        except TimeoutError as e:
                            logging.error(f"OCR timed out for {image_path}: {str(e)}")
                            if attempt < max_retries - 1:
                                logging.info("Retrying OCR on next attempt...")
                                raise  # Retry by re-raising the exception
                            else:
                                logging.warning(f"Max retries reached for OCR on {image_path}. Skipping page.")
                                continue
                        except Exception as e:
                            logging.error(f"OCR failed for {image_path} (attempt {attempt+1}/{max_retries}): {str(e)}")
                            if attempt < max_retries - 1:
                                logging.info("Retrying OCR on next attempt...")
                                raise  # Retry by re-raising the exception
                            else:
                                logging.warning(f"Max retries reached for OCR on {image_path}. Skipping page.")
                                continue
                        finally:
                            time.sleep(2)  # Delay to prevent CPU overload
                except Exception as e:
                    logging.error(f"OCR fallback failed for {pdf_path} (attempt {attempt+1}/{max_retries}): {str(e)}")
                    if attempt < max_retries - 1:
                        logging.info("Retrying OCR on next attempt...")
                        raise  # Retry by re-raising the exception
                    else:
                        logging.warning(f"Max retries reached for OCR on {pdf_path}. Skipping file.")
                        return None
                finally:
                    # Clean up temporary directory after a delay
                    time.sleep(1)
                    shutil.rmtree(temp_dir, ignore_errors=True)
            
            # Normalize the text to handle OCR issues
            full_text = normalize_text(full_text)
            logging.debug(f"Extracted text for metadata from {pdf_path}: {full_text[:500]}...")  # Log first 500 chars
            
            # Extract metadata
            # Extract date with more flexible patterns
            date_patterns = [
                r'\b\d{1,2}/\d{1,2}/\d{2,4}\b',  # e.g., 11/22/1963
                r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b',  # e.g., November 22, 1963
                r'\b\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b',  # e.g., 9 November 1976
                r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s*\d{2}\b',  # e.g., June 8, 64
                r'\b\d{4}-\d{2}-\d{2}\b',  # e.g., 1964-10-23
                r'\b\d{2}-\d{2}-\d{4}\b'  # e.g., 06-30-1997
            ]
            for pattern in date_patterns:
                match = re.search(pattern, full_text, re.IGNORECASE)
                if match:
                    date = match.group(0)
                    break
            
            # Extract time (e.g., HH:MM AM/PM or HH:MM)
            time_pattern = r'\b\d{1,2}:\d{2}(?:\s*(?:AM|PM))?\b'
            match = re.search(time_pattern, full_text, re.IGNORECASE)
            if match:
                extracted_time = match.group(0)
            
            # Extract location (based on common locations list)
            locations_found = []
            for loc in COMMON_LOCATIONS:
                # Use a more flexible pattern to match locations
                pattern = r'\b' + re.escape(loc) + r'(?:,\s*[A-Za-z]+)?\b'
                if re.search(pattern, full_text, re.IGNORECASE):
                    locations_found.append(loc)
            location = ", ".join(locations_found) if locations_found else None
            
            # Extract mission names (based on keywords and following words)
            mission_names_list = []
            for keyword in MISSION_KEYWORDS:
                pattern = r'\b' + re.escape(keyword) + r'\s+([A-Za-z0-9-]+)\b'
                matches = re.finditer(pattern, full_text, re.IGNORECASE)
                for match in matches:
                    mission_name = f"{keyword} {match.group(1)}"
                    if mission_name not in mission_names_list:
                        mission_names_list.append(mission_name)
            mission_names = ", ".join(mission_names_list) if mission_names_list else None
            
            return {
                "date": date,
                "time": extracted_time,
                "location": location,
                "mission_names": mission_names
            }
        except Exception as e:
            if attempt < max_retries - 1:
                logging.info(f"Retrying metadata extraction for {pdf_path} (attempt {attempt+2}/{max_retries})...")
                time.sleep(5)  # Wait before retrying
                continue
            else:
                logging.error(f"Max retries reached for metadata extraction on {pdf_path}: {str(e)}")
                return None

def connect_to_db():
    """Connect to MySQL with reconnection logic."""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            return conn
        except mysql.connector.Error as e:
            logging.error(f"Failed to connect to MySQL (attempt {attempt+1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise Exception(f"Could not connect to MySQL after {max_retries} attempts: {str(e)}")

def index_files(limit=None):
    """Index PDF files using pdftoppm and EasyOCR, then store in MySQL with metadata."""
    global indexing_status_dict
    indexing_status_dict["in_progress"] = True
    indexing_status_dict["files_processed"] = 0
    save_indexing_status()

    logging.info("Starting indexing...")
    conn = None
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        
        # Recompute the list of PDF files to ensure accuracy
        pdf_files = []
        for subdir in ["national_archives", "dallas_police"]:
            subdir_path = os.path.join(BASE_DIR, subdir)
            for f in os.listdir(subdir_path):
                if f.endswith('.pdf'):
                    relative_path = os.path.join(subdir, f)
                    pdf_files.append(relative_path)
        if limit:
            pdf_files = pdf_files[:limit]
        
        indexing_status_dict["total_files"] = len(pdf_files)
        logging.info(f"Total files to index: {indexing_status_dict['total_files']}")
        save_indexing_status()
        
        if indexing_status_dict["total_files"] > 0:
            for pdf_file in pdf_files:
                # Use the full relative path as part of the item_id to ensure uniqueness
                item_id = pdf_file.replace('.pdf', '').replace('/', '_')
                pdf_path = os.path.join(BASE_DIR, pdf_file)
                
                # Check if the file is already indexed
                try:
                    cursor.execute("SELECT id FROM files WHERE id = %s", (item_id,))
                    result = cursor.fetchone()
                    if result:
                        logging.info(f"Skipping already indexed file: {pdf_file} (ID: {item_id})")
                        indexing_status_dict["files_processed"] += 1
                        indexing_status_dict["progress"] = (indexing_status_dict["files_processed"] / indexing_status_dict["total_files"]) * 100
                        save_indexing_status()
                        continue
                except mysql.connector.Error as e:
                    logging.error(f"MySQL error during index check for {pdf_file}: {str(e)}")
                    conn.close()
                    conn = connect_to_db()
                    cursor = conn.cursor()
                    continue
                
                # Check if OCR text already exists
                ocr_path = os.path.join(BASE_DIR, f"ocr_{item_id}.txt")
                if os.path.exists(ocr_path) and os.path.getsize(ocr_path) > 0:
                    logging.info(f"Skipping OCR for {pdf_file}, using existing OCR text: {ocr_path}")
                    with open(ocr_path, 'r') as f:
                        content = f.read()
                    # Re-extract metadata since it might not have been stored correctly
                    metadata = extract_metadata(pdf_path)
                    if metadata is None:
                        logging.warning(f"Failed to extract metadata for {pdf_file}. Skipping file.")
                        indexing_status_dict["files_processed"] += 1
                        indexing_status_dict["progress"] = (indexing_status_dict["files_processed"] / indexing_status_dict["total_files"]) * 100
                        save_indexing_status()
                        continue
                else:
                    logging.info(f"Indexing {pdf_file}...")
                    
                    # Extract metadata
                    metadata = extract_metadata(pdf_path)
                    if metadata is None:
                        logging.warning(f"Failed to extract metadata for {pdf_file}. Skipping file.")
                        indexing_status_dict["files_processed"] += 1
                        indexing_status_dict["progress"] = (indexing_status_dict["files_processed"] / indexing_status_dict["total_files"]) * 100
                        save_indexing_status()
                        continue
                    
                    # Create a temporary directory for page images
                    temp_dir = os.path.join(BASE_DIR, f"temp_{item_id}")
                    os.makedirs(temp_dir, exist_ok=True)
                    os.chmod(temp_dir, 0o775)
                    os.chown(temp_dir, 1000, 1000)  # Assuming jfk user has UID/GID 1000
                    page_images = []
                    try:
                        # Convert PDF to images using pdftoppm
                        output_prefix = os.path.join(temp_dir, "page")
                        try:
                            result = subprocess.run(
                                ["pdftoppm", "-jpeg", pdf_path, output_prefix],
                                check=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                text=True
                            )
                            logging.debug(f"pdftoppm stdout: {result.stdout}")
                            logging.debug(f"pdftoppm stderr: {result.stderr}")
                        except subprocess.CalledProcessError as e:
                            logging.error(f"pdftoppm failed for {pdf_file}: {e.stderr}")
                            indexing_status_dict["files_processed"] += 1
                            indexing_status_dict["progress"] = (indexing_status_dict["files_processed"] / indexing_status_dict["total_files"]) * 100
                            save_indexing_status()
                            continue
                        
                        # Find all generated images (pdftoppm names them page-001.jpg, page-002.jpg, etc.)
                        for f in os.listdir(temp_dir):
                            if f.endswith('.jpg'):
                                # Normalize the file name for consistent sorting
                                page_num = f.split('-')[1].split('.jpg')[0]
                                page_num = int(page_num.lstrip('0') or '0')  # Handle page-000.jpg
                                page_images.append((f, page_num))
                        page_images.sort(key=lambda x: x[1])  # Sort by page number
                        page_images = [f[0] for f in page_images]  # Keep only the filenames
                        
                        # Limit the number of pages to reduce memory usage (first 3 pages only)
                        page_images = page_images[:3]
                        
                        if not page_images:
                            logging.error(f"No images generated by pdftoppm for {pdf_file}")
                            indexing_status_dict["files_processed"] += 1
                            indexing_status_dict["progress"] = (indexing_status_dict["files_processed"] / indexing_status_dict["total_files"]) * 100
                            save_indexing_status()
                            continue
                        
                        # Perform OCR on each page using EasyOCR (CPU-only)
                        ocr_text = []
                        max_retries = 2
                        for i, page_image in enumerate(page_images):
                            image_path = os.path.join(temp_dir, page_image)
                            os.chmod(image_path, 0o664)
                            os.chown(image_path, 1000, 1000)  # Assuming jfk user has UID/GID 1000
                            # Validate the image
                            is_valid, reason = validate_image(image_path)
                            if not is_valid:
                                logging.warning(f"Skipping image {image_path}: {reason}")
                                continue
                            for attempt in range(max_retries):
                                try:
                                    # Log system resources before OCR
                                    log_system_resources()
                                    # Add timeout for OCR operation
                                    def timeout_handler(signum, frame):
                                        raise TimeoutError("OCR operation timed out")
                                    signal.signal(signal.SIGALRM, timeout_handler)
                                    signal.alarm(60)  # Set a 60-second timeout
                                    result = reader.readtext(image_path, detail=0)
                                    signal.alarm(0)  # Disable the alarm
                                    text = " ".join(result).strip()
                                    if text:
                                        ocr_text.append(f"Page {i+1}:\n{text}\n{'='*80}")
                                    break  # Success, exit retry loop
                                except TimeoutError as e:
                                    logging.error(f"OCR timed out for {image_path}: {str(e)}")
                                    if attempt < max_retries - 1:
                                        logging.info("Retrying OCR on next attempt...")
                                        time.sleep(5)
                                        continue
                                    else:
                                        logging.warning(f"Max retries reached for OCR on {image_path}. Skipping page.")
                                        break
                                except Exception as e:
                                    logging.error(f"Error performing OCR on {image_path} (attempt {attempt+1}/{max_retries}): {str(e)}")
                                    if attempt < max_retries - 1:
                                        logging.info("Retrying OCR on next attempt...")
                                        time.sleep(5)
                                        continue
                                    else:
                                        logging.warning(f"Max retries reached for OCR on {image_path}. Skipping page.")
                                        break
                                finally:
                                    time.sleep(2)  # Delay to prevent CPU overload
                        
                        # Save OCR text
                        with open(ocr_path, 'w') as f:
                            f.write('\n'.join(ocr_text))
                        os.chmod(ocr_path, 0o664)
                        os.chown(ocr_path, 1000, 1000)  # Assuming jfk user has UID/GID 1000
                        content = '\n'.join(ocr_text)
                    
                    except Exception as e:
                        logging.error(f"Error processing {pdf_file}: {str(e)}. Skipping file.")
                        indexing_status_dict["files_processed"] += 1
                        indexing_status_dict["progress"] = (indexing_status_dict["files_processed"] / indexing_status_dict["total_files"]) * 100
                        save_indexing_status()
                        continue
                    finally:
                        # Clean up temporary directory after a delay
                        time.sleep(1)
                        shutil.rmtree(temp_dir, ignore_errors=True)
                
                # Store in MySQL with metadata
                try:
                    cursor.execute(
                        """
                        INSERT INTO files (id, filename, content, index_time, date, time, location, mission_names)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            content=%s, index_time=%s, date=%s, time=%s, location=%s, mission_names=%s
                        """,
                        (
                            item_id, pdf_file, content, datetime.now(),
                            metadata["date"], metadata["time"], metadata["location"], metadata["mission_names"],
                            content, datetime.now(),
                            metadata["date"], metadata["time"], metadata["location"], metadata["mission_names"]
                        )
                    )
                    conn.commit()
                except mysql.connector.Error as e:
                    logging.error(f"MySQL error during insert for {pdf_file}: {str(e)}")
                    conn.close()
                    conn = connect_to_db()
                    cursor = conn.cursor()
                    continue
                
                indexing_status_dict["files_processed"] += 1
                indexing_status_dict["progress"] = (indexing_status_dict["files_processed"] / indexing_status_dict["total_files"]) * 100
                save_indexing_status()
                logging.info(f"Indexed {pdf_file} with metadata - Date: {metadata['date']}, Time: {metadata['time']}, Location: {metadata['location']}, Mission Names: {metadata['mission_names']}")
        
        else:
            logging.info("No files to index.")
    
    except Exception as e:
        logging.error(f"Error during indexing: {str(e)}")
        raise  # Re-raise the exception to allow the loop in main to handle it
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
        indexing_status_dict["in_progress"] = False
        save_indexing_status()

def download_national_archives():
    """Download files from the National Archives, including 2025 release."""
    global download_status_dict
    # Set in_progress immediately to ensure the gauge reflects activity
    download_in_progress.set()
    with download_status_lock:
        download_status_dict["start_time"] = time.time()
        download_status_dict["bytes_downloaded"] = 0
        download_status_dict["download_speed"] = 0

    logging.info("Starting National Archives download...")
    try:
        response = requests.get(NATIONAL_ARCHIVES_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for direct ZIP file links or other downloadable resources
        download_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.zip') or href.endswith('.pdf'):
                download_url = urljoin(NATIONAL_ARCHIVES_URL, href)
                download_links.append(download_url)
        
        # Download each file
        for download_url in download_links:
            filename = os.path.join(BASE_DIR, "national_archives", os.path.basename(download_url))
            if os.path.exists(filename) and os.path.getsize(filename) > 0:
                logging.info(f"Skipping existing file: {filename}")
                # Simulate a small "download" to keep the gauge active
                chunk_bytes = 0
                chunk_start_time = time.time()
                requests.head(download_url)
                elapsed = time.time() - chunk_start_time
                with download_status_lock:
                    download_status_dict["download_speed"] = (1024 / 1024) / elapsed if elapsed > 0 else 0  # Simulate 1 KB download
                    logging.info(f"Simulated download speed for skipped file: {download_status_dict['download_speed']:.2f} KB/s")
                time.sleep(1)  # Delay to keep gauge active
                continue
            
            logging.info(f"Downloading {download_url}...")
            chunk_size = 4096  # Smaller chunk size to ensure streaming
            chunk_bytes = 0
            chunk_start_time = time.time()
            response = requests.get(download_url, stream=True)
            response.raise_for_status()
            
            with open(filename + ".tmp", 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        chunk_bytes += len(chunk)
                        elapsed = time.time() - chunk_start_time
                        with download_status_lock:
                            download_status_dict["bytes_downloaded"] += len(chunk)
                            download_status_dict["download_speed"] = (chunk_bytes / 1024) / elapsed if elapsed > 0 else 0
                            logging.info(f"Download speed: {download_status_dict['download_speed']:.2f} KB/s")
                        chunk_bytes = 0
                        chunk_start_time = time.time()
            file_size = os.path.getsize(filename + ".tmp") / (1024 ** 2)  # Convert to MB
            os.rename(filename + ".tmp", filename)
            os.chmod(filename, 0o664)
            os.chown(filename, 1000, 1000)  # Assuming jfk user has UID/GID 1000
            logging.info(f"Downloaded {filename} ({file_size:.2f} MB)")
            
            # If it's a ZIP file, extract it
            if filename.endswith('.zip'):
                logging.info(f"Extracting {filename}...")
                with zipfile.ZipFile(filename, 'r') as zip_ref:
                    zip_ref.extractall(os.path.join(BASE_DIR, "national_archives"))
                for extracted_file in os.listdir(os.path.join(BASE_DIR, "national_archives")):
                    extracted_path = os.path.join(BASE_DIR, "national_archives", extracted_file)
                    os.chmod(extracted_path, 0o664)
                    os.chown(extracted_path, 1000, 1000)  # Assuming jfk user has UID/GID 1000
                logging.info(f"Extracted {filename}")
            time.sleep(1)  # Delay to keep gauge active
        
        # Request bulk download if no direct links are found
        if not download_links:
            logging.info("No direct download links found. Requesting bulk download from National Archives...")
            requests.post(
                "mailto:bulkdownload@nara.gov",
                data={"subject": "JFK Bulk Download"}
            )
            logging.info("Bulk download request sent. Check your email for the download link.")
            time.sleep(5)  # Delay to keep gauge active
    
    except Exception as e:
        logging.error(f"Error downloading from National Archives: {str(e)}")
    finally:
        # Keep the in_progress state for a minimum duration to ensure the gauge displays the activity
        time.sleep(5)
        with download_status_lock:
            download_status_dict["in_progress"] = False
            download_status_dict["download_speed"] = 0
        download_in_progress.clear()

def run_dallas_police_scraper():
    """Run the scrape_texas_history.py script for Dallas Police Archives as the jfk user."""
    global download_status_dict
    # Set in_progress immediately to ensure the gauge reflects activity
    download_in_progress.set()
    with download_status_lock:
        download_status_dict["start_time"] = time.time()
        download_status_dict["bytes_downloaded"] = 0

    logging.info("Starting Dallas Police Archives download...")
    try:
        # Verify img2pdf binary permissions
        img2pdf_path = shutil.which("img2pdf")
        if img2pdf_path:
            stat_info = os.stat(img2pdf_path)
            perms = oct(stat_info.st_mode & 0o777)[2:]
            uid = stat_info.st_uid
            gid = stat_info.st_gid
            logging.info(f"img2pdf binary at {img2pdf_path} - Permissions: {perms}, UID: {uid}, GID: {gid}")
        else:
            logging.error("img2pdf binary not found in PATH")
            raise Exception("img2pdf binary not found")

        # Change to the dallas_police directory to run the scraper
        os.chdir(DALLAS_POLICE_DIR)
        # Run the scraper as the jfk user
        process = subprocess.Popen(
            ["sudo", "-u", "jfk", "/jfk_data/scrape_texas_history.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Poll the speed log file while the subprocess is running
        while process.poll() is None:
            download_in_progress.set()
            try:
                with open(SPEED_LOG_FILE, 'r') as f:
                    speed_data = json.load(f)
                    with download_status_lock:
                        download_status_dict["download_speed"] = speed_data.get("download_speed", 0)
                        logging.info(f"Dallas Police download speed: {download_status_dict['download_speed']:.2f} KB/s")
            except (json.JSONDecodeError, FileNotFoundError) as e:
                logging.error(f"Failed to read download speed: {str(e)}")
                with download_status_lock:
                    download_status_dict["download_speed"] = 0
            time.sleep(0.5)  # Check every 0.5 seconds to keep gauge active
        
        # Check if the subprocess completed successfully
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            logging.error(f"Error running Dallas Police scraper: {stderr.decode()}")
            raise subprocess.CalledProcessError(process.returncode, process.args, stderr)
        logging.info("Dallas Police Archives download completed.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running Dallas Police scraper: {str(e)}")
    except Exception as e:
        logging.error(f"Error during Dallas Police download: {str(e)}")
    finally:
        # Keep the in_progress state for a minimum duration to ensure the gauge displays the activity
        time.sleep(5)
        with download_status_lock:
            download_status_dict["in_progress"] = False
            download_status_dict["download_speed"] = 0
        download_in_progress.clear()

@app.route('/')
def index():
    """Render the web interface."""
    logging.info("Handling request for /")
    try:
        return render_template('index.html')
    except Exception as e:
        logging.error(f"Error rendering web interface: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/search', methods=['GET'])
def search():
    """Search indexed files across content, location, and mission_names (case-insensitive)."""
    logging.info("Handling request for /search")
    query = request.args.get('q', '')
    if not query:
        logging.warning("Search query parameter 'q' is missing")
        return jsonify({"error": "Query parameter 'q' is required"}), 400
    
    try:
        conn = connect_to_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT id, filename, content, date, time, location, mission_names
            FROM files
            WHERE LOWER(content) LIKE %s
            OR LOWER(location) LIKE %s
            OR LOWER(mission_names) LIKE %s
            """,
            (f"%{query.lower()}%", f"%{query.lower()}%", f"%{query.lower()}%")
        )
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        logging.info(f"Search returned {len(results)} results for query: {query}")
        return jsonify(results)
    except Exception as e:
        logging.error(f"Error searching files: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/logs')
def logs():
    """Display recent logs."""
    logging.info("Handling request for /logs")
    try:
        with open('/jfk_data/jfk_manager.log', 'r') as f:
            lines = f.readlines()[-100:]  # Last 100 lines
        logging.info("Successfully retrieved logs")
        return jsonify({"logs": lines})
    except Exception as e:
        logging.error(f"Error reading logs: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/system_metrics')
def system_metrics():
    """Return CPU, GPU, and memory usage."""
    logging.info("Handling request for /system_metrics")
    try:
        # CPU usage
        cpu_usage = psutil.cpu_percent(interval=1)

        # Memory usage
        memory = psutil.virtual_memory()
        memory_usage = memory.percent

        # GPU usage (take multiple samples to improve accuracy)
        gpus = GPUtil.getGPUs()
        if gpus:
            gpu_samples = []
            for _ in range(3):  # Take 3 samples over 0.3 seconds
                gpu_usage = gpus[0].load * 100
                gpu_samples.append(gpu_usage)
                time.sleep(0.1)
            gpu_usage = sum(gpu_samples) / len(gpu_samples)  # Average the samples
        else:
            gpu_usage = 0

        logging.info(f"System metrics - CPU: {cpu_usage}%, Memory: {memory_usage}%, GPU: {gpu_usage}%")
        return jsonify({
            "cpu_usage": cpu_usage,
            "memory_usage": memory_usage,
            "gpu_usage": gpu_usage
        })
    except Exception as e:
        logging.error(f"Error fetching system metrics: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/download_status')
def download_status():
    """Return download status and speed."""
    logging.info("Handling request for /download_status")
    with download_status_lock:
        status = {
            "in_progress": download_in_progress.is_set(),
            "download_speed": download_status_dict["download_speed"]  # KB/s
        }
    logging.info(f"Download status - In progress: {status['in_progress']}, Speed: {status['download_speed']} KB/s")
    return jsonify(status)

@app.route('/indexing_status')
def indexing_status():
    """Return indexing progress from the status file."""
    logging.info("Handling request for /indexing_status")
    status = load_indexing_status()
    logging.info(f"Indexing status - In progress: {status['in_progress']}, Progress: {status['progress']}%")
    return jsonify(status)

@app.route('/total_files')
def total_files():
    """Return the total number of downloaded files."""
    logging.info("Handling request for /total_files")
    try:
        total = count_files()
        logging.info(f"Total files: {total}")
        return jsonify({"total_files": total})
    except Exception as e:
        logging.error(f"Error fetching total files: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/new_files')
def new_files():
    """Return the number of newly added files since startup."""
    logging.info("Handling request for /new_files")
    try:
        current_count = count_files()
        new_files = max(0, current_count - initial_file_count)
        logging.info(f"New files since startup: {new_files}")
        return jsonify({"new_files": new_files})
    except Exception as e:
        logging.error(f"Error fetching new files: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/files/<path:filepath>')
def serve_file(filepath):
    """Serve a file from the BASE_DIR."""
    logging.info(f"Handling request for file: {filepath}")
    try:
        # Construct the full path to the file
        full_path = os.path.join(BASE_DIR, filepath)
        # Ensure the file exists and is within BASE_DIR
        if not os.path.exists(full_path):
            logging.error(f"File not found: {full_path}")
            return jsonify({"error": "File not found"}), 404
        if not full_path.startswith(BASE_DIR):
            logging.error(f"Access denied: {full_path} is outside of BASE_DIR")
            return jsonify({"error": "Access denied"}), 403
        # Serve the file
        directory = os.path.dirname(full_path)
        filename = os.path.basename(full_path)
        logging.info(f"Serving file: {full_path}")
        return send_from_directory(directory, filename, as_attachment=True)
    except Exception as e:
        logging.error(f"Error serving file {filepath}: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="JFK Document Manager")
    parser.add_argument('action', choices=['download', 'index'], help="Action to perform")
    parser.add_argument('--limit', type=int, help="Limit the number of files to index")
    args = parser.parse_args()
    
    if args.action == 'download':
        download_national_archives()
        run_dallas_police_scraper()
    elif args.action == 'index':
        while True:
            try:
                # Log system resources before starting a new cycle
                log_system_resources()
                logging.info("Starting a new indexing cycle...")
                index_files(limit=args.limit)
                logging.info("Indexing cycle completed successfully. Restarting in 30 seconds...")
                time.sleep(30)  # Delay to allow system to stabilize
            except KeyboardInterrupt:
                logging.info("Indexing interrupted by user (Ctrl+C). Exiting...")
                break
            except Exception as e:
                logging.error(f"Indexing cycle failed with error: {str(e)}. Restarting in 30 seconds...")
                time.sleep(30)  # Delay to allow system to stabilize
            finally:
                # Ensure logging of loop iteration completion
                logging.info("Indexing loop iteration completed.")
EOF

# Set permissions for jfk_manager.py
chmod +x "$JFK_MANAGER_PATH"
chown jfk:jfk "$JFK_MANAGER_PATH"
chmod 775 "$JFK_MANAGER_PATH"

# Step 6.1: Create the index.html template with restored layout and new gauges
echo "Step 6.1: Creating index.html with gauges..."
mkdir -p "$TEMPLATES_DIR"
cat > "$TEMPLATES_DIR/index.html" << 'EOF'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>JFK Document Management System</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f4f4f4;
        }
        h1 {
            color: #333;
            text-align: center;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        .gauge-container {
            display: flex;
            justify-content: space-around;
            flex-wrap: wrap;
            margin-bottom: 20px;
        }
        .gauge {
            width: 200px;
            height: 160px;
            margin: 10px;
        }
        .search-box {
            text-align: center;
            margin: 20px 0;
        }
        .search-box input {
            padding: 10px;
            width: 300px;
            border: 1px solid #ccc;
            border-radius: 5px;
        }
        .search-box button {
            padding: 10px 20px;
            background-color: #007bff;
            color: #fff;
            border: none;
            border-radius: 5px;
            cursor: pointer;
        }
        .search-box button:hover {
            background-color: #0056b3;
        }
        #search-results {
            margin-top: 20px;
        }
        .log-box {
            margin-top: 20px;
            background-color: #fff;
            padding: 10px;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            max-height: 300px;
            overflow-y: auto;
        }
        #search-results ul {
            list-style-type: none;
            padding: 0;
        }
        #search-results li {
            background-color: #fff;
            padding: 10px;
            margin: 5px 0;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }
        #search-results a {
            color: #007bff;
            text-decoration: none;
        }
        #search-results a:hover {
            text-decoration: underline;
        }
    </style>
    <!-- Include JustGage and Raphael for the gauges -->
    <script src="https://cdnjs.cloudflare.com/ajax/libs/raphael/2.3.0/raphael.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/justgage/1.2.2/justgage.min.js"></script>
</head>
<body>
    <div class="container">
        <h1>JFK Document Management System</h1>
        
        <!-- Gauges in a horizontal row -->
        <div class="gauge-container">
            <div id="download-gauge" class="gauge"></div>
            <div id="cpu-gauge" class="gauge"></div>
            <div id="indexing-gauge" class="gauge"></div>
            <div id="memory-gauge" class="gauge"></div>
            <div id="gpu-gauge" class="gauge"></div>
            <div id="total-files-gauge" class="gauge"></div>
            <div id="new-files-gauge" class="gauge"></div>
        </div>
        
        <!-- Search Box -->
        <div class="search-box">
            <input type="text" id="search-query" placeholder="Search documents...">
            <button onclick="searchDocuments()">Search</button>
        </div>
        <div id="search-results"></div>
        
        <!-- Logs -->
        <h2>Recent Logs</h2>
        <div class="log-box" id="logs"></div>
    </div>

    <script>
        // Initialize the Download Speed gauge
        let downloadGauge = new JustGage({
            id: "download-gauge",
            value: 0,
            min: 0,
            max: 150000,  // 150,000 KB/s (150 MB/s) to accommodate 1 Gbps fiber
            title: "Download Speed",
            label: "KB/s",
            gaugeWidthScale: 0.6,
            levelColors: ["#ff0000", "#f9c802", "#00ff00"],
            decimals: 2,
            customSectors: {
                percents: true,
                ranges: [
                    { from: 0, to: 20, color: "#ff0000" },  // 0-30,000 KB/s
                    { from: 20, to: 60, color: "#f9c802" },  // 30,000-90,000 KB/s
                    { from: 60, to: 100, color: "#00ff00" }  // 90,000-150,000 KB/s
                ]
            },
            counter: true
        });

        // Initialize the CPU Usage gauge
        let cpuGauge = new JustGage({
            id: "cpu-gauge",
            value: 0,
            min: 0,
            max: 100,
            title: "CPU Usage",
            label: "%",
            gaugeWidthScale: 0.6,
            levelColors: ["#00ff00", "#f9c802", "#ff0000"],
            decimals: 2,
            customSectors: {
                percents: true,
                ranges: [
                    { from: 0, to: 33, color: "#00ff00" },
                    { from: 33, to: 66, color: "#f9c802" },
                    { from: 66, to: 100, color: "#ff0000" }
                ]
            },
            counter: true
        });

        // Initialize the Indexing Progress gauge
        let indexingGauge = new JustGage({
            id: "indexing-gauge",
            value: 0,
            min: 0,
            max: 100,
            title: "Indexer",
            label: "%",
            gaugeWidthScale: 0.6,
            levelColors: ["#ff0000", "#f9c802", "#00ff00"],
            decimals: 2,
            customSectors: {
                percents: true,
                ranges: [
                    { from: 0, to: 33, color: "#ff0000" },
                    { from: 33, to: 66, color: "#f9c802" },
                    { from: 66, to: 100, color: "#00ff00" }
                ]
            },
            counter: true
        });

        // Initialize the Memory Usage gauge
        let memoryGauge = new JustGage({
            id: "memory-gauge",
            value: 0,
            min: 0,
            max: 100,
            title: "Memory Usage",
            label: "%",
            gaugeWidthScale: 0.6,
            levelColors: ["#00ff00", "#f9c802", "#ff0000"],
            decimals: 2,
            customSectors: {
                percents: true,
                ranges: [
                    { from: 0, to: 33, color: "#00ff00" },
                    { from: 33, to: 66, color: "#f9c802" },
                    { from: 66, to: 100, color: "#ff0000" }
                ]
            },
            counter: true
        });

        // Initialize the GPU Usage gauge
        let gpuGauge = new JustGage({
            id: "gpu-gauge",
            value: 0,
            min: 0,
            max: 100,
            title: "GPU Usage",
            label: "%",
            gaugeWidthScale: 0.6,
            levelColors: ["#00ff00", "#f9c802", "#ff0000"],
            decimals: 2,
            customSectors: {
                percents: true,
                ranges: [
                    { from: 0, to: 33, color: "#00ff00" },
                    { from: 33, to: 66, color: "#f9c802" },
                    { from: 66, to: 100, color: "#ff0000" }
                ]
            },
            counter: true
        });

        // Initialize the Total Files Downloaded gauge
        let totalFilesGauge = new JustGage({
            id: "total-files-gauge",
            value: 0,
            min: 0,
            max: 1000,  // Adjust max based on expected file count
            title: "Total Files",
            label: "files",
            gaugeWidthScale: 0.6,
            levelColors: ["#ff0000", "#f9c802", "#00ff00"],
            decimals: 0,
            customSectors: {
                percents: true,
                ranges: [
                    { from: 0, to: 33, color: "#ff0000" },
                    { from: 33, to: 66, color: "#f9c802" },
                    { from: 66, to: 100, color: "#00ff00" }
                ]
            },
            counter: true
        });

        // Initialize the Newly Added Files gauge
        let newFilesGauge = new JustGage({
            id: "new-files-gauge",
            value: 0,
            min: 0,
            max: 100,  // Adjust max based on expected new files
            title: "New Files",
            label: "files",
            gaugeWidthScale: 0.6,
            levelColors: ["#ff0000", "#f9c802", "#00ff00"],
            decimals: 0,
            customSectors: {
                percents: true,
                ranges: [
                    { from: 0, to: 33, color: "#ff0000" },
                    { from: 33, to: 66, color: "#f9c802" },
                    { from: 66, to: 100, color: "#00ff00" }
                ]
            },
            counter: true
        });

        // Function to update the download status and gauge
        function updateDownloadStatus() {
            fetch('/download_status')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        console.error('Error fetching download status:', data.error);
                        return;
                    }
                    downloadGauge.refresh(data.download_speed);
                })
                .catch(error => console.error('Error fetching download status:', error));
        }

        // Function to update the indexing status and gauge
        function updateIndexingStatus() {
            fetch('/indexing_status')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        console.error('Error fetching indexing status:', data.error);
                        return;
                    }
                    indexingGauge.refresh(data.progress);
                })
                .catch(error => console.error('Error fetching indexing status:', error));
        }

        // Function to update system metrics and gauges
        function updateSystemMetrics() {
            fetch('/system_metrics')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        console.error('Error fetching system metrics:', data.error);
                        return;
                    }
                    cpuGauge.refresh(data.cpu_usage);
                    memoryGauge.refresh(data.memory_usage);
                    gpuGauge.refresh(data.gpu_usage);
                })
                .catch(error => console.error('Error fetching system metrics:', error));
        }

        // Function to update total files gauge
        function updateTotalFiles() {
            fetch('/total_files')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        console.error('Error fetching total files:', data.error);
                        return;
                    }
                    totalFilesGauge.refresh(data.total_files);
                })
                .catch(error => console.error('Error fetching total files:', error));
        }

        // Function to update newly added files gauge
        function updateNewFiles() {
            fetch('/new_files')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        console.error('Error fetching new files:', data.error);
                        return;
                    }
                    newFilesGauge.refresh(data.new_files);
                })
                .catch(error => console.error('Error fetching new files:', error));
        }

        // Function to update logs
        function updateLogs() {
            fetch('/logs')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        console.error('Error fetching logs:', data.error);
                        return;
                    }
                    document.getElementById('logs').innerHTML = data.logs.join('<br>');
                })
                .catch(error => console.error('Error fetching logs:', error));
        }

        // Function to search documents
        function searchDocuments() {
            const query = document.getElementById('search-query').value;
            if (!query) {
                alert("Please enter a search query.");
                return;
            }
            fetch(`/search?q=${encodeURIComponent(query)}`)
                .then(response => response.json())
                .then(data => {
                    const resultsDiv = document.getElementById('search-results');
                    if (data.error) {
                        resultsDiv.innerHTML = `<p>Error: ${data.error}</p>`;
                        return;
                    }
                    if (data.length === 0) {
                        resultsDiv.innerHTML = "<p>No results found.</p>";
                        return;
                    }
                    let html = "<ul>";
                    data.forEach(result => {
                        html += `<li><strong><a href="/files/${result.filename}" target="_blank">${result.filename}</a></strong><br>`;
                        html += `Date: ${result.date || 'N/A'}, Time: ${result.time || 'N/A'}<br>`;
                        html += `Location: ${result.location || 'N/A'}<br>`;
                        html += `Mission Names: ${result.mission_names || 'N/A'}<br>`;
                        html += `Content: ${result.content ? result.content.substring(0, 200) + '...' : 'N/A'}</li>`;
                    });
                    html += "</ul>";
                    resultsDiv.innerHTML = html;
                })
                .catch(error => console.error('Error searching documents:', error));
        }

        // Update gauges and logs at regular intervals
        setInterval(updateDownloadStatus, 1000);
        setInterval(updateIndexingStatus, 2000);
        setInterval(updateSystemMetrics, 2000);
        setInterval(updateTotalFiles, 2000);
        setInterval(updateNewFiles, 2000);
        setInterval(updateLogs, 5000);

        // Initial updates
        updateDownloadStatus();
        updateIndexingStatus();
        updateSystemMetrics();
        updateTotalFiles();
        updateNewFiles();
        updateLogs();
    </script>
</body>
</html>
EOF

# Set permissions for templates directory
chown -R jfk:jfk "$TEMPLATES_DIR"
chmod -R 775 "$TEMPLATES_DIR"

# Step 7: Create the scrape_texas_history.py script
echo "Step 7: Creating scrape_texas_history.py to download Dallas Police Archives..."
cat > "$SCRAPER_PATH" << 'EOF'
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
            # Read the response content into a variable to avoid consuming it twice
            content = b""
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    content += chunk
                    chunk_bytes += len(chunk)
                    elapsed = time.time() - chunk_start_time
                    update_download_speed(chunk_bytes, elapsed)
                    chunk_bytes = 0
                    chunk_start_time = time.time()
            soup = BeautifulSoup(content, 'html.parser')
            
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
EOF

# Set permissions for scrape_texas_history.py
chmod +x "$SCRAPER_PATH"
chown jfk:jfk "$SCRAPER_PATH"
chmod 775 "$SCRAPER_PATH"

# Step 8: Initialize the indexing status file
echo "Step 8: Initializing indexing status file..."
echo '{"files_processed":0,"in_progress":false,"progress":0,"total_files":0}' > "$STATUS_FILE"
chown jfk:jfk "$STATUS_FILE"
chmod 664 "$STATUS_FILE"

# Step 9: Truncate the log file if it exists
echo "Step 9: Truncating log file..."
> "$LOG_PATH"
chown jfk:jfk "$LOG_PATH"
chmod 664 "$LOG_PATH"

# Step 10: Clone or initialize the GitHub repository
echo "Step 10: Setting up GitHub repository for file uploads..."
if [ ! -d "$REPO_DIR" ]; then
    echo "Cloning repository from $REPO_URL..."
    git clone "$REPO_URL" "$REPO_DIR"
else
    echo "Repository already exists at $REPO_DIR. Pulling latest changes..."
    cd "$REPO_DIR"
    git pull origin main
    cd "$BASE_DIR"
fi

# Step 11: Upload initial files to GitHub
echo "Step 11: Uploading initial files to GitHub repository..."
cd "$REPO_DIR"
cp "$JFK_MANAGER_PATH" .
cp "$SCRAPER_PATH" .
cp "$SCRIPT_PATH" ./setup_jfk_system.sh  # Use the absolute path of this script
if [ -f "$LOG_PATH" ]; then
    cp "$LOG_PATH" .
else
    touch indexing.log
fi
if [ -f "$MANAGER_LOG_PATH" ]; then
    cp "$MANAGER_LOG_PATH" .
else
    touch jfk_manager.log
fi
git add .
git commit -m "Initial upload of JFK document system files before downloads and indexing" || {
    echo "No changes to commit, repository is up to date."
}
git push origin main
cd "$BASE_DIR"
echo "Initial files uploaded to GitHub repository at $REPO_URL."

# Step 12: Finalize setup and provide instructions
echo "Step 12: Setup complete."
echo "The JFK document management system environment has been set up."
echo "To proceed with downloading, indexing, and starting the services, run the following script:"
echo "  bash /jfk_data/run_jfk_system.sh"

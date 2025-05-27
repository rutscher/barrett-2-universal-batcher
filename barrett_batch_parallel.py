#!/usr/bin/env python
"""
Parallel Batch-runner for the Barrett Universal II online calculator.
Reads IOL_input.xlsx ➜ writes IOL_results.xlsx (new columns appended).
Performs two calculations per patient: biometry and topography.
Uses 8 parallel browser tabs to divide the workload.
"""

import time, sys, pathlib, logging, argparse, json
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import math

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Thread-%(thread)d] - %(message)s',
    handlers=[
        logging.FileHandler('batch_parallel.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

URL = "https://calc.apacrs.org/barrett_universal2105"

# ---------- Config ----------
EXCEL_IN   = "IOL_input_updated.xlsx"
EXCEL_OUT  = "IOL_results.xlsx"
NUM_WORKERS = 8  # Number of parallel browser tabs

# Run 1: Biometry (IOLm)
FIELD_MAP_RUN1 = {
    'MainContent_DoctorName'    : 'Doctor Name',
    'MainContent_PatientName'   : 'Patient Name',
    'MainContent_PatientNo'     : 'Patient ID',
    'MainContent_LensFactor'    : 'Lens Factor',
    'MainContent_Aconstant'     : 'A-Constant',
    'MainContent_IOLModel'      : 'IOL Model',
    'MainContent_Axlength'      : 'Axial length',
    'MainContent_MeasuredK1'    : 'Corneal power flat meridian K1 - Biometry (IOLm)',
    'MainContent_MeasuredK2'    : 'Corneal power steep meridian K2 - Biometry (IOLm)',
    'MainContent_OpticalACD'    : 'Anterior chamber depth',
    'MainContent_Refraction'    : 'Target Refraction',
    'MainContent_LensThickness' : 'central thickness of crystalline lens',
    'MainContent_WTW'           : 'Horizontal corneal diameter'
}

# Run 2: Topography
FIELD_MAP_RUN2 = {
    'MainContent_DoctorName'    : 'Doctor Name',
    'MainContent_PatientName'   : 'Patient Name',
    'MainContent_PatientNo'     : 'Patient ID',
    'MainContent_LensFactor'    : 'Lens Factor',
    'MainContent_Aconstant'     : 'A-Constant',
    'MainContent_IOLModel'      : 'IOL Model',
    'MainContent_Axlength'      : 'Axial length',
    'MainContent_MeasuredK1'    : 'Corneal power flat meridian K1 - topography',
    'MainContent_MeasuredK2'    : 'Corneal power steep meridian K2 - topography',
    'MainContent_OpticalACD'    : 'Anterior chamber depth',
    'MainContent_Refraction'    : 'Target Refraction',
    'MainContent_LensThickness' : 'central thickness of crystalline lens',
    'MainContent_WTW'           : 'Horizontal corneal diameter'
}

def parse_gridview1_results(driver):
    """
    Parse GridView1 (Right Eye OD) results table to extract IOL power and refraction pairs.
    Returns a dictionary mapping IOL power to refraction value.
    """
    soup = BeautifulSoup(driver.page_source, "lxml")
    table = soup.find(id="MainContent_GridView1")
    
    if not table:
        logger.warning("GridView1 table not found")
        return {}
    
    rows = table.find_all("tr")
    logger.debug(f"Found {len(rows)} rows in GridView1")
    
    power_refraction_map = {}
    
    # Skip header row (index 0)
    for row in rows[1:]:
        cells = row.find_all("td")
        if len(cells) >= 3:
            power_text = cells[0].get_text(strip=True)
            refraction_text = cells[2].get_text(strip=True)
            
            # Skip empty rows
            if power_text and refraction_text and power_text != "&nbsp;":
                try:
                    power = float(power_text)
                    refraction = float(refraction_text)
                    power_refraction_map[power] = refraction
                    logger.debug(f"Captured IOL power {power} -> refraction {refraction}")
                except ValueError:
                    logger.warning(f"Could not parse power '{power_text}' or refraction '{refraction_text}'")
    
    logger.debug(f"Extracted {len(power_refraction_map)} IOL power/refraction pairs")
    return power_refraction_map

def find_refraction_for_implanted_power(power_refraction_map, implanted_power):
    """
    Find the refraction value for the specific implanted lens power.
    Returns the refraction value or None if not found.
    """
    if not power_refraction_map:
        return None
    
    # Try exact match first
    if implanted_power in power_refraction_map:
        return power_refraction_map[implanted_power]
    
    # Try to find closest match (within 0.1 diopter tolerance)
    tolerance = 0.1
    for power, refraction in power_refraction_map.items():
        if abs(power - implanted_power) <= tolerance:
            logger.info(f"Found close match: implanted {implanted_power} matched to calculated {power}")
            return refraction
    
    return None

def perform_calculation(driver, wait, field_map, row, idx, run_name, worker_id):
    """
    Perform a single calculation (biometry or topography) and return the refraction for implanted power.
    """
    logger.info(f"Worker {worker_id} - Row {idx + 1}: Starting {run_name} calculation")
    
    try:
        # 1. Fill fields
        for html_id, col in field_map.items():
            if col not in row:       # safety check for optional columns
                continue
            value = row[col]
            if pd.isna(value):
                continue
            
            try:
                elem = driver.find_element(By.ID, html_id)
                
                # Drop-down vs text box
                if elem.tag_name == "select":
                    Select(elem).select_by_visible_text(str(value))
                else:
                    elem.clear()
                    elem.send_keys(str(value))
            except NoSuchElementException:
                logger.warning(f"Worker {worker_id} - Row {idx + 1}: Element {html_id} not found")
                continue

        # 2. Calculate
        calc_button = driver.find_element(By.ID, "MainContent_Button1")
        logger.info(f"Worker {worker_id} - Row {idx + 1}: Clicking Calculate button for {run_name}...")
        calc_button.click()
        
        # Give the page a moment to process
        time.sleep(1)
        
        # 2.5. Navigate to Universal formula tab to see the results tables
        try:
            logger.info(f"Worker {worker_id} - Row {idx + 1}: Looking for Universal formula tab...")
            # Try to find and click the Universal Formula tab
            universal_tab = driver.find_element(By.XPATH, "//a[contains(text(), 'Universal Formula')]")
            universal_tab.click()
            logger.info(f"Worker {worker_id} - Row {idx + 1}: Clicked Universal formula tab")
            time.sleep(1)  # Wait for tab content to load
        except NoSuchElementException:
            logger.warning(f"Worker {worker_id} - Row {idx + 1}: Universal Formula tab not found, proceeding anyway...")

        # 3. Wait for results and parse GridView1
        try:
            logger.info(f"Worker {worker_id} - Row {idx + 1}: Waiting for results table...")
            wait.until(EC.presence_of_element_located((By.ID, "MainContent_GridView1")))
            logger.info(f"Worker {worker_id} - Row {idx + 1}: Results table found, extracting {run_name} values...")
            
            # Additional wait to ensure table is fully populated
            time.sleep(0.5)
            
            # Parse GridView1 results
            power_refraction_map = parse_gridview1_results(driver)
            
            if not power_refraction_map:
                logger.warning(f"Worker {worker_id} - Row {idx + 1}: No results found in GridView1 for {run_name}")
                return None
            
            # Find refraction for implanted power
            implanted_power = row.get('Power of implanted lens')
            if pd.isna(implanted_power):
                logger.warning(f"Worker {worker_id} - Row {idx + 1}: No implanted lens power specified")
                return None
            
            refraction = find_refraction_for_implanted_power(power_refraction_map, float(implanted_power))
            
            if refraction is not None:
                logger.info(f"Worker {worker_id} - Row {idx + 1}: {run_name} - Found refraction {refraction} for implanted power {implanted_power}")
                return refraction
            else:
                logger.warning(f"Worker {worker_id} - Row {idx + 1}: {run_name} - Implanted power {implanted_power} not found in calculator results")
                return None
                
        except TimeoutException:
            logger.error(f"Worker {worker_id} - Row {idx + 1}: Timeout waiting for results in {run_name}")
            return None
        
        return refraction
        
    except Exception as e:
        logger.error(f"Worker {worker_id} - Row {idx + 1}: Error in {run_name} calculation - {e}")
        return None

def reset_form_and_return_to_patient_data(driver, idx, worker_id, context=""):
    """
    Navigate back to Patient Data tab and reset the form.
    """
    try:
        logger.info(f"Worker {worker_id} - Row {idx + 1}: Navigating back to Patient Data tab{context}...")
        patient_data_tab = driver.find_element(By.XPATH, "//a[contains(text(), 'Patient Data')]")
        patient_data_tab.click()
        logger.info(f"Worker {worker_id} - Row {idx + 1}: Clicked Patient Data tab")
        time.sleep(0.5)  # Wait for tab content to load
    except NoSuchElementException:
        logger.warning(f"Worker {worker_id} - Row {idx + 1}: Patient Data tab not found")
        
    # Reset form
    try:
        reset_button = driver.find_element(By.ID, "MainContent_btnReset")
        reset_button.click()
        logger.info(f"Worker {worker_id} - Row {idx + 1}: Reset form clicked{context}")
        time.sleep(0.5)  # Brief pause after reset
    except NoSuchElementException:
        logger.warning(f"Worker {worker_id} - Row {idx + 1}: Reset button not found")

# Required columns for validation
REQUIRED_COLUMNS = [
    'Patient Name',
    'Power of implanted lens',
    'Patient ID',
    'Axial length',
    'Corneal power flat meridian K1 - Biometry (IOLm)',
    'Corneal power steep meridian K2 - Biometry (IOLm)',
    'Corneal power flat meridian K1 - topography',
    'Corneal power steep meridian K2 - topography',
    'Anterior chamber depth'
]

def validate_row(row, idx):
    """Validate required fields are not NaN"""
    for col in REQUIRED_COLUMNS:
        if col in row and pd.isna(row[col]):
            logger.warning(f"Row {idx}: Missing required field '{col}' - skipping row")
            return False
    return True

def apply_defaults(row, custom_a_constant=None):
    """Apply default values as specified in section 5"""
    # A-Constant: use custom value if provided, otherwise default to 119.34 if blank
    if custom_a_constant is not None:
        row['A-Constant'] = custom_a_constant
    elif 'A-Constant' in row and pd.isna(row['A-Constant']):
        row['A-Constant'] = 119.34
    
    # Target Refraction defaults to 0 if blank
    if 'Target Refraction' in row and pd.isna(row['Target Refraction']):
        row['Target Refraction'] = 0
    
    # IOL Model defaults to "Personal Constant" if blank
    if 'IOL Model' in row and pd.isna(row['IOL Model']):
        row['IOL Model'] = "Personal Constant"
    
    return row

def process_chunk(chunk_data, worker_id, custom_a_constant=None):
    """
    Process a chunk of data in a separate browser instance.
    Returns a list of results for each row in the chunk.
    """
    logger.info(f"Worker {worker_id}: Starting with {len(chunk_data)} rows")
    
    # Create a separate browser instance for this worker
    driver = webdriver.Chrome()
    wait = WebDriverWait(driver, 10)
    
    results = []
    warnings_list = []
    
    try:
        driver.get(URL)
        logger.info(f"Worker {worker_id}: Opened Barrett calculator at {URL}")
        
        for chunk_idx, (original_idx, row) in enumerate(chunk_data):
            logger.info(f"Worker {worker_id}: Processing chunk row {chunk_idx + 1}/{len(chunk_data)} (original row {original_idx + 1})")
            
            # Validate required fields
            if not validate_row(row, original_idx + 1):
                warnings_list.append(f"Row {original_idx + 1}: Skipped due to missing required fields")
                results.append({
                    'original_idx': original_idx,
                    'biometry_refraction': None,
                    'topography_refraction': None,
                    'warnings': [f"Row {original_idx + 1}: Skipped due to missing required fields"]
                })
                continue
            
            # Apply defaults (including custom A-constant if provided)
            row = apply_defaults(row, custom_a_constant=custom_a_constant)
            
            row_warnings = []
            
            # Perform biometry calculation (Run 1)
            biometry_refraction = perform_calculation(driver, wait, FIELD_MAP_RUN1, row, original_idx, "Biometry", worker_id)
            if biometry_refraction is None:
                row_warnings.append(f"Row {original_idx + 1}: Could not find biometry refraction for implanted power {row.get('Power of implanted lens', 'N/A')}")
            
            # Reset form between biometry and topography calculations
            reset_form_and_return_to_patient_data(driver, original_idx, worker_id, " (between biometry and topography)")
            
            # Perform topography calculation (Run 2)
            topography_refraction = perform_calculation(driver, wait, FIELD_MAP_RUN2, row, original_idx, "Topography", worker_id)
            if topography_refraction is None:
                row_warnings.append(f"Row {original_idx + 1}: Could not find topography refraction for implanted power {row.get('Power of implanted lens', 'N/A')}")
            
            # Reset form after both calculations are complete (ready for next patient)
            reset_form_and_return_to_patient_data(driver, original_idx, worker_id, " (ready for next patient)")
            
            results.append({
                'original_idx': original_idx,
                'biometry_refraction': biometry_refraction,
                'topography_refraction': topography_refraction,
                'warnings': row_warnings
            })
            
            if biometry_refraction is not None or topography_refraction is not None:
                logger.info(f"Worker {worker_id} - Row {original_idx + 1}: Successfully processed")
    
    except Exception as e:
        logger.error(f"Worker {worker_id}: Fatal error - {e}")
        
    finally:
        driver.quit()
        logger.info(f"Worker {worker_id}: Browser closed")
    
    return results, warnings_list

def split_dataframe(df, num_chunks):
    """
    Split dataframe into roughly equal chunks for parallel processing.
    Returns list of chunks, where each chunk is a list of (index, row) tuples.
    """
    chunk_size = math.ceil(len(df) / num_chunks)
    chunks = []
    
    for i in range(0, len(df), chunk_size):
        chunk_data = []
        for idx in range(i, min(i + chunk_size, len(df))):
            chunk_data.append((idx, df.iloc[idx]))
        chunks.append(chunk_data)
    
    return chunks

# ---------- Main ----------
def main(test_mode=False, a_constant=None) -> None:
    mode_text = "TEST MODE - First row only" if test_mode else f"Parallel batch mode with {NUM_WORKERS} workers"
    a_constant_text = f" with A-Constant: {a_constant}" if a_constant else ""
    logger.info(f"Starting Barrett Universal II parallel batch calculator - {mode_text}{a_constant_text}")
    
    try:
        df = pd.read_excel(EXCEL_IN, sheet_name=0)
        logger.info(f"Loaded {len(df)} rows from {EXCEL_IN}")
        
        # In test mode, only process the first row
        if test_mode:
            df = df.head(1)
            logger.info("TEST MODE: Processing only the first row")
            
    except Exception as e:
        logger.error(f"Failed to load {EXCEL_IN}: {e}")
        return
    
    # Initialize new output columns
    df['Expected Post-Op Refraction Biometry'] = None
    df['Expected Post-Op Refraction Topography'] = None
    
    # Split data into chunks for parallel processing
    chunks = split_dataframe(df, NUM_WORKERS)
    logger.info(f"Split data into {len(chunks)} chunks for parallel processing")
    
    # Log chunk sizes
    for i, chunk in enumerate(chunks):
        logger.info(f"Chunk {i + 1}: {len(chunk)} rows")
    
    all_warnings = []
    processed_count = 0
    
    # Process chunks in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Submit all chunks for processing
        future_to_worker = {}
        for worker_id, chunk in enumerate(chunks):
            if chunk:  # Only submit non-empty chunks
                future = executor.submit(process_chunk, chunk, worker_id + 1, a_constant)
                future_to_worker[future] = worker_id + 1
        
        # Collect results as they complete
        for future in as_completed(future_to_worker):
            worker_id = future_to_worker[future]
            try:
                results, warnings = future.result()
                logger.info(f"Worker {worker_id}: Completed processing")
                
                # Update dataframe with results
                for result in results:
                    original_idx = result['original_idx']
                    if result['biometry_refraction'] is not None:
                        df.at[original_idx, 'Expected Post-Op Refraction Biometry'] = result['biometry_refraction']
                    if result['topography_refraction'] is not None:
                        df.at[original_idx, 'Expected Post-Op Refraction Topography'] = result['topography_refraction']
                    
                    if result['biometry_refraction'] is not None or result['topography_refraction'] is not None:
                        processed_count += 1
                    
                    all_warnings.extend(result['warnings'])
                
                all_warnings.extend(warnings)
                
            except Exception as e:
                logger.error(f"Worker {worker_id}: Exception occurred - {e}")
    
    # ---------- Save ----------
    df.to_excel(EXCEL_OUT, index=False)
    logger.info(f"✓ Finished – results written to {EXCEL_OUT}")
    logger.info(f"Successfully processed {processed_count}/{len(df)} rows")
    
    # Display warnings summary
    if all_warnings:
        logger.info("\n" + "="*50)
        logger.info("WARNINGS SUMMARY:")
        logger.info("="*50)
        for warning in all_warnings:
            logger.info(warning)
        logger.info("="*50)
    else:
        logger.info("No warnings - all calculations completed successfully!")

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Barrett Universal II parallel batch calculator')
    parser.add_argument('--test', action='store_true', 
                       help='Test mode: process only the first row')
    parser.add_argument('--a-constant', type=float, metavar='VALUE',
                       help='Custom A-Constant value to use for all calculations (overrides Excel values)')
    parser.add_argument('--workers', type=int, default=8, metavar='N',
                       help='Number of parallel workers (default: 8)')
    args = parser.parse_args()
    
    # Update number of workers if specified
    if args.workers:
        NUM_WORKERS = args.workers
        logger.info(f"Using {NUM_WORKERS} parallel workers")
    
    # Validate A-constant if provided
    if args.a_constant is not None:
        if args.a_constant < 100 or args.a_constant > 130:
            logger.warning(f"A-Constant value {args.a_constant} is outside typical range (100-130)")
            response = input("Continue anyway? (y/N): ")
            if response.lower() != 'y':
                logger.info("Exiting due to user choice")
                sys.exit(0)
    
    try:
        main(test_mode=args.test, a_constant=args.a_constant)
    except KeyboardInterrupt:
        logger.info("Interrupted by user; shutting down.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1) 
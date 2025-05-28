#!/usr/bin/env python
"""
Fixed Parallel API-based batch-runner for the Barrett Universal II online calculator.
Addresses 403 Forbidden errors with improved rate limiting and session management.
Reads IOL_input.xlsx ➜ writes IOL_results.xlsx (new columns appended).
Uses asyncio and aiohttp for concurrent processing with multiple workers.
Performs two calculations per patient: biometry and topography.
"""

import asyncio
import time
import sys
import pathlib
import logging
import argparse
import json
import re
import random
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlencode
from dataclasses import dataclass
import threading
from concurrent.futures import ThreadPoolExecutor

# Setup logging with thread-safe formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Worker-%(thread)d] - %(message)s',
    handlers=[
        logging.FileHandler('batch_api_parallel_fixed.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

URL = "https://calc.apacrs.org/barrett_universal2105/"

# ---------- Config ----------
EXCEL_IN = "IOL_input_updated.xlsx"
EXCEL_OUT = "IOL_results_api_parallel_fixed.xlsx"
DEFAULT_WORKERS = 4  # Reduced default to be more conservative
MAX_WORKERS = 25    # Increased max workers (was 4) - adjust based on server tolerance

# Timing configuration to avoid rate limiting
MIN_REQUEST_DELAY = .1  # Minimum delay between requests per worker
MAX_REQUEST_DELAY = .3  # Maximum delay between requests per worker
INTER_CALCULATION_DELAY = .1  # Delay between biometry and topography

# Run 1: Biometry (IOLm)
FIELD_MAP_RUN1 = {
    'ctl00$MainContent$DoctorName': 'Doctor Name',
    'ctl00$MainContent$PatientName': 'Patient Name',
    'ctl00$MainContent$PatientNo': 'MRN',
    'ctl00$MainContent$LensFactor': 'Lens Factor',
    'ctl00$MainContent$Aconstant': 'A-Constant',
    'ctl00$MainContent$IOLModel': 'IOL Model',
    'ctl00$MainContent$Axlength': 'Axial length',
    'ctl00$MainContent$MeasuredK1': 'Corneal power flat meridian K1 - Biometry (IOLm)',
    'ctl00$MainContent$MeasuredK2': 'Corneal power steep meridian K2 - Biometry (IOLm)',
    'ctl00$MainContent$OpticalACD': 'Anterior chamber depth',
    'ctl00$MainContent$Refraction': 'Target Refraction',
    'ctl00$MainContent$LensThickness': 'central thickness of crystalline lens',
    'ctl00$MainContent$WTW': 'Horizontal corneal diameter (WTW)'
}

# Run 2: Topography
FIELD_MAP_RUN2 = {
    'ctl00$MainContent$DoctorName': 'Doctor Name',
    'ctl00$MainContent$PatientName': 'Patient Name',
    'ctl00$MainContent$PatientNo': 'MRN',
    'ctl00$MainContent$LensFactor': 'Lens Factor',
    'ctl00$MainContent$Aconstant': 'A-Constant',
    'ctl00$MainContent$IOLModel': 'IOL Model',
    'ctl00$MainContent$Axlength': 'Axial length',
    'ctl00$MainContent$MeasuredK1': 'Corneal power flat meridian K1 - topography',
    'ctl00$MainContent$MeasuredK2': 'Corneal power steep meridian K2 - topography',
    'ctl00$MainContent$OpticalACD': 'Anterior chamber depth',
    'ctl00$MainContent$Refraction': 'Target Refraction',
    'ctl00$MainContent$LensThickness': 'central thickness of crystalline lens',
    'ctl00$MainContent$WTW': 'Horizontal corneal diameter (WTW)'
}

# User agents to rotate for better anti-detection
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36'
]

@dataclass
class PatientTask:
    """Represents a patient processing task"""
    idx: int
    row: pd.Series
    custom_a_constant: Optional[float] = None

@dataclass
class CalculationResult:
    """Represents the result of a calculation"""
    idx: int
    biometry_refraction: Optional[float] = None
    topography_refraction: Optional[float] = None
    warnings: List[str] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []

class AsyncBarrettAPIClient:
    """Async HTTP client for Barrett Universal II calculator API with anti-detection measures"""
    
    def __init__(self, session: aiohttp.ClientSession, worker_id: int):
        self.session = session
        self.worker_id = worker_id
        self.viewstate = None
        self.viewstate_generator = None
        self.event_validation = None
        self.current_form_data = {}
        self.user_agent = random.choice(USER_AGENTS)
        self.last_request_time = 0
        
    async def _wait_for_rate_limit(self):
        """Implement rate limiting to avoid 403 errors"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        # Random delay between MIN and MAX to avoid predictable patterns
        min_delay = MIN_REQUEST_DELAY + (self.worker_id * 0.2)  # Stagger workers
        delay_needed = random.uniform(min_delay, MAX_REQUEST_DELAY)
        
        if time_since_last < delay_needed:
            wait_time = delay_needed - time_since_last
            logger.debug(f"Worker {self.worker_id}: Rate limiting - waiting {wait_time:.2f}s")
            await asyncio.sleep(wait_time)
        
        self.last_request_time = time.time()
        
    def _get_headers(self, content_type: str = None) -> Dict[str, str]:
        """Get headers with anti-detection measures"""
        headers = {
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }
        
        if content_type:
            headers['Content-Type'] = content_type
            headers['Origin'] = 'https://calc.apacrs.org'
            headers['Referer'] = URL
        
        return headers
        
    async def get_initial_page(self):
        """Get the initial page and extract ViewState and EventValidation"""
        await self._wait_for_rate_limit()
        
        logger.debug(f"Worker {self.worker_id}: Getting initial page to extract ViewState...")
        
        headers = self._get_headers()
        
        try:
            async with self.session.get(URL, headers=headers) as response:
                if response.status == 403:
                    logger.error(f"Worker {self.worker_id}: 403 Forbidden on initial page - server may be blocking requests")
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=403,
                        message="Forbidden - server blocking requests"
                    )
                
                response.raise_for_status()
                html = await response.text()
                
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Failed to get initial page: {e}")
            raise
            
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract ViewState
        viewstate_input = soup.find('input', {'name': '__VIEWSTATE'})
        if viewstate_input:
            self.viewstate = viewstate_input.get('value')
            logger.debug(f"Worker {self.worker_id}: Extracted ViewState: {self.viewstate[:50]}...")
        
        # Extract ViewStateGenerator
        viewstate_gen_input = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})
        if viewstate_gen_input:
            self.viewstate_generator = viewstate_gen_input.get('value')
            logger.debug(f"Worker {self.worker_id}: Extracted ViewStateGenerator: {self.viewstate_generator}")
        
        # Extract EventValidation
        event_val_input = soup.find('input', {'name': '__EVENTVALIDATION'})
        if event_val_input:
            self.event_validation = event_val_input.get('value')
            logger.debug(f"Worker {self.worker_id}: Extracted EventValidation: {self.event_validation[:50]}...")
        
        if not all([self.viewstate, self.viewstate_generator, self.event_validation]):
            raise Exception(f"Worker {self.worker_id}: Failed to extract required ViewState parameters")
        
        logger.debug(f"Worker {self.worker_id}: Successfully extracted ViewState parameters")
        return html
    
    async def calculate(self, field_map: Dict[str, str], row: pd.Series):
        """Submit calculation request with anti-detection measures"""
        await self._wait_for_rate_limit()
        
        # Prepare form data with all required fields
        form_data = {
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            '__LASTFOCUS': '',
            '__VIEWSTATE': self.viewstate,
            '__VIEWSTATEGENERATOR': self.viewstate_generator,
            '__EVENTVALIDATION': self.event_validation,
            'ctl00$MainContent$RadioButtonList1': '337.5',  # K Index 1.3375
            'ctl00$MainContent$Button1': 'Calculate',
            # Initialize all form fields with empty values
            'ctl00$MainContent$DoctorName': '',
            'ctl00$MainContent$PatientName': '',
            'ctl00$MainContent$PatientNo': '',
            'ctl00$MainContent$LensFactor': '',
            'ctl00$MainContent$Aconstant': '',
            'ctl00$MainContent$IOLModel': 'Personal Constant',
            'ctl00$MainContent$Axlength': '',
            'ctl00$MainContent$Axlength0': '',
            'ctl00$MainContent$MeasuredK1': '',
            'ctl00$MainContent$MeasuredK10': '',
            'ctl00$MainContent$MeasuredK2': '',
            'ctl00$MainContent$MeasuredK20': '',
            'ctl00$MainContent$OpticalACD': '',
            'ctl00$MainContent$OpticalACD0': '',
            'ctl00$MainContent$Refraction': '0',
            'ctl00$MainContent$Refraction0': '0',
            'ctl00$MainContent$LensThickness': '',
            'ctl00$MainContent$LensThickness0': '',
            'ctl00$MainContent$WTW': '',
            'ctl00$MainContent$WTW0': ''
        }
        
        # Fill in patient data
        for form_field, excel_column in field_map.items():
            if excel_column in row and not pd.isna(row[excel_column]):
                value = str(row[excel_column])
                form_data[form_field] = value
                logger.debug(f"Worker {self.worker_id}: Setting {form_field} = {value}")
        
        # Store current form data for tab switching
        self.current_form_data = form_data.copy()
        
        # Make the calculation request
        logger.debug(f"Worker {self.worker_id}: Submitting calculation request...")
        
        headers = self._get_headers('application/x-www-form-urlencoded')
        
        try:
            async with self.session.post(URL, data=form_data, headers=headers) as response:
                if response.status == 403:
                    logger.error(f"Worker {self.worker_id}: 403 Forbidden on calculation request")
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=403,
                        message="Forbidden - server blocking calculation request"
                    )
                
                response.raise_for_status()
                html = await response.text()
                
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Failed to submit calculation: {e}")
            raise
            
        # Update ViewState from response
        self._update_viewstate_from_response(html)
        
        return html
    
    async def switch_to_universal_tab(self):
        """Switch to Universal Formula tab to get results"""
        await self._wait_for_rate_limit()
        
        # Use the current form data but change the event target
        form_data = self.current_form_data.copy()
        form_data.update({
            '__EVENTTARGET': 'ctl00$MainContent$menuTabs',
            '__EVENTARGUMENT': '1',
            '__VIEWSTATE': self.viewstate,
            '__VIEWSTATEGENERATOR': self.viewstate_generator,
            '__EVENTVALIDATION': self.event_validation,
        })
        
        # Remove the Calculate button since we're switching tabs
        if 'ctl00$MainContent$Button1' in form_data:
            del form_data['ctl00$MainContent$Button1']
        
        logger.debug(f"Worker {self.worker_id}: Switching to Universal Formula tab...")
        
        headers = self._get_headers('application/x-www-form-urlencoded')
        
        try:
            async with self.session.post(URL, data=form_data, headers=headers) as response:
                if response.status == 403:
                    logger.error(f"Worker {self.worker_id}: 403 Forbidden on tab switch")
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=403,
                        message="Forbidden - server blocking tab switch"
                    )
                
                response.raise_for_status()
                html = await response.text()
                
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Failed to switch to Universal tab: {e}")
            raise
            
        # Update ViewState from response
        self._update_viewstate_from_response(html)
        
        # Debug: Check if calculation was successful by looking for "Recommended IOL" in Universal Formula tab
        if "Recommended IOL" in html:
            logger.debug(f"Worker {self.worker_id}: Calculation appears successful - found 'Recommended IOL' in Universal Formula tab")
        else:
            logger.warning(f"Worker {self.worker_id}: Calculation may have failed - 'Recommended IOL' not found in Universal Formula tab")
        
        return html
    
    def _update_viewstate_from_response(self, html: str):
        """Update ViewState parameters from response HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        
        viewstate_input = soup.find('input', {'name': '__VIEWSTATE'})
        if viewstate_input:
            self.viewstate = viewstate_input.get('value')
        
        viewstate_gen_input = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})
        if viewstate_gen_input:
            self.viewstate_generator = viewstate_gen_input.get('value')
        
        event_val_input = soup.find('input', {'name': '__EVENTVALIDATION'})
        if event_val_input:
            self.event_validation = event_val_input.get('value')

def parse_gridview1_results(html: str, worker_id: int) -> Dict[float, float]:
    """
    Parse GridView1 (Right Eye OD) results table to extract IOL power and refraction pairs.
    Returns a dictionary mapping IOL power to refraction value.
    """
    soup = BeautifulSoup(html, "html.parser")
    
    table = soup.find(id="MainContent_GridView1")
    
    if not table:
        logger.warning(f"Worker {worker_id}: GridView1 table not found")
        # Try to find any tables for debugging
        all_tables = soup.find_all('table')
        logger.debug(f"Worker {worker_id}: Found {len(all_tables)} total tables in response")
        for i, table in enumerate(all_tables):
            table_id = table.get('id', 'no-id')
            logger.debug(f"Worker {worker_id}: Table {i}: id='{table_id}'")
        return {}
    
    rows = table.find_all("tr")
    logger.debug(f"Worker {worker_id}: Found {len(rows)} rows in GridView1")
    
    power_refraction_map = {}
    
    # Skip header row (index 0)
    for i, row in enumerate(rows[1:], 1):
        cells = row.find_all("td")
        if len(cells) >= 3:
            power_text = cells[0].get_text(strip=True)
            refraction_text = cells[2].get_text(strip=True)
            
            logger.debug(f"Worker {worker_id}: Row {i}: power_text='{power_text}', refraction_text='{refraction_text}'")
            
            # Skip empty rows
            if power_text and refraction_text and power_text != "&nbsp;":
                try:
                    power = float(power_text)
                    refraction = float(refraction_text)
                    power_refraction_map[power] = refraction
                    logger.debug(f"Worker {worker_id}: Captured IOL power {power} -> refraction {refraction}")
                except ValueError:
                    logger.warning(f"Worker {worker_id}: Could not parse power '{power_text}' or refraction '{refraction_text}'")
    
    logger.debug(f"Worker {worker_id}: Extracted {len(power_refraction_map)} IOL power/refraction pairs: {list(power_refraction_map.keys())}")
    return power_refraction_map

def find_refraction_for_implanted_power(power_refraction_map: Dict[float, float], implanted_power: float, worker_id: int) -> Optional[float]:
    """
    Find the refraction value for the specific implanted lens power.
    Returns the refraction value or None if not found.
    """
    if not power_refraction_map:
        logger.warning(f"Worker {worker_id}: Empty power_refraction_map - no results to search")
        return None
    
    logger.debug(f"Worker {worker_id}: Looking for implanted power {implanted_power} in available powers: {list(power_refraction_map.keys())}")
    
    # Try exact match first
    if implanted_power in power_refraction_map:
        result = power_refraction_map[implanted_power]
        logger.debug(f"Worker {worker_id}: Found exact match: {implanted_power} -> {result}")
        return result
    
    # Try to find closest match (within 0.1 diopter tolerance)
    tolerance = 0.1
    logger.debug(f"Worker {worker_id}: No exact match found, checking tolerance of ±{tolerance}")
    
    for power, refraction in power_refraction_map.items():
        diff = abs(power - implanted_power)
        logger.debug(f"Worker {worker_id}: Checking {power}: difference = {diff}")
        if diff <= tolerance:
            logger.info(f"Worker {worker_id}: Found close match: implanted {implanted_power} matched to calculated {power} (diff: {diff})")
            return refraction
    
    # If no match within tolerance, find the closest available power for informative logging
    min_diff = min(abs(power - implanted_power) for power in power_refraction_map.keys())
    closest_available = min(power_refraction_map.keys(), key=lambda x: abs(x - implanted_power))
    
    logger.warning(f"Worker {worker_id}: Implanted power {implanted_power} is outside recommended range. Closest available: {closest_available} (diff: {min_diff:.1f}D). Available range: {min(power_refraction_map.keys()):.1f}-{max(power_refraction_map.keys()):.1f}D")
    return None

async def perform_calculation(client: AsyncBarrettAPIClient, field_map: Dict[str, str], row: pd.Series, idx: int, run_name: str) -> Optional[float]:
    """
    Perform a single calculation (biometry or topography) and return the refraction for implanted power.
    """
    logger.info(f"Worker {client.worker_id}: Row {idx + 1}: Starting {run_name} calculation")
    
    try:
        # 1. Submit calculation
        calc_html = await client.calculate(field_map, row)
        logger.info(f"Worker {client.worker_id}: Row {idx + 1}: Calculation submitted for {run_name}")
        
        # 2. Switch to Universal Formula tab to get results
        results_html = await client.switch_to_universal_tab()
        logger.info(f"Worker {client.worker_id}: Row {idx + 1}: Switched to Universal Formula tab")
        
        # 3. Parse GridView1 results
        power_refraction_map = parse_gridview1_results(results_html, client.worker_id)
        
        if not power_refraction_map:
            logger.warning(f"Worker {client.worker_id}: Row {idx + 1}: No results found in GridView1 for {run_name}")
            return None
        
        # 4. Find refraction for implanted power
        implanted_power = row.get('Power of implanted lens')
        if pd.isna(implanted_power):
            logger.warning(f"Worker {client.worker_id}: Row {idx + 1}: No implanted lens power specified")
            return None
        
        refraction = find_refraction_for_implanted_power(power_refraction_map, float(implanted_power), client.worker_id)
        
        if refraction is not None:
            logger.info(f"Worker {client.worker_id}: Row {idx + 1}: {run_name} - Found refraction {refraction} for implanted power {implanted_power}")
            return refraction
        else:
            logger.warning(f"Worker {client.worker_id}: Row {idx + 1}: {run_name} - Implanted power {implanted_power} not found in calculator results")
            return None
            
    except Exception as e:
        logger.error(f"Worker {client.worker_id}: Row {idx + 1}: Error in {run_name} calculation - {e}")
        return None

# Required columns for validation
REQUIRED_COLUMNS = [
    'Patient Name',
    'Power of implanted lens',
    'MRN',
    'Axial length',
    'Corneal power flat meridian K1 - Biometry (IOLm)',
    'Corneal power steep meridian K2 - Biometry (IOLm)',
    'Corneal power flat meridian K1 - topography',
    'Corneal power steep meridian K2 - topography',
    'Anterior chamber depth'
]

def validate_row(row: pd.Series, idx: int) -> bool:
    """Validate required fields are not NaN"""
    for col in REQUIRED_COLUMNS:
        if col in row and pd.isna(row[col]):
            logger.warning(f"Row {idx}: Missing required field '{col}' - skipping row")
            return False
    return True

def apply_defaults(row: pd.Series, custom_a_constant: Optional[float] = None) -> pd.Series:
    """Apply default values as specified in section 5"""
    # Create a copy to avoid modifying the original
    row = row.copy()
    
    # A-Constant: use custom value if provided, otherwise default to 119.34 if blank
    if custom_a_constant is not None:
        row['A-Constant'] = custom_a_constant
    elif 'A-Constant' not in row or pd.isna(row.get('A-Constant')):
        row['A-Constant'] = 119.34
    
    # Target Refraction defaults to 0 if blank
    if 'Target Refraction' not in row or pd.isna(row.get('Target Refraction')):
        row['Target Refraction'] = 0
    
    # IOL Model defaults to "Personal Constant" if blank
    if 'IOL Model' not in row or pd.isna(row.get('IOL Model')):
        row['IOL Model'] = "Personal Constant"
    
    return row

async def process_patient_worker(session: aiohttp.ClientSession, task_queue: asyncio.Queue, results_queue: asyncio.Queue, worker_id: int):
    """
    Worker coroutine that processes patients from the task queue with improved error handling
    """
    logger.info(f"Worker {worker_id}: Starting")
    
    # Create client for this worker
    client = AsyncBarrettAPIClient(session, worker_id)
    
    try:
        while True:
            try:
                # Get next task from queue (with timeout to allow graceful shutdown)
                task = await asyncio.wait_for(task_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                # Check if queue is empty and we should exit
                if task_queue.empty():
                    break
                continue
            
            if task is None:  # Sentinel value to stop worker
                break
                
            logger.info(f"Worker {worker_id}: Processing patient {task.idx + 1}")
            
            # Validate required fields
            if not validate_row(task.row, task.idx + 1):
                result = CalculationResult(
                    idx=task.idx,
                    warnings=[f"Row {task.idx + 1}: Skipped due to missing required fields"]
                )
                await results_queue.put(result)
                task_queue.task_done()
                continue
            
            # Apply defaults (including custom A-constant if provided)
            processed_row = apply_defaults(task.row, custom_a_constant=task.custom_a_constant)
            
            result = CalculationResult(idx=task.idx)
            
            # Initialize fresh client session for this patient
            try:
                await client.get_initial_page()
            except Exception as e:
                logger.error(f"Worker {worker_id}: Row {task.idx + 1}: Failed to initialize session - {e}")
                result.warnings.append(f"Row {task.idx + 1}: Failed to initialize session - {e}")
                await results_queue.put(result)
                task_queue.task_done()
                continue
            
            # Perform biometry calculation (Run 1)
            try:
                biometry_refraction = await perform_calculation(client, FIELD_MAP_RUN1, processed_row, task.idx, "Biometry")
                result.biometry_refraction = biometry_refraction
                if biometry_refraction is None:
                    result.warnings.append(f"Row {task.idx + 1}: Could not find biometry refraction for implanted power {processed_row.get('Power of implanted lens', 'N/A')}")
            except Exception as e:
                logger.error(f"Worker {worker_id}: Row {task.idx + 1}: Biometry calculation failed - {e}")
                result.warnings.append(f"Row {task.idx + 1}: Biometry calculation failed - {e}")
            
            # Delay between calculations
            await asyncio.sleep(INTER_CALCULATION_DELAY)
            
            # Reset session between calculations
            try:
                await client.get_initial_page()
            except Exception as e:
                logger.error(f"Worker {worker_id}: Row {task.idx + 1}: Failed to reset session - {e}")
                result.warnings.append(f"Row {task.idx + 1}: Failed to reset session for topography - {e}")
                await results_queue.put(result)
                task_queue.task_done()
                continue
            
            # Perform topography calculation (Run 2)
            try:
                topography_refraction = await perform_calculation(client, FIELD_MAP_RUN2, processed_row, task.idx, "Topography")
                result.topography_refraction = topography_refraction
                if topography_refraction is None:
                    result.warnings.append(f"Row {task.idx + 1}: Could not find topography refraction for implanted power {processed_row.get('Power of implanted lens', 'N/A')}")
            except Exception as e:
                logger.error(f"Worker {worker_id}: Row {task.idx + 1}: Topography calculation failed - {e}")
                result.warnings.append(f"Row {task.idx + 1}: Topography calculation failed - {e}")
            
            # Put result in results queue
            await results_queue.put(result)
            
            # Mark task as done
            task_queue.task_done()
            
            logger.info(f"Worker {worker_id}: Completed patient {task.idx + 1}")
            
            # Additional delay to be respectful to the server
            await asyncio.sleep(random.uniform(0.5, 1.0))
            
    except Exception as e:
        logger.error(f"Worker {worker_id}: Fatal error - {e}")
    finally:
        logger.info(f"Worker {worker_id}: Shutting down")

async def main_async(test_mode: bool = False, a_constant: Optional[float] = None, num_workers: int = DEFAULT_WORKERS) -> None:
    """
    Main async function that orchestrates the parallel processing with improved error handling
    """
    mode_text = "TEST MODE - First row only" if test_mode else "Full batch mode"
    a_constant_text = f" with A-Constant: {a_constant}" if a_constant else ""
    logger.info(f"Starting Barrett Universal II API parallel batch calculator (FIXED) - {mode_text}{a_constant_text}")
    logger.info(f"Using {num_workers} concurrent workers with enhanced rate limiting")
    
    # Track warnings for summary
    all_warnings = []
    
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
    
    # Create task and results queues
    task_queue = asyncio.Queue()
    results_queue = asyncio.Queue()
    
    # Populate task queue
    for idx, row in df.iterrows():
        task = PatientTask(idx=idx, row=row, custom_a_constant=a_constant)
        await task_queue.put(task)
    
    # Create aiohttp session with more conservative settings
    connector = aiohttp.TCPConnector(
        limit=num_workers,  # Reduced connection pool size
        limit_per_host=num_workers,  # Connections per host
        ttl_dns_cache=300,
        use_dns_cache=True,
        enable_cleanup_closed=True
    )
    
    timeout = aiohttp.ClientTimeout(total=120, connect=60)  # Increased timeouts
    
    async with aiohttp.ClientSession(
        connector=connector, 
        timeout=timeout,
        cookie_jar=aiohttp.CookieJar()  # Enable cookie handling
    ) as session:
        # Start worker tasks
        workers = []
        for worker_id in range(num_workers):
            worker = asyncio.create_task(
                process_patient_worker(session, task_queue, results_queue, worker_id)
            )
            workers.append(worker)
        
        # Wait for all tasks to be processed
        await task_queue.join()
        
        # Stop workers by sending sentinel values
        for _ in range(num_workers):
            await task_queue.put(None)
        
        # Wait for workers to finish
        await asyncio.gather(*workers, return_exceptions=True)
    
    # Collect results
    processed_count = 0
    while not results_queue.empty():
        result = await results_queue.get()
        
        # Update dataframe with results
        if result.biometry_refraction is not None:
            df.at[result.idx, 'Expected Post-Op Refraction Biometry'] = result.biometry_refraction
        
        if result.topography_refraction is not None:
            df.at[result.idx, 'Expected Post-Op Refraction Topography'] = result.topography_refraction
        
        # Collect warnings
        all_warnings.extend(result.warnings)
        
        # Count successful processing
        if result.biometry_refraction is not None or result.topography_refraction is not None:
            processed_count += 1
    
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

def main(test_mode: bool = False, a_constant: Optional[float] = None, num_workers: int = DEFAULT_WORKERS) -> None:
    """
    Main function that runs the async event loop
    """
    try:
        asyncio.run(main_async(test_mode, a_constant, num_workers))
    except KeyboardInterrupt:
        logger.info("Interrupted by user; shutting down.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Barrett Universal II API parallel batch calculator (FIXED)')
    parser.add_argument('--test', action='store_true', 
                       help='Test mode: process only the first row')
    parser.add_argument('--a-constant', type=float, metavar='VALUE',
                       help='Custom A-Constant value to use for all calculations (overrides Excel values)')
    parser.add_argument('--workers', type=int, metavar='N', default=DEFAULT_WORKERS,
                       help=f'Number of concurrent workers (default: {DEFAULT_WORKERS}, max: {MAX_WORKERS})')
    args = parser.parse_args()
    
    # Validate arguments
    if args.workers < 1:
        logger.error("Number of workers must be at least 1")
        sys.exit(1)
    
    if args.workers > MAX_WORKERS:
        logger.warning(f"Number of workers ({args.workers}) exceeds maximum ({MAX_WORKERS}). Using {MAX_WORKERS} workers.")
        args.workers = MAX_WORKERS
    
    # Validate A-constant if provided
    if args.a_constant is not None:
        if args.a_constant < 100 or args.a_constant > 130:
            logger.warning(f"A-Constant value {args.a_constant} is outside typical range (100-130)")
            response = input("Continue anyway? (y/N): ")
            if response.lower() != 'y':
                logger.info("Exiting due to user choice")
                sys.exit(0)
    
    try:
        main(test_mode=args.test, a_constant=args.a_constant, num_workers=args.workers)
    except KeyboardInterrupt:
        logger.info("Interrupted by user; shutting down.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1) 
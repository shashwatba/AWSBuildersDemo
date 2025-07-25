#!/usr/bin/env python3
"""
ISCC Certificate PDF Scraper
Scrapes audit report PDFs from ISCC certificate database and uploads to AWS S3
Supports both Brightdata and Tavily APIs
"""

import os
import re
import time
import json
import hashlib
import requests
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime
from typing import List, Dict, Optional, Set
import logging

import boto3
from botocore.exceptions import ClientError
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('iscc_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ISCCPDFScraper:
    def __init__(self, config: Dict):
        """
        Initialize the ISCC PDF scraper
        
        Args:
            config: Configuration dictionary containing API keys and settings
        """
        self.config = config
        self.base_url = "https://www.iscc-system.org"
        self.certificates_url = f"{self.base_url}/certification/certificate-database/valid-certificates/"
        
        # Initialize AWS S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=config.get('aws_access_key_id'),
            aws_secret_access_key=config.get('aws_secret_access_key'),
            region_name=config.get('aws_region', 'us-east-1')
        )
        self.s3_bucket = config['s3_bucket_name']
        
        # Initialize scraping service
        self.scraping_service = config.get('scraping_service', 'selenium').lower()
        
        # Track processed PDFs to avoid duplicates
        self.processed_pdfs: Set[str] = set()
        self.session = requests.Session()
        
        # Setup headers to mimic a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

    def setup_selenium_driver(self) -> webdriver.Chrome:
        """Setup Chrome WebDriver with appropriate options"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        
        return webdriver.Chrome(options=chrome_options)

    def scrape_with_brightdata(self, url: str) -> Optional[str]:
        """
        Scrape content using Brightdata API
        
        Args:
            url: URL to scrape
            
        Returns:
            HTML content or None if failed
        """
        if not self.config.get('brightdata_api_key'):
            logger.error("Brightdata API key not provided")
            return None
            
        endpoint = "https://api.brightdata.com/request"
        
        payload = {
            'url': url,
            'format': 'html',
            'render_js': True,
            'wait_for': 3000  # Wait 3 seconds for JS to load
        }
        
        headers = {
            'Authorization': f'Bearer {self.config["brightdata_api_key"]}',
            'Content-Type': 'application/json'
        }
        
        try:
            response = requests.post(endpoint, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            return result.get('content')
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Brightdata scraping failed for {url}: {e}")
            return None

    def scrape_with_tavily(self, url: str) -> Optional[str]:
        """
        Scrape content using Tavily API
        
        Args:
            url: URL to scrape
            
        Returns:
            HTML content or None if failed
        """
        if not self.config.get('tavily_api_key'):
            logger.error("Tavily API key not provided")
            return None
            
        endpoint = "https://api.tavily.com/search"
        
        payload = {
            'api_key': self.config['tavily_api_key'],
            'query': f'site:{url}',
            'include_raw_content': True,
            'max_results': 1
        }
        
        try:
            response = requests.post(endpoint, json=payload, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            if result.get('results'):
                return result['results'][0].get('raw_content')
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Tavily scraping failed for {url}: {e}")
            return None

    def scrape_with_selenium(self, url: str) -> Optional[str]:
        """
        Scrape content using Selenium WebDriver
        
        Args:
            url: URL to scrape
            
        Returns:
            HTML content or None if failed
        """
        driver = None
        try:
            driver = self.setup_selenium_driver()
            driver.get(url)
            
            # Wait for the certificate table to load
            wait = WebDriverWait(driver, 20)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            
            # Wait a bit more for dynamic content
            time.sleep(3)
            
            return driver.page_source
            
        except TimeoutException:
            logger.error(f"Timeout waiting for page to load: {url}")
            return None
        except Exception as e:
            logger.error(f"Selenium scraping failed for {url}: {e}")
            return None
        finally:
            if driver:
                driver.quit()

    def get_page_content(self, url: str) -> Optional[str]:
        """
        Get page content using the configured scraping service
        
        Args:
            url: URL to scrape
            
        Returns:
            HTML content or None if failed
        """
        logger.info(f"Scraping {url} using {self.scraping_service}")
        
        if self.scraping_service == 'brightdata':
            return self.scrape_with_brightdata(url)
        elif self.scraping_service == 'tavily':
            return self.scrape_with_tavily(url)
        else:  # Default to selenium
            return self.scrape_with_selenium(url)

    def extract_certificate_data(self, html_content: str) -> List[Dict]:
        """
        Extract certificate data and PDF links from HTML content
        
        Args:
            html_content: HTML content of the certificates page
            
        Returns:
            List of certificate data dictionaries
        """
        from bs4 import BeautifulSoup
        
        soup = BeautifulSoup(html_content, 'html.parser')
        certificates = []
        
        # Find the certificate table
        table = soup.find('table')
        if not table:
            logger.warning("No certificate table found")
            return certificates
        
        # Extract table rows (skip header)
        rows = table.find_all('tr')[1:]
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 5:  # Ensure we have enough columns
                continue
                
            # Extract certificate information
            cert_data = {
                'certificate_number': cells[0].get_text(strip=True) if cells[0] else '',
                'company_name': cells[1].get_text(strip=True) if cells[1] else '',
                'country': cells[2].get_text(strip=True) if cells[2] else '',
                'validity_period': cells[3].get_text(strip=True) if cells[3] else '',
                'certification_body': cells[4].get_text(strip=True) if cells[4] else '',
                'pdf_links': []
            }
            
            # Look for PDF links in the row
            for cell in cells:
                links = cell.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if href.lower().endswith('.pdf'):
                        # Convert relative URLs to absolute
                        if href.startswith('/'):
                            href = urljoin(self.base_url, href)
                        elif not href.startswith('http'):
                            href = urljoin(self.base_url, href)
                        
                        cert_data['pdf_links'].append({
                            'url': href,
                            'text': link.get_text(strip=True),
                            'type': self.classify_pdf_type(link.get_text(strip=True))
                        })
            
            if cert_data['pdf_links']:  # Only include certificates with PDF links
                certificates.append(cert_data)
        
        logger.info(f"Found {len(certificates)} certificates with PDF links")
        return certificates

    def classify_pdf_type(self, link_text: str) -> str:
        """
        Classify PDF type based on link text
        
        Args:
            link_text: Text of the PDF link
            
        Returns:
            PDF type classification
        """
        link_text = link_text.lower()
        
        if 'audit' in link_text or 'summary' in link_text:
            return 'audit_report'
        elif 'certificate' in link_text:
            return 'certificate'
        else:
            return 'unknown'

    def download_pdf(self, pdf_url: str) -> Optional[bytes]:
        """
        Download PDF content
        
        Args:
            pdf_url: URL of the PDF to download
            
        Returns:
            PDF content as bytes or None if failed
        """
        try:
            response = self.session.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            # Verify content is actually a PDF
            if not response.content.startswith(b'%PDF'):
                logger.warning(f"Downloaded content is not a valid PDF: {pdf_url}")
                return None
                
            return response.content
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download PDF {pdf_url}: {e}")
            return None

    def generate_s3_key(self, cert_data: Dict, pdf_info: Dict) -> str:
        """
        Generate S3 key for storing the PDF
        
        Args:
            cert_data: Certificate data
            pdf_info: PDF information
            
        Returns:
            S3 key string
        """
        # Clean certificate number for filename
        cert_num = re.sub(r'[^\w\-_.]', '_', cert_data['certificate_number'])
        company = re.sub(r'[^\w\-_.]', '_', cert_data['company_name'][:50])  # Limit length
        pdf_type = pdf_info['type']
        
        # Create timestamp
        timestamp = datetime.now().strftime('%Y%m%d')
        
        return f"iscc_certificates/{timestamp}/{cert_num}_{company}_{pdf_type}.pdf"

    def upload_to_s3(self, pdf_content: bytes, s3_key: str, metadata: Dict) -> bool:
        """
        Upload PDF to AWS S3
        
        Args:
            pdf_content: PDF content as bytes
            s3_key: S3 key for the object
            metadata: Metadata to attach to the object
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=pdf_content,
                ContentType='application/pdf',
                Metadata=metadata
            )
            
            logger.info(f"Successfully uploaded to S3: {s3_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to upload to S3 {s3_key}: {e}")
            return False

    def process_certificates(self, max_pdfs: Optional[int] = None) -> Dict:
        """
        Main method to process certificates and download PDFs
        
        Args:
            max_pdfs: Maximum number of PDFs to process (None for all)
            
        Returns:
            Processing statistics
        """
        stats = {
            'certificates_found': 0,
            'pdfs_found': 0,
            'pdfs_downloaded': 0,
            'pdfs_uploaded': 0,
            'errors': 0
        }
        
        # Get the certificates page
        html_content = self.get_page_content(self.certificates_url)
        if not html_content:
            logger.error("Failed to get certificates page content")
            return stats
        
        # Extract certificate data
        certificates = self.extract_certificate_data(html_content)
        stats['certificates_found'] = len(certificates)
        
        pdf_count = 0
        for cert_data in certificates:
            for pdf_info in cert_data['pdf_links']:
                if max_pdfs and pdf_count >= max_pdfs:
                    logger.info(f"Reached maximum PDF limit: {max_pdfs}")
                    return stats
                
                pdf_url = pdf_info['url']
                stats['pdfs_found'] += 1
                
                # Skip if already processed
                url_hash = hashlib.md5(pdf_url.encode()).hexdigest()
                if url_hash in self.processed_pdfs:
                    logger.info(f"Skipping already processed PDF: {pdf_url}")
                    continue
                
                logger.info(f"Processing PDF: {pdf_url}")
                
                # Download PDF
                pdf_content = self.download_pdf(pdf_url)
                if not pdf_content:
                    stats['errors'] += 1
                    continue
                
                stats['pdfs_downloaded'] += 1
                
                # Generate S3 key
                s3_key = self.generate_s3_key(cert_data, pdf_info)
                
                # Prepare metadata
                metadata = {
                    'certificate_number': cert_data['certificate_number'],
                    'company_name': cert_data['company_name'],
                    'country': cert_data['country'],
                    'pdf_type': pdf_info['type'],
                    'source_url': pdf_url,
                    'scraped_date': datetime.now().isoformat()
                }
                
                # Upload to S3
                if self.upload_to_s3(pdf_content, s3_key, metadata):
                    stats['pdfs_uploaded'] += 1
                    self.processed_pdfs.add(url_hash)
                else:
                    stats['errors'] += 1
                
                pdf_count += 1
                
                # Small delay to be respectful
                time.sleep(1)
        
        return stats

    def save_progress(self, filename: str = 'processed_pdfs.json'):
        """Save processing progress to file"""
        with open(filename, 'w') as f:
            json.dump(list(self.processed_pdfs), f)

    def load_progress(self, filename: str = 'processed_pdfs.json'):
        """Load processing progress from file"""
        try:
            with open(filename, 'r') as f:
                self.processed_pdfs = set(json.load(f))
        except FileNotFoundError:
            logger.info("No previous progress file found")


def main():
    """Main function to run the scraper"""
    
    # Configuration - Replace with your actual credentials
    config = {
        # AWS Configuration
        'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID', 'your_aws_access_key'),
        'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY', 'your_aws_secret_key'),
        'aws_region': os.getenv('AWS_REGION', 'us-east-1'),
        's3_bucket_name': os.getenv('S3_BUCKET_NAME', 'your-iscc-pdfs-bucket'),
        
        # Scraping Service Configuration (choose one)
        'scraping_service': os.getenv('SCRAPING_SERVICE', 'selenium'),  # 'brightdata', 'tavily', or 'selenium'
        'brightdata_api_key': os.getenv('BRIGHTDATA_API_KEY'),  # Optional
        'tavily_api_key': os.getenv('TAVILY_API_KEY'),  # Optional
    }
    
    # Validate configuration
    required_aws_config = ['aws_access_key_id', 'aws_secret_access_key', 's3_bucket_name']
    for key in required_aws_config:
        if not config.get(key) or config[key].startswith('your_'):
            logger.error(f"Please set {key} in configuration")
            return
    
    # Initialize scraper
    scraper = ISCCPDFScraper(config)
    
    # Load previous progress if exists
    scraper.load_progress()
    
    try:
        # Process certificates (limit to 50 PDFs for testing)
        logger.info("Starting ISCC certificate PDF scraping...")
        stats = scraper.process_certificates(max_pdfs=50)  # Remove limit for full scrape
        
        # Print statistics
        logger.info("Scraping completed!")
        logger.info(f"Statistics: {json.dumps(stats, indent=2)}")
        
        # Save progress
        scraper.save_progress()
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        scraper.save_progress()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        scraper.save_progress()


if __name__ == "__main__":
    main()
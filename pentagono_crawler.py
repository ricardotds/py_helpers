"""
Pentágono S.A. DTVM Crawler Implementation
"""

from typing import List, Optional, Tuple
from datetime import date
import logging
import re
from urllib.parse import urljoin

from app.crawlers.base_crawler import BaseCrawler, DocumentInfo

logger = logging.getLogger(__name__)


class PentagonoCrawler(BaseCrawler):
    """Crawler for Pentágono S.A. DTVM"""
    
    def get_agent_name(self) -> str:
        return "Pentágono"
    
    def get_base_url(self) -> str:
        return "https://www.pentagonotrustee.com.br"
    
    def discover_documents(self, date_from: Optional[date] = None, 
                          date_to: Optional[date] = None) -> List[DocumentInfo]:
        """Discover all available documents from Pentágono"""
        documents = []
        
        try:
            # Step 1: Get list of all debentures
            logger.info("Fetching debentures list from Pentágono")
            debentures_url = f"{self.get_base_url()}/Site/Investidores?tipo=1"
            response = self.make_request(debentures_url)
            
            # Parse HTML to extract asset codes and company names
            asset_codes = self.parse_asset_codes(response.text)
            logger.info(f"Found {len(asset_codes)} assets")
            
            # Step 2: For each asset, get documents
            for i, (asset_code, company_name) in enumerate(asset_codes):
                try:
                    logger.info(f"Processing asset {i+1}/{len(asset_codes)}: {asset_code} - {company_name}")
                    docs = self.get_documents_for_asset(asset_code, company_name)
                    documents.extend(docs)
                    logger.info(f"Found {len(docs)} documents for {asset_code}")
                except Exception as e:
                    error_msg = f"Failed to get documents for {asset_code}: {str(e)}"
                    self.errors.append(error_msg)
                    logger.error(error_msg)
            
        except Exception as e:
            error_msg = f"Failed to discover documents: {str(e)}"
            self.errors.append(error_msg)
            logger.error(error_msg)
        
        # Filter by date if specified
        if date_from or date_to:
            documents = self.filter_documents_by_date(documents, date_from, date_to)
        
        return documents
    
    def parse_asset_codes(self, html: str) -> List[Tuple[str, str]]:
        """Parse HTML to extract asset codes and company names"""
        soup = self.parse_html(html)
        asset_codes = []
        
        try:
            # Look for the debentures table or list
            # Based on research, there should be a table with company and asset information
            
            # Try to find table rows with asset information
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        # Extract company name and asset code
                        company_cell = cells[0]
                        asset_cell = cells[1] if len(cells) > 1 else cells[0]
                        
                        company_name = company_cell.get_text(strip=True)
                        asset_code = asset_cell.get_text(strip=True)
                        
                        # Clean up the data
                        if company_name and asset_code and len(asset_code) <= 10:
                            # Remove common prefixes/suffixes
                            company_name = re.sub(r'\s+(S\.?A\.?|LTDA\.?|CIA\.?)$', '', company_name, flags=re.IGNORECASE)
                            asset_codes.append((asset_code.strip(), company_name.strip()))
            
            # If no table found, try alternative parsing
            if not asset_codes:
                # Look for links or divs that might contain asset information
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '')
                    text = link.get_text(strip=True)
                    
                    # Look for patterns that might indicate asset codes
                    if 'ativo=' in href:
                        asset_match = re.search(r'ativo=([^&]+)', href)
                        if asset_match:
                            asset_code = asset_match.group(1)
                            company_name = text or "Unknown Company"
                            asset_codes.append((asset_code, company_name))
            
        except Exception as e:
            logger.error(f"Error parsing asset codes: {str(e)}")
        
        # Remove duplicates
        asset_codes = list(set(asset_codes))
        
        return asset_codes
    
    def get_documents_for_asset(self, asset_code: str, company_name: str) -> List[DocumentInfo]:
        """Get all documents for a specific asset"""
        documents = []
        
        try:
            # Access documents page for asset
            docs_url = f"{self.get_base_url()}/Site/DetalhesEmissor?ativo={asset_code}&tipo=1&aba=tab-2"
            response = self.make_request(docs_url)
            
            # Parse documents from the page
            documents = self.parse_documents_page(response.text, asset_code, company_name, docs_url)
            
        except Exception as e:
            logger.error(f"Error getting documents for asset {asset_code}: {str(e)}")
        
        return documents
    
    def parse_documents_page(self, html: str, asset_code: str, company_name: str, source_url: str) -> List[DocumentInfo]:
        """Parse documents from the asset details page"""
        soup = self.parse_html(html)
        documents = []
        
        try:
            # Find document links in the "DOCUMENTOS DA EMISSÃO" section
            # Look for PDF links or download links
            
            # Method 1: Find all links that might be documents
            links = soup.find_all('a', href=True)
            for link in links:
                href = link.get('href', '')
                link_text = link.get_text(strip=True)
                
                # Check if this looks like a document link
                if self.is_valid_pdf_url(href) or self.is_valid_pdf_url(link_text):
                    doc_name = link_text or href.split('/')[-1]
                    
                    # Skip if no meaningful name
                    if not doc_name or len(doc_name) < 3:
                        continue
                    
                    # Extract date from document name if available
                    doc_date = self.extract_date_from_text(doc_name)
                    
                    # Determine document type from name
                    doc_type = self.classify_document_type(doc_name)
                    
                    # Build full URL
                    download_url = self.build_absolute_url(href)
                    
                    # Extract emission and series info if available
                    emission_number, series = self.extract_emission_info(doc_name, html)
                    
                    documents.append(DocumentInfo(
                        company_name=company_name,
                        asset_code=asset_code,
                        emission_number=emission_number,
                        series=series,
                        document_type=doc_type,
                        document_name=doc_name,
                        download_url=download_url,
                        document_date=doc_date,
                        file_size=None,
                        last_modified=None,
                        source_url=source_url
                    ))
            
            # Method 2: Look for specific document sections
            # Find sections that might contain documents
            sections = soup.find_all(['div', 'section'], class_=re.compile(r'document|arquivo|emissao', re.I))
            for section in sections:
                section_links = section.find_all('a', href=True)
                for link in section_links:
                    href = link.get('href', '')
                    if self.is_valid_pdf_url(href):
                        # Process similar to above
                        doc_name = link.get_text(strip=True) or href.split('/')[-1]
                        if doc_name and len(doc_name) >= 3:
                            # Avoid duplicates
                            download_url = self.build_absolute_url(href)
                            if not any(doc.download_url == download_url for doc in documents):
                                doc_date = self.extract_date_from_text(doc_name)
                                doc_type = self.classify_document_type(doc_name)
                                emission_number, series = self.extract_emission_info(doc_name, html)
                                
                                documents.append(DocumentInfo(
                                    company_name=company_name,
                                    asset_code=asset_code,
                                    emission_number=emission_number,
                                    series=series,
                                    document_type=doc_type,
                                    document_name=doc_name,
                                    download_url=download_url,
                                    document_date=doc_date,
                                    file_size=None,
                                    last_modified=None,
                                    source_url=source_url
                                ))
            
        except Exception as e:
            logger.error(f"Error parsing documents page: {str(e)}")
        
        return documents
    
    def extract_emission_info(self, doc_name: str, html: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract emission number and series from document name or page content"""
        emission_number = None
        series = None
        
        # Try to extract from document name
        emission_match = re.search(r'(\d+)ª?\s*emissão', doc_name, re.IGNORECASE)
        if emission_match:
            emission_number = emission_match.group(1)
        
        series_match = re.search(r'(\d+)ª?\s*série', doc_name, re.IGNORECASE)
        if series_match:
            series = series_match.group(1)
        
        # If not found in document name, try to extract from page content
        if not emission_number or not series:
            soup = self.parse_html(html)
            page_text = soup.get_text()
            
            if not emission_number:
                emission_match = re.search(r'(\d+)ª?\s*emissão', page_text, re.IGNORECASE)
                if emission_match:
                    emission_number = emission_match.group(1)
            
            if not series:
                series_match = re.search(r'(\d+)ª?\s*série', page_text, re.IGNORECASE)
                if series_match:
                    series = series_match.group(1)
        
        return emission_number, series
    
    def filter_documents_by_date(self, documents: List[DocumentInfo], 
                                date_from: Optional[date], 
                                date_to: Optional[date]) -> List[DocumentInfo]:
        """Filter documents by date range"""
        if not date_from and not date_to:
            return documents
        
        filtered = []
        for doc in documents:
            if doc.document_date:
                if date_from and doc.document_date < date_from:
                    continue
                if date_to and doc.document_date > date_to:
                    continue
            filtered.append(doc)
        
        return filtered


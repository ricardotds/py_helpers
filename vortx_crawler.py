"""
Vórtx DTVM Ltda. Crawler Implementation
"""

from typing import List, Optional, Dict
from datetime import date
import logging
import re
from urllib.parse import urljoin

from app.crawlers.base_crawler import BaseCrawler, DocumentInfo

logger = logging.getLogger(__name__)


class VortxCrawler(BaseCrawler):
    """Crawler for Vórtx DTVM Ltda."""
    
    def get_agent_name(self) -> str:
        return "Vórtx"
    
    def get_base_url(self) -> str:
        return "https://www.vortx.com.br"
    
    def discover_documents(self, date_from: Optional[date] = None, 
                          date_to: Optional[date] = None) -> List[DocumentInfo]:
        """Discover all available documents from Vórtx"""
        documents = []
        
        try:
            # Step 1: Get list of all debentures
            logger.info("Fetching debentures list from Vórtx")
            debentures_url = f"{self.get_base_url()}/investidor/debenture"
            response = self.make_request(debentures_url)
            
            # Parse operations from the table
            operations = self.parse_operations_table(response.text)
            logger.info(f"Found {len(operations)} operations")
            
            # Step 2: For each operation, get documents
            for i, operation in enumerate(operations):
                try:
                    logger.info(f"Processing operation {i+1}/{len(operations)}: {operation['company_name']}")
                    docs = self.get_documents_for_operation(operation)
                    documents.extend(docs)
                    logger.info(f"Found {len(docs)} documents for {operation['company_name']}")
                except Exception as e:
                    error_msg = f"Failed to get documents for operation {operation['id']}: {str(e)}"
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
    
    def parse_operations_table(self, html: str) -> List[Dict]:
        """Parse operations table to extract operation details"""
        soup = self.parse_html(html)
        operations = []
        
        try:
            # Find the main table with debenture operations
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                # Skip if not enough rows
                if len(rows) < 2:
                    continue
                
                # Process data rows (skip header)
                for row in rows[1:]:
                    cells = row.find_all(['td', 'th'])
                    
                    # Based on research, the table should have columns for:
                    # Company, Emission, Series, Asset Code, etc.
                    if len(cells) >= 4:
                        try:
                            # Look for company link that contains operation ID
                            company_cell = cells[0] if cells else None
                            company_link = company_cell.find('a') if company_cell else None
                            
                            if company_link and company_link.get('href'):
                                href = company_link.get('href')
                                
                                # Extract operation ID from URL
                                operation_id = self.extract_operation_id(href)
                                
                                if operation_id:
                                    company_name = company_link.get_text(strip=True)
                                    
                                    # Extract other information from cells
                                    emission = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                                    series = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                                    asset_code = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                                    
                                    # Build full URL
                                    operation_url = self.build_absolute_url(href)
                                    
                                    operations.append({
                                        'id': operation_id,
                                        'company_name': company_name,
                                        'emission': emission,
                                        'series': series,
                                        'asset_code': asset_code,
                                        'url': operation_url
                                    })
                        
                        except Exception as e:
                            logger.warning(f"Error parsing table row: {str(e)}")
                            continue
            
            # Alternative parsing if table structure is different
            if not operations:
                # Look for links that might contain operation information
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '')
                    if 'operacao' in href and 'operacaoDataId=' in href:
                        operation_id = self.extract_operation_id(href)
                        if operation_id:
                            company_name = link.get_text(strip=True) or "Unknown Company"
                            operation_url = self.build_absolute_url(href)
                            
                            operations.append({
                                'id': operation_id,
                                'company_name': company_name,
                                'emission': "",
                                'series': "",
                                'asset_code': "",
                                'url': operation_url
                            })
            
        except Exception as e:
            logger.error(f"Error parsing operations table: {str(e)}")
        
        # Remove duplicates based on operation ID
        seen_ids = set()
        unique_operations = []
        for op in operations:
            if op['id'] not in seen_ids:
                seen_ids.add(op['id'])
                unique_operations.append(op)
        
        return unique_operations
    
    def extract_operation_id(self, url: str) -> str:
        """Extract operation ID from URL"""
        match = re.search(r'operacaoDataId=(\d+)', url)
        return match.group(1) if match else ''
    
    def get_documents_for_operation(self, operation: Dict) -> List[DocumentInfo]:
        """Get all documents for a specific operation"""
        documents = []
        
        try:
            # Access operation page
            response = self.make_request(operation['url'])
            
            # Parse documents from the operation page
            documents = self.parse_operation_documents(response.text, operation)
            
        except Exception as e:
            logger.error(f"Error getting documents for operation {operation['id']}: {str(e)}")
        
        return documents
    
    def parse_operation_documents(self, html: str, operation: Dict) -> List[DocumentInfo]:
        """Parse documents from operation page"""
        soup = self.parse_html(html)
        documents = []
        
        try:
            # Based on research, documents are in expandable sections
            # Look for document sections and links
            document_sections = [
                'EMISSÃO DEBÊNTURES',
                'AF AÇÕES', 
                'CESSÃO FIDUCIÁRIA',
                'COVENANTS',
                'NOTIFICAÇÃO',
                'DFP'
            ]
            
            # Method 1: Look for documents in specific sections
            for section_name in document_sections:
                section_docs = self.find_documents_in_section(soup, section_name, operation)
                documents.extend(section_docs)
            
            # Method 2: General document link search
            # Look for any PDF links on the page
            links = soup.find_all('a', href=True)
            for link in links:
                href = link.get('href', '')
                link_text = link.get_text(strip=True)
                
                if self.is_valid_pdf_url(href) or self.is_valid_pdf_url(link_text):
                    doc_name = link_text or href.split('/')[-1]
                    
                    # Skip if no meaningful name or already found
                    if not doc_name or len(doc_name) < 3:
                        continue
                    
                    download_url = self.build_absolute_url(href)
                    
                    # Check if already added
                    if any(doc.download_url == download_url for doc in documents):
                        continue
                    
                    # Extract date from document name
                    doc_date = self.extract_date_from_text(doc_name)
                    
                    # Classify document type
                    doc_type = self.classify_vortx_document_type(doc_name, "")
                    
                    documents.append(DocumentInfo(
                        company_name=operation['company_name'],
                        asset_code=operation['asset_code'],
                        emission_number=operation['emission'],
                        series=operation['series'],
                        document_type=doc_type,
                        document_name=doc_name,
                        download_url=download_url,
                        document_date=doc_date,
                        file_size=None,
                        last_modified=None,
                        source_url=operation['url']
                    ))
            
            # Method 3: Look for expandable content or JavaScript-loaded content
            # Find divs or sections that might contain hidden documents
            expandable_sections = soup.find_all(['div', 'section'], 
                                               class_=re.compile(r'collapse|expand|accordion|tab', re.I))
            
            for section in expandable_sections:
                section_links = section.find_all('a', href=True)
                for link in section_links:
                    href = link.get('href', '')
                    if self.is_valid_pdf_url(href):
                        doc_name = link.get_text(strip=True) or href.split('/')[-1]
                        download_url = self.build_absolute_url(href)
                        
                        # Check if already added
                        if any(doc.download_url == download_url for doc in documents):
                            continue
                        
                        if doc_name and len(doc_name) >= 3:
                            doc_date = self.extract_date_from_text(doc_name)
                            doc_type = self.classify_vortx_document_type(doc_name, section.get_text())
                            
                            documents.append(DocumentInfo(
                                company_name=operation['company_name'],
                                asset_code=operation['asset_code'],
                                emission_number=operation['emission'],
                                series=operation['series'],
                                document_type=doc_type,
                                document_name=doc_name,
                                download_url=download_url,
                                document_date=doc_date,
                                file_size=None,
                                last_modified=None,
                                source_url=operation['url']
                            ))
            
        except Exception as e:
            logger.error(f"Error parsing operation documents: {str(e)}")
        
        return documents
    
    def find_documents_in_section(self, soup, section_name: str, operation: Dict) -> List[DocumentInfo]:
        """Find documents in a specific section"""
        documents = []
        
        try:
            # Look for section headers or containers
            section_elements = soup.find_all(text=re.compile(section_name, re.I))
            
            for element in section_elements:
                # Find the parent container
                parent = element.parent
                if parent:
                    # Look for links in this section
                    section_links = parent.find_all('a', href=True)
                    for link in section_links:
                        href = link.get('href', '')
                        if self.is_valid_pdf_url(href):
                            doc_name = link.get_text(strip=True) or href.split('/')[-1]
                            
                            if doc_name and len(doc_name) >= 3:
                                download_url = self.build_absolute_url(href)
                                doc_date = self.extract_date_from_text(doc_name)
                                doc_type = self.classify_vortx_document_type(doc_name, section_name)
                                
                                documents.append(DocumentInfo(
                                    company_name=operation['company_name'],
                                    asset_code=operation['asset_code'],
                                    emission_number=operation['emission'],
                                    series=operation['series'],
                                    document_type=doc_type,
                                    document_name=doc_name,
                                    download_url=download_url,
                                    document_date=doc_date,
                                    file_size=None,
                                    last_modified=None,
                                    source_url=operation['url']
                                ))
        
        except Exception as e:
            logger.warning(f"Error finding documents in section {section_name}: {str(e)}")
        
        return documents
    
    def classify_vortx_document_type(self, doc_name: str, section: str) -> str:
        """Classify document type for Vórtx documents"""
        doc_name_lower = doc_name.lower()
        section_lower = section.lower()
        
        # Use section information for classification
        if 'emissão' in section_lower or 'debêntures' in section_lower:
            return 'Emissão de Debêntures'
        elif 'cessão' in section_lower or 'fiduciária' in section_lower:
            return 'Cessão Fiduciária'
        elif 'covenant' in section_lower:
            return 'Covenants'
        elif 'notificação' in section_lower:
            return 'Notificação'
        elif 'dfp' in section_lower:
            return 'Demonstrações Financeiras'
        elif 'af' in section_lower and 'ações' in section_lower:
            return 'Agente Fiduciário - Ações'
        
        # Fall back to document name classification
        return self.classify_document_type(doc_name)
    
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


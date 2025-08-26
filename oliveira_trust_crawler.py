"""
Oliveira Trust DTVM S.A. Crawler Implementation
"""

from typing import List, Optional, Dict
from datetime import date
import logging
import re
from urllib.parse import urljoin, parse_qs, urlparse

from app.crawlers.base_crawler import BaseCrawler, DocumentInfo

logger = logging.getLogger(__name__)


class OliveiraTrustCrawler(BaseCrawler):
    """Crawler for Oliveira Trust DTVM S.A."""
    
    def get_agent_name(self) -> str:
        return "Oliveira Trust"
    
    def get_base_url(self) -> str:
        return "https://www.oliveiratrust.com.br"
    
    def discover_documents(self, date_from: Optional[date] = None, 
                          date_to: Optional[date] = None) -> List[DocumentInfo]:
        """Discover all available documents from Oliveira Trust"""
        documents = []
        
        try:
            # Step 1: Access the investor portal
            logger.info("Accessing Oliveira Trust investor portal")
            portal_url = f"{self.get_base_url()}/portal-do-investidor"
            response = self.make_request(portal_url)
            
            # Step 2: Get all documents with pagination
            documents = self.get_all_documents_with_pagination(response.text)
            
        except Exception as e:
            error_msg = f"Failed to discover documents: {str(e)}"
            self.errors.append(error_msg)
            logger.error(error_msg)
        
        # Filter by date if specified
        if date_from or date_to:
            documents = self.filter_documents_by_date(documents, date_from, date_to)
        
        return documents
    
    def get_all_documents_with_pagination(self, initial_html: str) -> List[DocumentInfo]:
        """Get all documents handling pagination"""
        all_documents = []
        page = 1
        
        try:
            # Parse initial page
            documents = self.parse_documents_page(initial_html)
            all_documents.extend(documents)
            logger.info(f"Page {page}: Found {len(documents)} documents")
            
            # Check for pagination and get additional pages
            soup = self.parse_html(initial_html)
            max_pages = self.get_max_pages(soup)
            
            if max_pages and max_pages > 1:
                logger.info(f"Found {max_pages} pages total")
                
                # Get remaining pages
                for page in range(2, min(max_pages + 1, 50)):  # Limit to 50 pages for safety
                    try:
                        page_url = f"{self.get_base_url()}/portal-do-investidor?page={page}"
                        response = self.make_request(page_url)
                        
                        page_documents = self.parse_documents_page(response.text)
                        all_documents.extend(page_documents)
                        
                        logger.info(f"Page {page}: Found {len(page_documents)} documents")
                        
                        # Update pagination tracking
                        self.last_page_processed = page
                        self.total_pages = max_pages
                        
                        # If no documents found, might have reached the end
                        if not page_documents:
                            logger.info(f"No documents found on page {page}, stopping pagination")
                            break
                        
                    except Exception as e:
                        logger.error(f"Error processing page {page}: {str(e)}")
                        break
            
        except Exception as e:
            logger.error(f"Error in pagination: {str(e)}")
        
        return all_documents
    
    def get_max_pages(self, soup) -> Optional[int]:
        """Extract maximum number of pages from pagination"""
        try:
            # Look for pagination elements
            pagination = soup.find('nav', class_=re.compile(r'pagination', re.I))
            if not pagination:
                pagination = soup.find('div', class_=re.compile(r'pagination|pager', re.I))
            
            if pagination:
                # Find page links
                page_links = pagination.find_all('a', href=True)
                max_page = 1
                
                for link in page_links:
                    href = link.get('href', '')
                    text = link.get_text(strip=True)
                    
                    # Try to extract page number from href
                    page_match = re.search(r'page=(\d+)', href)
                    if page_match:
                        page_num = int(page_match.group(1))
                        max_page = max(max_page, page_num)
                    
                    # Try to extract page number from text
                    if text.isdigit():
                        page_num = int(text)
                        max_page = max(max_page, page_num)
                
                return max_page if max_page > 1 else None
            
            # Alternative: look for "última" or "last" page
            last_links = soup.find_all('a', text=re.compile(r'última|last|»', re.I))
            for link in last_links:
                href = link.get('href', '')
                page_match = re.search(r'page=(\d+)', href)
                if page_match:
                    return int(page_match.group(1))
            
        except Exception as e:
            logger.warning(f"Error extracting max pages: {str(e)}")
        
        return None
    
    def parse_documents_page(self, html: str) -> List[DocumentInfo]:
        """Parse documents from a page"""
        soup = self.parse_html(html)
        documents = []
        
        try:
            # Based on research, documents are displayed in a table or list format
            # Look for document entries
            
            # Method 1: Look for table rows with document information
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows[1:]:  # Skip header
                    doc_info = self.parse_document_row(row)
                    if doc_info:
                        documents.append(doc_info)
            
            # Method 2: Look for document cards or divs
            if not documents:
                # Look for document containers
                doc_containers = soup.find_all(['div', 'article'], 
                                             class_=re.compile(r'document|arquivo|item', re.I))
                
                for container in doc_containers:
                    doc_info = self.parse_document_container(container)
                    if doc_info:
                        documents.append(doc_info)
            
            # Method 3: Look for direct PDF links
            if not documents:
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '')
                    if self.is_valid_pdf_url(href):
                        doc_info = self.create_document_from_link(link)
                        if doc_info:
                            documents.append(doc_info)
            
        except Exception as e:
            logger.error(f"Error parsing documents page: {str(e)}")
        
        return documents
    
    def parse_document_row(self, row) -> Optional[DocumentInfo]:
        """Parse document information from table row"""
        try:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 3:
                return None
            
            # Extract information from cells
            # Based on research, columns might be: Company, Document Type, Document Name, Date, etc.
            
            company_name = ""
            document_type = ""
            document_name = ""
            download_url = ""
            doc_date = None
            asset_code = ""
            
            # Try to find download link
            for cell in cells:
                link = cell.find('a', href=True)
                if link and self.is_valid_pdf_url(link.get('href', '')):
                    download_url = self.build_absolute_url(link.get('href'))
                    document_name = link.get_text(strip=True) or link.get('href').split('/')[-1]
                    break
            
            if not download_url:
                return None
            
            # Extract other information from cells
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            
            # Try to identify company name (usually first or second column)
            for text in cell_texts:
                if text and len(text) > 3 and not self.is_valid_pdf_url(text):
                    if not company_name:
                        company_name = text
                    elif not document_type and text != company_name:
                        document_type = text
                        break
            
            # Extract date from any cell
            for text in cell_texts:
                date_found = self.extract_date_from_text(text)
                if date_found:
                    doc_date = date_found
                    break
            
            # Extract asset code if present
            for text in cell_texts:
                if re.match(r'^[A-Z]{4}\d{2}$', text):  # Pattern like ADAG11
                    asset_code = text
                    break
            
            # Classify document type if not found
            if not document_type:
                document_type = self.classify_document_type(document_name)
            
            return DocumentInfo(
                company_name=company_name or "Unknown Company",
                asset_code=asset_code or None,
                emission_number=None,
                series=None,
                document_type=document_type,
                document_name=document_name,
                download_url=download_url,
                document_date=doc_date,
                file_size=None,
                last_modified=None,
                source_url=f"{self.get_base_url()}/portal-do-investidor"
            )
            
        except Exception as e:
            logger.warning(f"Error parsing document row: {str(e)}")
            return None
    
    def parse_document_container(self, container) -> Optional[DocumentInfo]:
        """Parse document information from container div"""
        try:
            # Find download link
            link = container.find('a', href=True)
            if not link or not self.is_valid_pdf_url(link.get('href', '')):
                return None
            
            download_url = self.build_absolute_url(link.get('href'))
            document_name = link.get_text(strip=True) or link.get('href').split('/')[-1]
            
            # Extract other information from container text
            container_text = container.get_text()
            
            # Try to find company name
            company_name = "Unknown Company"
            company_patterns = [
                r'Empresa:\s*([^\n]+)',
                r'Company:\s*([^\n]+)',
                r'([A-Z][A-Za-z\s]+(?:S\.?A\.?|LTDA\.?))'
            ]
            
            for pattern in company_patterns:
                match = re.search(pattern, container_text)
                if match:
                    company_name = match.group(1).strip()
                    break
            
            # Extract date
            doc_date = self.extract_date_from_text(container_text)
            
            # Extract asset code
            asset_code = None
            asset_match = re.search(r'\b([A-Z]{4}\d{2})\b', container_text)
            if asset_match:
                asset_code = asset_match.group(1)
            
            # Classify document type
            document_type = self.classify_document_type(document_name, container_text)
            
            return DocumentInfo(
                company_name=company_name,
                asset_code=asset_code,
                emission_number=None,
                series=None,
                document_type=document_type,
                document_name=document_name,
                download_url=download_url,
                document_date=doc_date,
                file_size=None,
                last_modified=None,
                source_url=f"{self.get_base_url()}/portal-do-investidor"
            )
            
        except Exception as e:
            logger.warning(f"Error parsing document container: {str(e)}")
            return None
    
    def create_document_from_link(self, link) -> Optional[DocumentInfo]:
        """Create document info from a simple link"""
        try:
            href = link.get('href', '')
            if not self.is_valid_pdf_url(href):
                return None
            
            download_url = self.build_absolute_url(href)
            document_name = link.get_text(strip=True) or href.split('/')[-1]
            
            if not document_name or len(document_name) < 3:
                return None
            
            # Try to extract information from link context
            parent = link.parent
            context_text = parent.get_text() if parent else ""
            
            # Extract company name from context
            company_name = "Unknown Company"
            company_match = re.search(r'([A-Z][A-Za-z\s]+(?:S\.?A\.?|LTDA\.?))', context_text)
            if company_match:
                company_name = company_match.group(1).strip()
            
            # Extract date
            doc_date = self.extract_date_from_text(context_text + " " + document_name)
            
            # Extract asset code
            asset_code = None
            asset_match = re.search(r'\b([A-Z]{4}\d{2})\b', context_text + " " + document_name)
            if asset_match:
                asset_code = asset_match.group(1)
            
            # Classify document type
            document_type = self.classify_document_type(document_name, context_text)
            
            return DocumentInfo(
                company_name=company_name,
                asset_code=asset_code,
                emission_number=None,
                series=None,
                document_type=document_type,
                document_name=document_name,
                download_url=download_url,
                document_date=doc_date,
                file_size=None,
                last_modified=None,
                source_url=f"{self.get_base_url()}/portal-do-investidor"
            )
            
        except Exception as e:
            logger.warning(f"Error creating document from link: {str(e)}")
            return None
    
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


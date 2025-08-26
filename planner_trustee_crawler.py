"""
Planner Trustee DTVM Ltda Crawler
Crawls documents from Trustee DTVM fiduciary portal
"""

import asyncio
import aiohttp
from typing import List, Dict, Any, Optional
from datetime import datetime
import hashlib
import os
from urllib.parse import urljoin, urlparse
import logging

from .base_crawler import BaseCrawler

logger = logging.getLogger(__name__)


class PlannerTrusteeCrawler(BaseCrawler):
    """Crawler for Planner Trustee DTVM Ltda documents"""
    
    def __init__(self, session: aiohttp.ClientSession, storage_path: str):
        super().__init__(session, storage_path)
        self.base_url = "https://www.trusteedtvm.com.br"
        self.fiduciary_portal = "https://fiduciario.com.br"
        self.name = "Planner Trustee DTVM"
        
    async def crawl_documents(self) -> List[Dict[str, Any]]:
        """
        Crawl documents from Planner Trustee DTVM
        """
        documents = []
        
        try:
            # First try to access the main website
            main_docs = await self._crawl_main_website()
            documents.extend(main_docs)
            
            # Then try to access the fiduciary portal
            portal_docs = await self._crawl_fiduciary_portal()
            documents.extend(portal_docs)
            
        except Exception as e:
            logger.error(f"Error crawling Planner Trustee documents: {e}")
            
        return documents
    
    async def _crawl_main_website(self) -> List[Dict[str, Any]]:
        """Crawl documents from main website"""
        documents = []
        
        try:
            # Access main website
            async with self.session.get(self.base_url) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # Look for document links in the HTML
                    soup = self._get_soup(html)
                    
                    # Find links to PDF documents
                    pdf_links = soup.find_all('a', href=lambda x: x and x.endswith('.pdf'))
                    
                    for link in pdf_links:
                        href = link.get('href')
                        if href:
                            doc_url = urljoin(self.base_url, href)
                            title = link.get_text(strip=True) or link.get('title', 'Document')
                            
                            # Download and process document
                            doc_info = await self._process_document(doc_url, title)
                            if doc_info:
                                documents.append(doc_info)
                                
        except Exception as e:
            logger.error(f"Error crawling main website: {e}")
            
        return documents
    
    async def _crawl_fiduciary_portal(self) -> List[Dict[str, Any]]:
        """Crawl documents from fiduciary portal"""
        documents = []
        
        try:
            # Try to access fiduciary portal
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            
            async with self.session.get(self.fiduciary_portal, headers=headers) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # Check if we need to handle Cloudflare protection
                    if 'cloudflare' in html.lower() or 'checking your browser' in html.lower():
                        logger.warning("Cloudflare protection detected on fiduciary portal")
                        return documents
                    
                    soup = self._get_soup(html)
                    
                    # Look for document sections or links
                    doc_sections = soup.find_all(['div', 'section'], class_=lambda x: x and 'document' in x.lower())
                    
                    for section in doc_sections:
                        links = section.find_all('a', href=lambda x: x and x.endswith('.pdf'))
                        
                        for link in links:
                            href = link.get('href')
                            if href:
                                doc_url = urljoin(self.fiduciary_portal, href)
                                title = link.get_text(strip=True) or 'Fiduciary Document'
                                
                                doc_info = await self._process_document(doc_url, title)
                                if doc_info:
                                    documents.append(doc_info)
                                    
        except Exception as e:
            logger.error(f"Error crawling fiduciary portal: {e}")
            
        return documents
    
    async def _process_document(self, url: str, title: str) -> Optional[Dict[str, Any]]:
        """Process and download a document"""
        try:
            # Generate file hash for duplicate detection
            url_hash = hashlib.md5(url.encode()).hexdigest()
            
            # Download document
            async with self.session.get(url) as response:
                if response.status == 200:
                    content = await response.read()
                    
                    # Generate content hash
                    content_hash = hashlib.md5(content).hexdigest()
                    
                    # Save file
                    filename = f"planner_trustee_{url_hash}.pdf"
                    file_path = os.path.join(self.storage_path, filename)
                    
                    with open(file_path, 'wb') as f:
                        f.write(content)
                    
                    # Extract metadata
                    file_size = len(content)
                    
                    return {
                        'title': title,
                        'url': url,
                        'file_path': file_path,
                        'file_hash': content_hash,
                        'file_size': file_size,
                        'document_type': self._classify_document_type(title),
                        'company_name': self._extract_company_name(title),
                        'emission_date': self._extract_date(title),
                        'downloaded_at': datetime.utcnow(),
                        'source': 'Planner Trustee DTVM'
                    }
                    
        except Exception as e:
            logger.error(f"Error processing document {url}: {e}")
            
        return None
    
    def _classify_document_type(self, title: str) -> str:
        """Classify document type based on title"""
        title_lower = title.lower()
        
        if any(word in title_lower for word in ['debênture', 'debenture']):
            return 'debenture_emission'
        elif any(word in title_lower for word in ['assembleia', 'assembly']):
            return 'assembly'
        elif any(word in title_lower for word in ['relatório', 'report']):
            return 'report'
        elif any(word in title_lower for word in ['demonstrações', 'financeiro']):
            return 'financial_statement'
        elif any(word in title_lower for word in ['ata', 'minutes']):
            return 'meeting_minutes'
        else:
            return 'other'
    
    def _extract_company_name(self, title: str) -> Optional[str]:
        """Extract company name from document title"""
        # Simple extraction - can be enhanced with more sophisticated parsing
        if 'S.A.' in title:
            parts = title.split('S.A.')
            if parts:
                return parts[0].strip() + ' S.A.'
        elif 'LTDA' in title.upper():
            parts = title.upper().split('LTDA')
            if parts:
                return parts[0].strip() + ' Ltda'
        
        return None
    
    def _extract_date(self, title: str) -> Optional[datetime]:
        """Extract date from document title"""
        import re
        
        # Look for date patterns
        date_patterns = [
            r'(\d{2})/(\d{2})/(\d{4})',  # DD/MM/YYYY
            r'(\d{2})-(\d{2})-(\d{4})',  # DD-MM-YYYY
            r'(\d{4})-(\d{2})-(\d{2})',  # YYYY-MM-DD
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, title)
            if match:
                try:
                    if pattern == r'(\d{4})-(\d{2})-(\d{2})':
                        year, month, day = match.groups()
                    else:
                        day, month, year = match.groups()
                    
                    return datetime(int(year), int(month), int(day))
                except ValueError:
                    continue
        
        return None


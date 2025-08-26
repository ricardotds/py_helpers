"""
BRL Trust DTVM S.A. (Apex Group) Crawler
Crawls documents from Apex Group Brazil regulatory documents portal
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


class BRLTrustCrawler(BaseCrawler):
    """Crawler for BRL Trust DTVM S.A. (now Apex Group) documents"""
    
    def __init__(self, session: aiohttp.ClientSession, storage_path: str):
        super().__init__(session, storage_path)
        self.base_url = "https://www.apexgroup.com/apex-brazil/"
        self.documents_url = "https://www.apexgroup.com/apex-brazil/documentos-regulatorios/"
        self.name = "BRL Trust DTVM (Apex Group)"
        
    async def crawl_documents(self) -> List[Dict[str, Any]]:
        """
        Crawl documents from Apex Group Brazil regulatory documents portal
        """
        documents = []
        
        try:
            # Crawl BRL documents
            brl_docs = await self._crawl_brl_documents()
            documents.extend(brl_docs)
            
            # Crawl MAF documents
            maf_docs = await self._crawl_maf_documents()
            documents.extend(maf_docs)
            
            # Crawl AAM documents
            aam_docs = await self._crawl_aam_documents()
            documents.extend(aam_docs)
            
        except Exception as e:
            logger.error(f"Error crawling BRL Trust documents: {e}")
            
        return documents
    
    async def _crawl_brl_documents(self) -> List[Dict[str, Any]]:
        """Crawl BRL Trust specific documents"""
        documents = []
        
        try:
            async with self.session.get(self.documents_url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = self._get_soup(html)
                    
                    # Find BRL documents tab/section
                    brl_section = soup.find(['div', 'section'], attrs={'id': lambda x: x and 'brl' in x.lower()})
                    if not brl_section:
                        # Look for BRL in class names or text
                        brl_section = soup.find(['div', 'section'], class_=lambda x: x and 'brl' in x.lower())
                    
                    if brl_section:
                        # Find all PDF links in BRL section
                        pdf_links = brl_section.find_all('a', href=lambda x: x and x.endswith('.pdf'))
                        
                        for link in pdf_links:
                            href = link.get('href')
                            if href:
                                doc_url = urljoin(self.base_url, href)
                                title = link.get_text(strip=True) or link.get('title', 'BRL Document')
                                
                                doc_info = await self._process_document(doc_url, title, 'BRL')
                                if doc_info:
                                    documents.append(doc_info)
                    
                    # Also look for general document links that might be BRL-related
                    all_links = soup.find_all('a', href=lambda x: x and x.endswith('.pdf'))
                    for link in all_links:
                        link_text = link.get_text(strip=True).lower()
                        if 'brl' in link_text or 'trust' in link_text:
                            href = link.get('href')
                            if href:
                                doc_url = urljoin(self.base_url, href)
                                title = link.get_text(strip=True) or 'BRL Document'
                                
                                doc_info = await self._process_document(doc_url, title, 'BRL')
                                if doc_info:
                                    documents.append(doc_info)
                                    
        except Exception as e:
            logger.error(f"Error crawling BRL documents: {e}")
            
        return documents
    
    async def _crawl_maf_documents(self) -> List[Dict[str, Any]]:
        """Crawl MAF DTVM documents"""
        documents = []
        
        try:
            async with self.session.get(self.documents_url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = self._get_soup(html)
                    
                    # Find MAF documents section
                    maf_section = soup.find(['div', 'section'], attrs={'id': lambda x: x and 'maf' in x.lower()})
                    if not maf_section:
                        maf_section = soup.find(['div', 'section'], class_=lambda x: x and 'maf' in x.lower())
                    
                    if maf_section:
                        pdf_links = maf_section.find_all('a', href=lambda x: x and x.endswith('.pdf'))
                        
                        for link in pdf_links:
                            href = link.get('href')
                            if href:
                                doc_url = urljoin(self.base_url, href)
                                title = link.get_text(strip=True) or 'MAF Document'
                                
                                doc_info = await self._process_document(doc_url, title, 'MAF')
                                if doc_info:
                                    documents.append(doc_info)
                                    
        except Exception as e:
            logger.error(f"Error crawling MAF documents: {e}")
            
        return documents
    
    async def _crawl_aam_documents(self) -> List[Dict[str, Any]]:
        """Crawl AAM (Apex Asset Management) documents"""
        documents = []
        
        try:
            async with self.session.get(self.documents_url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = self._get_soup(html)
                    
                    # Find AAM documents section
                    aam_section = soup.find(['div', 'section'], attrs={'id': lambda x: x and 'aam' in x.lower()})
                    if not aam_section:
                        aam_section = soup.find(['div', 'section'], class_=lambda x: x and 'aam' in x.lower())
                    
                    if aam_section:
                        pdf_links = aam_section.find_all('a', href=lambda x: x and x.endswith('.pdf'))
                        
                        for link in pdf_links:
                            href = link.get('href')
                            if href:
                                doc_url = urljoin(self.base_url, href)
                                title = link.get_text(strip=True) or 'AAM Document'
                                
                                doc_info = await self._process_document(doc_url, title, 'AAM')
                                if doc_info:
                                    documents.append(doc_info)
                                    
        except Exception as e:
            logger.error(f"Error crawling AAM documents: {e}")
            
        return documents
    
    async def _process_document(self, url: str, title: str, entity: str) -> Optional[Dict[str, Any]]:
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
                    filename = f"brl_trust_{entity.lower()}_{url_hash}.pdf"
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
                        'company_name': f'Apex Group ({entity})',
                        'entity': entity,
                        'emission_date': self._extract_date_from_title(title),
                        'downloaded_at': datetime.utcnow(),
                        'source': 'BRL Trust DTVM (Apex Group)'
                    }
                    
        except Exception as e:
            logger.error(f"Error processing document {url}: {e}")
            
        return None
    
    def _classify_document_type(self, title: str) -> str:
        """Classify document type based on title"""
        title_lower = title.lower()
        
        if any(word in title_lower for word in ['demonstrações financeiras', 'financial statements']):
            return 'financial_statement'
        elif any(word in title_lower for word in ['política', 'policy']):
            return 'policy'
        elif any(word in title_lower for word in ['código de ética', 'code of ethics']):
            return 'ethics_code'
        elif any(word in title_lower for word in ['manual', 'procedure']):
            return 'manual'
        elif any(word in title_lower for word in ['relatório', 'report']):
            return 'report'
        elif any(word in title_lower for word in ['debênture', 'debenture']):
            return 'debenture_emission'
        elif any(word in title_lower for word in ['assembleia', 'assembly']):
            return 'assembly'
        elif any(word in title_lower for word in ['ouvidoria', 'ombudsman']):
            return 'ombudsman_report'
        else:
            return 'regulatory_document'
    
    def _extract_date_from_title(self, title: str) -> Optional[datetime]:
        """Extract date from document title"""
        import re
        
        # Look for year patterns first
        year_match = re.search(r'20\d{2}', title)
        if year_match:
            year = int(year_match.group())
            
            # Look for month patterns
            month_patterns = {
                'janeiro': 1, 'jan': 1,
                'fevereiro': 2, 'fev': 2,
                'março': 3, 'mar': 3,
                'abril': 4, 'abr': 4,
                'maio': 5, 'mai': 5,
                'junho': 6, 'jun': 6,
                'julho': 7, 'jul': 7,
                'agosto': 8, 'ago': 8,
                'setembro': 9, 'set': 9,
                'outubro': 10, 'out': 10,
                'novembro': 11, 'nov': 11,
                'dezembro': 12, 'dez': 12
            }
            
            title_lower = title.lower()
            for month_name, month_num in month_patterns.items():
                if month_name in title_lower:
                    try:
                        return datetime(year, month_num, 1)
                    except ValueError:
                        continue
            
            # If no month found, just return year
            try:
                return datetime(year, 1, 1)
            except ValueError:
                pass
        
        # Look for full date patterns
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


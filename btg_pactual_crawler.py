"""
BTG Pactual Serviços Financeiros S.A. DTVM Crawler
Crawls documents from BTG Pactual investor relations portal
"""

import asyncio
import aiohttp
from typing import List, Dict, Any, Optional
from datetime import datetime
import hashlib
import os
import json
from urllib.parse import urljoin, urlparse
import logging

from .base_crawler import BaseCrawler

logger = logging.getLogger(__name__)


class BTGPactualCrawler(BaseCrawler):
    """Crawler for BTG Pactual Serviços Financeiros S.A. DTVM documents"""
    
    def __init__(self, session: aiohttp.ClientSession, storage_path: str):
        super().__init__(session, storage_path)
        self.base_url = "https://www.btgpactual.com"
        self.investor_relations_url = "https://ri.btgpactual.com"
        self.cvm_documents_url = "https://ri.btgpactual.com/documentos-cvm/"
        self.name = "BTG Pactual DTVM"
        
    async def crawl_documents(self) -> List[Dict[str, Any]]:
        """
        Crawl documents from BTG Pactual investor relations portal
        """
        documents = []
        
        try:
            # Crawl CVM documents
            cvm_docs = await self._crawl_cvm_documents()
            documents.extend(cvm_docs)
            
            # Crawl financial reports
            financial_docs = await self._crawl_financial_reports()
            documents.extend(financial_docs)
            
            # Crawl assemblies and communications
            assembly_docs = await self._crawl_assemblies()
            documents.extend(assembly_docs)
            
        except Exception as e:
            logger.error(f"Error crawling BTG Pactual documents: {e}")
            
        return documents
    
    async def _crawl_cvm_documents(self) -> List[Dict[str, Any]]:
        """Crawl CVM regulatory documents"""
        documents = []
        
        try:
            # Get available years
            years = [2025, 2024, 2023, 2022, 2021, 2020]
            
            for year in years:
                year_docs = await self._crawl_cvm_year(year)
                documents.extend(year_docs)
                
                # Add delay between years
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Error crawling CVM documents: {e}")
            
        return documents
    
    async def _crawl_cvm_year(self, year: int) -> List[Dict[str, Any]]:
        """Crawl CVM documents for a specific year"""
        documents = []
        
        try:
            # Access CVM documents page for the year
            url = f"{self.cvm_documents_url}?year={year}"
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = self._get_soup(html)
                    
                    # Find document containers
                    doc_containers = soup.find_all(['div', 'article'], class_=lambda x: x and any(
                        term in x.lower() for term in ['document', 'arquivo', 'comunicado']
                    ))
                    
                    for container in doc_containers:
                        # Find PDF links
                        pdf_links = container.find_all('a', href=lambda x: x and x.endswith('.pdf'))
                        
                        for link in pdf_links:
                            href = link.get('href')
                            if href:
                                doc_url = urljoin(self.investor_relations_url, href)
                                
                                # Extract document info
                                title = link.get_text(strip=True) or link.get('title', '')
                                
                                # Look for date in container
                                date_elem = container.find(['span', 'div'], class_=lambda x: x and 'date' in x.lower())
                                doc_date = None
                                if date_elem:
                                    doc_date = self._parse_date(date_elem.get_text(strip=True))
                                
                                # Process document
                                doc_info = await self._process_document(doc_url, title, doc_date, year)
                                if doc_info:
                                    documents.append(doc_info)
                                    
        except Exception as e:
            logger.error(f"Error crawling CVM documents for year {year}: {e}")
            
        return documents
    
    async def _crawl_financial_reports(self) -> List[Dict[str, Any]]:
        """Crawl financial reports and quarterly results"""
        documents = []
        
        try:
            financial_url = f"{self.investor_relations_url}/informacoes-financeiras/resultados-trimestrais/"
            
            async with self.session.get(financial_url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = self._get_soup(html)
                    
                    # Find financial document sections
                    doc_sections = soup.find_all(['div', 'section'], class_=lambda x: x and any(
                        term in x.lower() for term in ['resultado', 'financial', 'trimestral']
                    ))
                    
                    for section in doc_sections:
                        links = section.find_all('a', href=lambda x: x and x.endswith('.pdf'))
                        
                        for link in links:
                            href = link.get('href')
                            if href:
                                doc_url = urljoin(self.investor_relations_url, href)
                                title = link.get_text(strip=True) or 'Financial Report'
                                
                                doc_info = await self._process_document(doc_url, title, None, None)
                                if doc_info:
                                    documents.append(doc_info)
                                    
        except Exception as e:
            logger.error(f"Error crawling financial reports: {e}")
            
        return documents
    
    async def _crawl_assemblies(self) -> List[Dict[str, Any]]:
        """Crawl assembly documents and shareholder communications"""
        documents = []
        
        try:
            # Try to find assemblies section
            assemblies_url = f"{self.investor_relations_url}/comunicados-eventos-e-replays/"
            
            async with self.session.get(assemblies_url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = self._get_soup(html)
                    
                    # Find assembly and communication documents
                    doc_containers = soup.find_all(['div', 'article'], class_=lambda x: x and any(
                        term in x.lower() for term in ['assembleia', 'comunicado', 'evento']
                    ))
                    
                    for container in doc_containers:
                        links = container.find_all('a', href=lambda x: x and x.endswith('.pdf'))
                        
                        for link in links:
                            href = link.get('href')
                            if href:
                                doc_url = urljoin(self.investor_relations_url, href)
                                title = link.get_text(strip=True) or 'Assembly Document'
                                
                                doc_info = await self._process_document(doc_url, title, None, None)
                                if doc_info:
                                    documents.append(doc_info)
                                    
        except Exception as e:
            logger.error(f"Error crawling assembly documents: {e}")
            
        return documents
    
    async def _process_document(self, url: str, title: str, doc_date: Optional[datetime], year: Optional[int]) -> Optional[Dict[str, Any]]:
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
                    filename = f"btg_pactual_{url_hash}.pdf"
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
                        'company_name': 'BTG Pactual',
                        'emission_date': doc_date or self._extract_date_from_title(title),
                        'year': year,
                        'downloaded_at': datetime.utcnow(),
                        'source': 'BTG Pactual DTVM'
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
        elif any(word in title_lower for word in ['comunicado ao mercado', 'market communication']):
            return 'market_communication'
        elif any(word in title_lower for word in ['aquisição', 'acquisition']):
            return 'acquisition'
        elif any(word in title_lower for word in ['resultado', 'financial', 'trimestral']):
            return 'financial_report'
        elif any(word in title_lower for word in ['demonstrações', 'statement']):
            return 'financial_statement'
        elif any(word in title_lower for word in ['aviso', 'notice']):
            return 'shareholder_notice'
        else:
            return 'other'
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date from string"""
        import re
        
        # Common date formats
        date_patterns = [
            (r'(\d{2})/(\d{2})/(\d{4})', '%d/%m/%Y'),
            (r'(\d{2})-(\d{2})-(\d{4})', '%d-%m-%Y'),
            (r'(\d{4})-(\d{2})-(\d{2})', '%Y-%m-%d'),
            (r'(\d{2})\.(\d{2})\.(\d{4})', '%d.%m.%Y'),
        ]
        
        for pattern, format_str in date_patterns:
            match = re.search(pattern, date_str)
            if match:
                try:
                    return datetime.strptime(match.group(), format_str.replace('(', '').replace(')', ''))
                except ValueError:
                    continue
        
        return None
    
    def _extract_date_from_title(self, title: str) -> Optional[datetime]:
        """Extract date from document title"""
        return self._parse_date(title)


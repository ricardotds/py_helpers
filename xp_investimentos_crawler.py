"""
XP Investimentos CCTVM S.A. Crawler
Crawls documents from XP Inc investor relations portal
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


class XPInvestimentosCrawler(BaseCrawler):
    """Crawler for XP Investimentos CCTVM S.A. documents"""
    
    def __init__(self, session: aiohttp.ClientSession, storage_path: str):
        super().__init__(session, storage_path)
        self.base_url = "https://www.xpi.com.br"
        self.investor_relations_url = "https://investors.xpinc.com"
        self.cvm_files_url = "https://investors.xpinc.com/informacoes-financeiras/arquivos-cvm/"
        self.sec_docs_url = "https://investors.xpinc.com/informacoes-financeiras/documentos-sec/"
        self.name = "XP Investimentos CCTVM"
        
    async def crawl_documents(self) -> List[Dict[str, Any]]:
        """
        Crawl documents from XP Inc investor relations portal
        """
        documents = []
        
        try:
            # Crawl CVM files
            cvm_docs = await self._crawl_cvm_files()
            documents.extend(cvm_docs)
            
            # Crawl SEC documents
            sec_docs = await self._crawl_sec_documents()
            documents.extend(sec_docs)
            
            # Crawl quarterly results
            quarterly_docs = await self._crawl_quarterly_results()
            documents.extend(quarterly_docs)
            
            # Crawl annual reports
            annual_docs = await self._crawl_annual_reports()
            documents.extend(annual_docs)
            
        except Exception as e:
            logger.error(f"Error crawling XP Investimentos documents: {e}")
            
        return documents
    
    async def _crawl_cvm_files(self) -> List[Dict[str, Any]]:
        """Crawl CVM regulatory files"""
        documents = []
        
        try:
            # Get available years
            years = [2025, 2024, 2023, 2022, 2021, 2020, 2019]
            
            for year in years:
                year_docs = await self._crawl_cvm_year(year)
                documents.extend(year_docs)
                
                # Add delay between years
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Error crawling CVM files: {e}")
            
        return documents
    
    async def _crawl_cvm_year(self, year: int) -> List[Dict[str, Any]]:
        """Crawl CVM files for a specific year"""
        documents = []
        
        try:
            # Access CVM files page for the year
            url = f"{self.cvm_files_url}?year={year}"
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = self._get_soup(html)
                    
                    # Find document containers
                    doc_containers = soup.find_all(['div', 'article', 'tr'], class_=lambda x: x and any(
                        term in x.lower() for term in ['document', 'arquivo', 'file', 'row']
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
                                date_elem = container.find(['span', 'div', 'td'], class_=lambda x: x and 'date' in x.lower())
                                doc_date = None
                                if date_elem:
                                    doc_date = self._parse_date(date_elem.get_text(strip=True))
                                
                                # Process document
                                doc_info = await self._process_document(doc_url, title, doc_date, year, 'CVM')
                                if doc_info:
                                    documents.append(doc_info)
                                    
        except Exception as e:
            logger.error(f"Error crawling CVM files for year {year}: {e}")
            
        return documents
    
    async def _crawl_sec_documents(self) -> List[Dict[str, Any]]:
        """Crawl SEC regulatory documents"""
        documents = []
        
        try:
            async with self.session.get(self.sec_docs_url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = self._get_soup(html)
                    
                    # Find SEC document containers
                    doc_containers = soup.find_all(['div', 'article', 'tr'], class_=lambda x: x and any(
                        term in x.lower() for term in ['document', 'sec', 'file', 'row']
                    ))
                    
                    for container in doc_containers:
                        pdf_links = container.find_all('a', href=lambda x: x and x.endswith('.pdf'))
                        
                        for link in pdf_links:
                            href = link.get('href')
                            if href:
                                doc_url = urljoin(self.investor_relations_url, href)
                                title = link.get_text(strip=True) or 'SEC Document'
                                
                                # Look for date
                                date_elem = container.find(['span', 'div', 'td'], class_=lambda x: x and 'date' in x.lower())
                                doc_date = None
                                if date_elem:
                                    doc_date = self._parse_date(date_elem.get_text(strip=True))
                                
                                doc_info = await self._process_document(doc_url, title, doc_date, None, 'SEC')
                                if doc_info:
                                    documents.append(doc_info)
                                    
        except Exception as e:
            logger.error(f"Error crawling SEC documents: {e}")
            
        return documents
    
    async def _crawl_quarterly_results(self) -> List[Dict[str, Any]]:
        """Crawl quarterly financial results"""
        documents = []
        
        try:
            quarterly_url = f"{self.investor_relations_url}/informacoes-financeiras/resultados-trimestrais/"
            
            async with self.session.get(quarterly_url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = self._get_soup(html)
                    
                    # Find quarterly result sections
                    doc_sections = soup.find_all(['div', 'section'], class_=lambda x: x and any(
                        term in x.lower() for term in ['resultado', 'quarterly', 'trimestral']
                    ))
                    
                    for section in doc_sections:
                        links = section.find_all('a', href=lambda x: x and x.endswith('.pdf'))
                        
                        for link in links:
                            href = link.get('href')
                            if href:
                                doc_url = urljoin(self.investor_relations_url, href)
                                title = link.get_text(strip=True) or 'Quarterly Report'
                                
                                doc_info = await self._process_document(doc_url, title, None, None, 'Quarterly')
                                if doc_info:
                                    documents.append(doc_info)
                                    
        except Exception as e:
            logger.error(f"Error crawling quarterly results: {e}")
            
        return documents
    
    async def _crawl_annual_reports(self) -> List[Dict[str, Any]]:
        """Crawl annual reports"""
        documents = []
        
        try:
            annual_url = f"{self.investor_relations_url}/informacoes-financeiras/relatorios-anuais/"
            
            async with self.session.get(annual_url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = self._get_soup(html)
                    
                    # Find annual report sections
                    doc_sections = soup.find_all(['div', 'section'], class_=lambda x: x and any(
                        term in x.lower() for term in ['annual', 'anual', 'relatório']
                    ))
                    
                    for section in doc_sections:
                        links = section.find_all('a', href=lambda x: x and x.endswith('.pdf'))
                        
                        for link in links:
                            href = link.get('href')
                            if href:
                                doc_url = urljoin(self.investor_relations_url, href)
                                title = link.get_text(strip=True) or 'Annual Report'
                                
                                doc_info = await self._process_document(doc_url, title, None, None, 'Annual')
                                if doc_info:
                                    documents.append(doc_info)
                                    
        except Exception as e:
            logger.error(f"Error crawling annual reports: {e}")
            
        return documents
    
    async def _process_document(self, url: str, title: str, doc_date: Optional[datetime], 
                              year: Optional[int], category: str) -> Optional[Dict[str, Any]]:
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
                    filename = f"xp_investimentos_{category.lower()}_{url_hash}.pdf"
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
                        'document_type': self._classify_document_type(title, category),
                        'company_name': 'XP Inc.',
                        'category': category,
                        'emission_date': doc_date or self._extract_date_from_title(title),
                        'year': year,
                        'downloaded_at': datetime.utcnow(),
                        'source': 'XP Investimentos CCTVM'
                    }
                    
        except Exception as e:
            logger.error(f"Error processing document {url}: {e}")
            
        return None
    
    def _classify_document_type(self, title: str, category: str) -> str:
        """Classify document type based on title and category"""
        title_lower = title.lower()
        
        if category == 'CVM':
            if any(word in title_lower for word in ['cancelamento', 'cancellation']):
                return 'registration_cancellation'
            elif any(word in title_lower for word in ['registro', 'registration']):
                return 'registration'
            elif any(word in title_lower for word in ['comunicado', 'communication']):
                return 'market_communication'
            else:
                return 'cvm_filing'
        elif category == 'SEC':
            if '20-f' in title_lower:
                return 'form_20f'
            elif '6-k' in title_lower:
                return 'form_6k'
            elif 'f-1' in title_lower:
                return 'form_f1'
            else:
                return 'sec_filing'
        elif category == 'Quarterly':
            return 'quarterly_report'
        elif category == 'Annual':
            return 'annual_report'
        
        # General classification
        if any(word in title_lower for word in ['debênture', 'debenture']):
            return 'debenture_emission'
        elif any(word in title_lower for word in ['assembleia', 'assembly']):
            return 'assembly'
        elif any(word in title_lower for word in ['resultado', 'earnings']):
            return 'earnings_report'
        elif any(word in title_lower for word in ['demonstrações', 'financial statements']):
            return 'financial_statement'
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


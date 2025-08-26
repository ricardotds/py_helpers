"""
Enhanced Crawler Service
Manages and executes web crawlers with real-time notifications and advanced features
"""

import asyncio
import aiohttp
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
import os

from sqlalchemy.orm import Session
from app.models.fiduciary_agent import FiduciaryAgent
from app.models.crawler import CrawlerRun
from app.models.document import Document
from app.crawlers.pentagono_crawler import PentagonoCrawler
from app.crawlers.vortx_crawler import VortxCrawler
from app.crawlers.oliveira_trust_crawler import OliveiraTrustCrawler
from app.crawlers.planner_trustee_crawler import PlannerTrusteeCrawler
from app.crawlers.btg_pactual_crawler import BTGPactualCrawler
from app.crawlers.brl_trust_crawler import BRLTrustCrawler
from app.crawlers.xp_investimentos_crawler import XPInvestimentosCrawler
from app.services.document_service import DocumentService
from app.services.notification_service import notification_service
from app.services.websocket_service import websocket_service
from app.config.settings import settings

logger = logging.getLogger(__name__)


class EnhancedCrawlerService:
    """Enhanced service for managing crawler operations with real-time features"""
    
    def __init__(self, db: Session):
        self.db = db
        self.storage_path = getattr(settings, 'STORAGE_PATH', '/app/storage')
        self.documents_path = os.path.join(self.storage_path, 'documents')
        
        # Ensure storage directory exists
        os.makedirs(self.documents_path, exist_ok=True)
        
        # Initialize document service
        self.document_service = DocumentService(db)
        
        # Enhanced crawler class mapping with all implemented crawlers
        self.crawler_classes = {
            'pentagono': PentagonoCrawler,
            'vortx': VortxCrawler,
            'oliveira_trust': OliveiraTrustCrawler,
            'planner_trustee': PlannerTrusteeCrawler,
            'btg_pactual': BTGPactualCrawler,
            'brl_trust': BRLTrustCrawler,
            'xp_investimentos': XPInvestimentosCrawler,
        }
        
        # Active crawler tasks with enhanced tracking
        self.active_crawlers: Dict[int, Dict[str, Any]] = {}
    
    async def start_crawler(self, agent_id: int, user_emails: Optional[List[str]] = None,
                          options: Optional[Dict[str, Any]] = None) -> Optional[CrawlerRun]:
        """Start a crawler for a specific fiduciary agent with enhanced options"""
        try:
            # Get fiduciary agent
            agent = self.db.query(FiduciaryAgent).filter(FiduciaryAgent.id == agent_id).first()
            if not agent:
                logger.error(f"Fiduciary agent not found: {agent_id}")
                return None
            
            # Check if crawler is already running
            if agent_id in self.active_crawlers:
                logger.warning(f"Crawler already running for agent {agent_id}")
                return None
            
            # Parse options
            options = options or {}
            force_full_crawl = options.get('force_full_crawl', False)
            max_pages = options.get('max_pages', None)
            date_filter = options.get('date_filter', None)
            
            # Create crawler run record
            crawler_run = CrawlerRun(
                fiduciary_agent_id=agent_id,
                status="running",
                started_at=datetime.utcnow(),
                metadata_={
                    'options': options,
                    'user_emails': user_emails
                }
            )
            self.db.add(crawler_run)
            self.db.commit()
            self.db.refresh(crawler_run)
            
            # Send start notification
            await notification_service.notify_crawler_started(
                agent_name=agent.name,
                crawler_run_id=crawler_run.id,
                user_emails=user_emails
            )
            
            # Start crawler task with enhanced tracking
            task = asyncio.create_task(
                self._run_enhanced_crawler(agent, crawler_run, user_emails, options)
            )
            
            self.active_crawlers[agent_id] = {
                'task': task,
                'crawler_run': crawler_run,
                'start_time': datetime.utcnow(),
                'progress': {'phase': 'initializing', 'percentage': 0}
            }
            
            logger.info(f"Enhanced crawler started for agent {agent.name} (ID: {agent_id})")
            return crawler_run
            
        except Exception as e:
            logger.error(f"Error starting enhanced crawler for agent {agent_id}: {e}")
            return None
    
    async def _run_enhanced_crawler(self, agent: FiduciaryAgent, crawler_run: CrawlerRun, 
                                  user_emails: Optional[List[str]] = None,
                                  options: Optional[Dict[str, Any]] = None):
        """Execute the enhanced crawler with real-time progress tracking"""
        start_time = datetime.utcnow()
        documents_found = 0
        documents_downloaded = 0
        pages_processed = 0
        errors = []
        
        try:
            # Get crawler class
            crawler_class = self.crawler_classes.get(agent.crawler_class)
            if not crawler_class:
                raise ValueError(f"Unknown crawler class: {agent.crawler_class}")
            
            # Update progress
            await self._update_crawler_progress(
                agent.id, crawler_run.id, agent.name,
                "initializing", 5, "Setting up crawler..."
            )
            
            # Create HTTP session with enhanced configuration
            timeout = aiohttp.ClientTimeout(total=60, connect=30)
            connector = aiohttp.TCPConnector(limit=10, limit_per_host=3)
            
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                # Initialize crawler
                crawler = crawler_class(session, self.documents_path)
                
                # Update progress
                await self._update_crawler_progress(
                    agent.id, crawler_run.id, agent.name,
                    "crawling", 10, "Starting document discovery..."
                )
                
                # Crawl documents with progress tracking
                documents = await self._crawl_with_progress_tracking(
                    crawler, agent, crawler_run, options
                )
                documents_found = len(documents)
                
                # Update progress
                await self._update_crawler_progress(
                    agent.id, crawler_run.id, agent.name,
                    "processing", 50, f"Processing {documents_found} documents..."
                )
                
                # Process each document with enhanced error handling
                for i, doc_data in enumerate(documents):
                    try:
                        # Add fiduciary agent ID to metadata
                        doc_data['fiduciary_agent_id'] = agent.id
                        doc_data['crawler_run_id'] = crawler_run.id
                        
                        # Process document with enhanced service
                        document = await self.document_service.process_document(
                            doc_data['file_path'], doc_data
                        )
                        
                        if document:
                            documents_downloaded += 1
                            
                            # Send real-time document update
                            await websocket_service.broadcast_document_update(
                                document_id=document.id,
                                action="created",
                                document_info={
                                    "title": document.title,
                                    "agent_name": agent.name,
                                    "document_type": document.document_type,
                                    "company_name": document.company_name,
                                    "file_size": document.file_size
                                }
                            )
                        
                        # Update progress
                        progress = 50 + (i + 1) / documents_found * 40  # 50-90%
                        await self._update_crawler_progress(
                            agent.id, crawler_run.id, agent.name,
                            "processing", progress,
                            f"Processed {i + 1}/{documents_found} documents"
                        )
                        
                        # Small delay to prevent overwhelming
                        await asyncio.sleep(0.1)
                        
                    except Exception as e:
                        error_msg = f"Error processing document {i+1}: {str(e)}"
                        errors.append(error_msg)
                        logger.error(error_msg)
                        continue
            
            # Final processing
            await self._update_crawler_progress(
                agent.id, crawler_run.id, agent.name,
                "finalizing", 95, "Finalizing crawler run..."
            )
            
            # Update crawler run record with enhanced data
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            crawler_run.status = "completed"
            crawler_run.completed_at = end_time
            crawler_run.documents_found = documents_found
            crawler_run.documents_downloaded = documents_downloaded
            crawler_run.duration = duration
            crawler_run.pages_processed = pages_processed
            crawler_run.error_count = len(errors)
            
            if errors:
                crawler_run.error_message = "; ".join(errors[:5])  # Store first 5 errors
            
            # Enhanced metadata
            crawler_run.metadata_ = {
                **(crawler_run.metadata_ or {}),
                'performance': {
                    'documents_per_second': documents_found / duration if duration > 0 else 0,
                    'success_rate': (documents_downloaded / documents_found * 100) if documents_found > 0 else 0,
                    'errors': errors
                }
            }
            
            self.db.commit()
            
            # Send completion notification with enhanced data
            await notification_service.notify_crawler_completed(
                agent_name=agent.name,
                crawler_run_id=crawler_run.id,
                documents_found=documents_found,
                documents_downloaded=documents_downloaded,
                duration=duration,
                user_emails=user_emails
            )
            
            # Final progress update
            await self._update_crawler_progress(
                agent.id, crawler_run.id, agent.name,
                "completed", 100, f"Completed: {documents_downloaded}/{documents_found} documents"
            )
            
            logger.info(f"Enhanced crawler completed for {agent.name}: {documents_downloaded}/{documents_found} documents")
            
        except Exception as e:
            # Enhanced error handling
            error_msg = str(e)
            errors.append(error_msg)
            
            # Update crawler run record with detailed error
            crawler_run.status = "failed"
            crawler_run.completed_at = datetime.utcnow()
            crawler_run.error_message = error_msg
            crawler_run.documents_found = documents_found
            crawler_run.documents_downloaded = documents_downloaded
            crawler_run.error_count = len(errors)
            
            # Enhanced error metadata
            crawler_run.metadata_ = {
                **(crawler_run.metadata_ or {}),
                'error_details': {
                    'main_error': error_msg,
                    'all_errors': errors,
                    'failure_point': 'crawler_execution'
                }
            }
            
            self.db.commit()
            
            # Send failure notification
            await notification_service.notify_crawler_failed(
                agent_name=agent.name,
                crawler_run_id=crawler_run.id,
                error_message=error_msg,
                user_emails=user_emails
            )
            
            # Error progress update
            await self._update_crawler_progress(
                agent.id, crawler_run.id, agent.name,
                "failed", 0, f"Failed: {error_msg}"
            )
            
            logger.error(f"Enhanced crawler failed for {agent.name}: {e}")
            
        finally:
            # Remove from active crawlers
            if agent.id in self.active_crawlers:
                del self.active_crawlers[agent.id]
    
    async def _crawl_with_progress_tracking(self, crawler, agent, crawler_run, options):
        """Crawl documents with enhanced progress tracking"""
        # This would be implemented based on the specific crawler's capabilities
        # For now, we'll use the basic crawl_documents method
        return await crawler.crawl_documents()
    
    async def _update_crawler_progress(self, agent_id: int, crawler_run_id: int, 
                                     agent_name: str, phase: str, percentage: float, 
                                     message: str):
        """Update crawler progress and broadcast to WebSocket clients"""
        progress = {
            'phase': phase,
            'percentage': percentage,
            'message': message,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Update local tracking
        if agent_id in self.active_crawlers:
            self.active_crawlers[agent_id]['progress'] = progress
        
        # Broadcast to WebSocket clients
        await websocket_service.broadcast_crawler_status(
            crawler_run_id=crawler_run_id,
            status=phase,
            agent_name=agent_name,
            progress=progress
        )
    
    def get_enhanced_crawler_status(self, agent_id: int) -> Dict[str, Any]:
        """Get enhanced status of a crawler including real-time progress"""
        # Get basic status
        basic_status = self.get_crawler_status(agent_id)
        
        # Add real-time information
        if agent_id in self.active_crawlers:
            active_info = self.active_crawlers[agent_id]
            basic_status.update({
                'is_running': True,
                'current_progress': active_info['progress'],
                'runtime': (datetime.utcnow() - active_info['start_time']).total_seconds()
            })
        else:
            basic_status['is_running'] = False
        
        return basic_status
    
    def get_crawler_status(self, agent_id: int) -> Dict[str, Any]:
        """Get current status of a crawler (legacy method for compatibility)"""
        # Check if crawler is running
        is_running = agent_id in self.active_crawlers
        
        # Get latest crawler run
        latest_run = self.db.query(CrawlerRun).filter(
            CrawlerRun.fiduciary_agent_id == agent_id
        ).order_by(CrawlerRun.started_at.desc()).first()
        
        status = {
            "is_running": is_running,
            "latest_run": None
        }
        
        if latest_run:
            status["latest_run"] = {
                "id": latest_run.id,
                "status": latest_run.status,
                "started_at": latest_run.started_at,
                "completed_at": latest_run.completed_at,
                "documents_found": latest_run.documents_found,
                "documents_downloaded": latest_run.documents_downloaded,
                "duration": latest_run.duration,
                "error_message": latest_run.error_message,
                "pages_processed": getattr(latest_run, 'pages_processed', None),
                "error_count": getattr(latest_run, 'error_count', None)
            }
        
        return status
    
    async def start_all_crawlers(self, user_emails: Optional[List[str]] = None,
                               options: Optional[Dict[str, Any]] = None) -> List[CrawlerRun]:
        """Start crawlers for all active fiduciary agents with enhanced coordination"""
        agents = self.db.query(FiduciaryAgent).filter(FiduciaryAgent.is_active == True).all()
        crawler_runs = []
        
        # Send system notification
        await notification_service.notify_system_alert(
            alert_type="batch_crawler_start",
            message=f"Starting crawlers for {len(agents)} fiduciary agents",
            severity="info",
            user_emails=user_emails
        )
        
        for i, agent in enumerate(agents):
            try:
                crawler_run = await self.start_crawler(agent.id, user_emails, options)
                if crawler_run:
                    crawler_runs.append(crawler_run)
                
                # Staggered start to prevent overwhelming servers
                if i < len(agents) - 1:  # Don't delay after the last one
                    await asyncio.sleep(5)
                    
            except Exception as e:
                logger.error(f"Failed to start crawler for agent {agent.name}: {e}")
                continue
        
        return crawler_runs
    
    def get_system_statistics(self) -> Dict[str, Any]:
        """Get comprehensive system statistics"""
        # Basic document statistics
        doc_stats = self.document_service.get_document_statistics()
        
        # Crawler run statistics
        total_runs = self.db.query(CrawlerRun).count()
        successful_runs = self.db.query(CrawlerRun).filter(CrawlerRun.status == "completed").count()
        failed_runs = self.db.query(CrawlerRun).filter(CrawlerRun.status == "failed").count()
        
        # Active crawler information
        active_count = len(self.active_crawlers)
        
        # Recent activity (last 24 hours)
        recent_cutoff = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        recent_runs = self.db.query(CrawlerRun).filter(
            CrawlerRun.started_at >= recent_cutoff
        ).count()
        
        recent_documents = self.db.query(Document).filter(
            Document.downloaded_at >= recent_cutoff
        ).count()
        
        return {
            'documents': doc_stats,
            'crawlers': {
                'total_runs': total_runs,
                'successful_runs': successful_runs,
                'failed_runs': failed_runs,
                'success_rate': (successful_runs / total_runs * 100) if total_runs > 0 else 0,
                'active_crawlers': active_count,
                'recent_runs_today': recent_runs,
                'recent_documents_today': recent_documents
            },
            'system': {
                'active_agents': self.db.query(FiduciaryAgent).filter(FiduciaryAgent.is_active == True).count(),
                'total_agents': self.db.query(FiduciaryAgent).count(),
                'websocket_connections': websocket_service.get_connection_stats()
            }
        }


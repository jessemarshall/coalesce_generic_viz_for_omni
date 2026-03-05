"""
Omni to Coalesce Catalog Sync Package

A Python package for syncing Omni dashboards and models to Coalesce Catalog.
"""

# Suppress the urllib3 OpenSSL warning before any imports
import warnings
warnings.filterwarnings('ignore', message='urllib3 v2 only supports OpenSSL')

__version__ = "3.0.0"
__author__ = "Coalesce"
__description__ = "Sync Omni dashboards and models to Coalesce Catalog"

from .extractor import OmniExtractor
from .transformer import OmniToBIImporter
from .uploader import BIImporterUploader
from .orchestrator import WorkflowOrchestrator

__all__ = [
    'OmniExtractor',
    'OmniToBIImporter',
    'BIImporterUploader',
    'WorkflowOrchestrator'
]
"""
Workflow Orchestrator for Omni-Coalesce Sync

Handles the complete sync workflow using direct method calls instead of subprocess.
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime

import time

from .extractor import OmniExtractor
from .transformer import OmniToBIImporter
from .uploader import BIImporterUploader
from .slack_notifier import send_slack_notification

logger = logging.getLogger(__name__)


class WorkflowOrchestrator:
    """Orchestrates the Omni to Coalesce sync workflow"""

    def __init__(self,
                 work_dir: Optional[Path] = None,
                 data_dir: Optional[Path] = None,
                 env_file: Optional[str] = None):
        """
        Initialize the orchestrator

        Args:
            work_dir: Working directory (defaults to current directory)
            data_dir: Data directory for extracted and generated files
            env_file: Path to environment file (optional, uses env vars if not provided)
        """
        self.work_dir = Path(work_dir) if work_dir else Path.cwd()
        self.data_dir = Path(data_dir) if data_dir else self.work_dir / "local_run_data"
        self.extracted_data_dir = self.data_dir / "extracted_data"
        self.bi_importer_dir = self.data_dir / "bi_importer"

        # Components (initialized when needed)
        self.extractor = None
        self.transformer = None
        self.uploader = None

        # Environment variables
        self.env_vars = {}
        if env_file:
            self.load_environment(env_file)
        else:
            # Use existing environment variables
            self.env_vars = dict(os.environ)

    def load_environment(self, env_file: str):
        """Load environment variables from .env file"""
        env_path = Path(env_file)
        if not env_path.exists():
            logger.error(f"Environment file {env_file} not found!")
            raise FileNotFoundError(f"Environment file {env_file} not found")

        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        # Remove inline comments
                        if ' #' in value and not value.startswith(("'", '"')):
                            value = value.split(' #')[0].strip()
                        # Remove surrounding quotes
                        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                            value = value[1:-1]
                        self.env_vars[key] = value
                        # Also set in os.environ for compatibility
                        os.environ[key] = value

        logger.info(f"Loaded {len(self.env_vars)} environment variables from {env_file}")

    def setup_directories(self):
        """Create necessary directories"""
        directories = [
            self.data_dir,
            self.extracted_data_dir,
            self.bi_importer_dir
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

        logger.info(f"Created working directories in {self.data_dir}")

    def extract_omni_metadata(self, mode: str = 'full') -> Dict[str, List]:
        """
        Extract metadata from Omni API

        Args:
            mode: Extraction mode (only 'full' is supported)

        Returns:
            Dictionary containing extracted data
        """
        logger.info("📥 Extracting Omni metadata...")

        # Get configuration from environment
        base_url = self.env_vars.get('OMNI_BASE_URL')
        api_token = self.env_vars.get('OMNI_API_TOKEN')

        if not base_url or not api_token:
            raise ValueError("OMNI_BASE_URL and OMNI_API_TOKEN must be set")

        # Initialize extractor
        self.extractor = OmniExtractor(
            base_url=base_url,
            api_token=api_token,
            output_dir=self.extracted_data_dir
        )

        # Test connection first
        if not self.extractor.test_connection():
            raise ConnectionError("Failed to connect to Omni API")

        # Extract data
        extracted_data = self.extractor.extract(mode=mode)

        # Save to files
        saved_files = self.extractor.save_to_files(extracted_data)

        logger.info(f"✅ Extracted {len(extracted_data['models'])} models, "
                   f"{len(extracted_data['dashboards'])} dashboards, "
                   f"{len(extracted_data['connections'])} connections")

        return extracted_data

    def generate_bi_importer_csv(self) -> Dict[str, Any]:
        """
        Generate BI Importer CSV files from extracted data

        Returns:
            Dictionary with generation results
        """
        logger.info("📝 Generating BI Importer CSV files...")

        # Initialize transformer
        base_url = self.env_vars.get('OMNI_BASE_URL', '')
        self.transformer = OmniToBIImporter(
            extracted_data_dir=self.extracted_data_dir,
            output_dir=self.bi_importer_dir,
            base_url=base_url
        )

        # Generate CSV files
        result = self.transformer.convert()

        if result and result.get('files_created'):
            logger.info(f"✅ Generated {len(result['files_created'])} CSV files")
            for file_path in result['files_created']:
                logger.info(f"  - {Path(file_path).name}")

            # Log statistics
            if 'statistics' in result:
                stats = result['statistics']
                logger.info(f"📊 Statistics:")
                logger.info(f"  - Dashboards: {stats.get('dashboards', 0)}")
                logger.info(f"  - Models: {stats.get('models', 0)}")
                logger.info(f"  - Queries: {stats.get('queries', 0)}")
                logger.info(f"  - Fields: {stats.get('fields', 0)}")
        else:
            logger.error(f"Failed to generate CSV files")

        return result

    def upload_to_coalesce(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Upload BI Importer CSV files to Coalesce Catalog

        Args:
            dry_run: If True, only simulate the upload

        Returns:
            Dictionary with upload results
        """
        logger.info("📤 Uploading to Coalesce Catalog...")

        # Get configuration
        api_token = self.env_vars.get('COALESCE_API_TOKEN')
        source_id = self.env_vars.get('COALESCE_SOURCE_ID')
        zone = self.env_vars.get('COALESCE_ZONE', 'US')

        logger.info(f"Zone: {zone}")
        logger.info(f"Source ID: {source_id}")
        logger.info(f"BI Importer Directory: {self.bi_importer_dir}")

        if not api_token or not source_id:
            raise ValueError("COALESCE_API_TOKEN and COALESCE_SOURCE_ID must be set")

        # Initialize uploader
        self.uploader = BIImporterUploader(
            api_token=api_token,
            source_id=source_id,
            zone=zone,
            bi_importer_dir=self.bi_importer_dir
        )

        # Check if castor-upload is available
        if not self.uploader.is_available():
            logger.error("castor-upload not available. Please install castor-extractor package.")
            logger.info("Install with: pip install castor-extractor")
            return {
                'success': False,
                'error': 'castor-upload not available'
            }

        # Upload files
        result = self.uploader.upload(dry_run=dry_run)

        if result['success']:
            logger.info("")
            logger.info(f"✅ Upload completed successfully!")
            logger.info(f"✅ Successfully uploaded {len(result['files_uploaded'])} files")
            logger.info("Check your Coalesce Catalog Dashboards section to verify the import.")
        elif result['files_uploaded']:
            logger.warning("")
            logger.warning(f"⚠️ Partial success: {len(result['files_uploaded'])} uploaded, "
                         f"{len(result['files_failed'])} failed")
            logger.info("Check your Coalesce Catalog Dashboards section for the uploaded files.")
        else:
            logger.error("")
            logger.error("❌ Upload failed")
            logger.error("❌ No files uploaded successfully")
            logger.error("Please check the error messages above and try again.")

        return result

    def sync_dashboard_tags(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Sync Omni dashboard labels as tags in Coalesce Catalog.

        Reads labels from extracted dashboards.json and attaches them
        to dashboard entities via the Catalog GraphQL API.

        Args:
            dry_run: If True, only simulate the sync

        Returns:
            Dictionary with sync results
        """
        logger.info("🏷️ Syncing dashboard tags to Coalesce Catalog...")

        # Get configuration
        api_token = self.env_vars.get('COALESCE_API_TOKEN')
        source_id = self.env_vars.get('COALESCE_SOURCE_ID')
        zone = self.env_vars.get('COALESCE_ZONE', 'US')

        if not api_token or not source_id:
            raise ValueError("COALESCE_API_TOKEN and COALESCE_SOURCE_ID must be set")

        # Initialize uploader if not already done
        if not self.uploader:
            self.uploader = BIImporterUploader(
                api_token=api_token,
                source_id=source_id,
                zone=zone,
                bi_importer_dir=self.bi_importer_dir
            )

        # Load extracted dashboards to get label data
        dashboards_file = self.extracted_data_dir / 'dashboards.json'
        if not dashboards_file.exists():
            logger.warning("No dashboards.json found - run extract step first")
            return {'success': False, 'error': 'dashboards.json not found'}

        with open(dashboards_file, 'r') as f:
            dashboards = json.load(f)

        # Fetch Coalesce Catalog dashboards to get name → UUID mapping
        logger.info("Fetching dashboard UUIDs from Coalesce Catalog...")
        name_to_uuid = self.uploader.get_catalog_dashboards()

        if not name_to_uuid:
            logger.warning("No dashboards found in Coalesce Catalog - run upload step first")
            return {'success': False, 'error': 'No dashboards found in Coalesce Catalog'}

        logger.info(f"Found {len(name_to_uuid)} dashboards in Coalesce Catalog")

        # Build tag data from dashboard labels, mapping names to Coalesce UUIDs
        tag_data = []
        skipped = 0
        for dashboard in dashboards:
            dashboard_name = dashboard.get('name', '')
            labels = dashboard.get('labels', [])
            if not labels or not dashboard_name:
                continue

            # Look up the Coalesce UUID by dashboard name
            coalesce_uuid = name_to_uuid.get(dashboard_name)
            if not coalesce_uuid:
                logger.debug(f"Dashboard '{dashboard_name}' not found in Coalesce Catalog, skipping tags")
                skipped += 1
                continue

            for label in labels:
                label_name = label.get('name') if isinstance(label, dict) else str(label)
                if label_name:
                    tag_data.append({
                        'label': label_name,
                        'entityId': coalesce_uuid,
                        'entityType': 'DASHBOARD'
                    })

        if skipped:
            logger.info(f"Skipped {skipped} dashboards not found in Coalesce Catalog")

        if not tag_data:
            logger.info("No dashboard labels found to sync")
            return {'success': True, 'tags_synced': 0,
                    'catalog_dashboards': len(name_to_uuid), 'skipped': skipped}

        logger.info(f"Found {len(tag_data)} tags across {len(set(t['entityId'] for t in tag_data))} dashboards")

        result = self.uploader.sync_tags(tag_data, dry_run=dry_run)
        result['catalog_dashboards'] = len(name_to_uuid)
        result['skipped'] = skipped

        if result['success']:
            logger.info(f"✅ Synced {result['tags_synced']} tags")
        else:
            logger.warning(f"Tag sync completed with errors: {result.get('error')}")

        return result

    def validate_connections(self) -> bool:
        """
        Validate connections to Omni and Coalesce APIs

        Returns:
            True if all connections are valid
        """
        logger.info("🔍 Validating connections...")

        all_valid = True

        # Test Omni connection
        try:
            logger.info("Testing Omni API connection...")
            base_url = self.env_vars.get('OMNI_BASE_URL')
            api_token = self.env_vars.get('OMNI_API_TOKEN')

            if not base_url or not api_token:
                logger.error("❌ Omni credentials not configured")
                all_valid = False
            else:
                extractor = OmniExtractor(base_url, api_token)
                if extractor.test_connection():
                    logger.info("✅ Omni API connection successful")
                else:
                    logger.error("❌ Omni API connection failed")
                    all_valid = False
        except Exception as e:
            logger.error(f"❌ Omni API validation failed: {e}")
            all_valid = False

        # Test Coalesce connection
        try:
            logger.info("Testing Coalesce Catalog connection...")
            api_token = self.env_vars.get('COALESCE_API_TOKEN')
            source_id = self.env_vars.get('COALESCE_SOURCE_ID')
            zone = self.env_vars.get('COALESCE_ZONE', 'US')

            if not api_token or not source_id:
                logger.error("❌ Coalesce credentials not configured")
                all_valid = False
            else:
                # Create dummy directory for test
                self.bi_importer_dir.mkdir(parents=True, exist_ok=True)

                uploader = BIImporterUploader(
                    api_token=api_token,
                    source_id=source_id,
                    zone=zone,
                    bi_importer_dir=self.bi_importer_dir
                )

                if uploader.test_connection():
                    logger.info("✅ Coalesce Catalog connection successful")
                else:
                    logger.error("❌ Coalesce Catalog connection failed")
                    logger.info("Note: Ensure castor-extractor is installed: pip install castor-extractor")
                    all_valid = False
        except Exception as e:
            logger.error(f"❌ Coalesce validation failed: {e}")
            all_valid = False

        return all_valid

    def run(self,
            mode: str = 'full',
            steps: Optional[List[str]] = None,
            dry_run: bool = False) -> int:
        """
        Run the orchestrated workflow

        Args:
            mode: Sync mode (only 'full' is supported)
            steps: Optional list of specific steps to run
            dry_run: If True, simulate upload without actually uploading

        Returns:
            0 on success, non-zero on failure
        """
        logger.info("🚀 Starting Omni-Coalesce sync workflow")
        logger.info(f"Mode: {mode}")

        # Use CSV timestamp prefix as workflow start time if running a later step
        start_time = time.time()
        try:
            import re
            csv_files = list(self.bi_importer_dir.glob('*_dashboards.csv'))
            if csv_files:
                match = re.match(r'^(\d+)_', csv_files[0].name)
                if match:
                    start_time = int(match.group(1))
        except Exception:
            pass
        slack_webhook = self.env_vars.get('SLACK_WEBHOOK_URL', '')
        stats = {}
        error_msg = None
        upload_details = None
        tag_details = None

        # Available steps
        available_steps = ['validate', 'extract', 'generate', 'upload', 'tag']

        # Default to all steps except validate if not specified
        if not steps:
            steps = ['extract', 'generate', 'upload', 'tag']

        # Validate steps
        invalid_steps = [s for s in steps if s not in available_steps]
        if invalid_steps:
            logger.error(f"Invalid steps: {invalid_steps}")
            logger.info(f"Available steps: {available_steps}")
            return 1

        # Setup directories
        self.setup_directories()

        try:
            # Run validation if requested
            if 'validate' in steps:
                if not self.validate_connections():
                    logger.error("Connection validation failed")
                    error_msg = "Connection validation failed"
                    return 1
                if len(steps) == 1:
                    # Only validation was requested
                    return 0

            # Run extraction if requested
            if 'extract' in steps:
                extracted_data = self.extract_omni_metadata(mode=mode)
                if not extracted_data:
                    logger.error("Failed to extract Omni metadata")
                    error_msg = "Failed to extract Omni metadata"
                    return 1
                stats['dashboards'] = len(extracted_data.get('dashboards', []))
                stats['models'] = len(extracted_data.get('models', []))

            # Generate BI Importer files if requested
            if 'generate' in steps:
                result = self.generate_bi_importer_csv()
                if not result or not result.get('files_created'):
                    logger.error("Failed to generate BI Importer CSV files")
                    error_msg = "Failed to generate BI Importer CSV files"
                    return 1
                if 'statistics' in result:
                    stats.update(result['statistics'])

            # Load stats from extracted data if available (for Slack reporting)
            if 'extract' not in steps and not stats:
                try:
                    import json as _json
                    for name in ('dashboards', 'models', 'queries'):
                        f = self.extracted_data_dir / f'{name}.json'
                        if f.exists():
                            stats[name] = len(_json.loads(f.read_text()))
                    # Count fields from generated CSV
                    fields_csvs = list(self.bi_importer_dir.glob('*_dashboard_fields.csv'))
                    if fields_csvs:
                        stats['fields'] = sum(1 for _ in open(fields_csvs[0])) - 1
                except Exception:
                    pass

            # Upload to Coalesce if requested
            if 'upload' in steps:
                result = self.upload_to_coalesce(dry_run=dry_run)
                upload_details = {
                    'files_uploaded': [Path(f).name for f in result.get('files_uploaded', [])],
                    'files_failed': [
                        {'file': Path(f['file']).name, 'error': f.get('error', '')}
                        for f in result.get('files_failed', [])
                    ]
                }
                if not result.get('success'):
                    logger.error(f"❌ Upload failed")
                    logger.error(f"❌ No files uploaded successfully")
                    logger.error(f"Please check the error messages above and try again.")
                    logger.warning(f"Failed to upload to Coalesce. "
                                 f"You can manually upload the CSV files from: {self.bi_importer_dir}")
                    error_msg = result.get('error', 'Upload failed')
                    # Fail by default on upload error (can be overridden with FAIL_ON_UPLOAD_ERROR=false)
                    if self.env_vars.get('FAIL_ON_UPLOAD_ERROR', 'true').lower() != 'false':
                        return 1
                else:
                    stats['files_uploaded'] = len(result.get('files_uploaded', []))

            # Sync tags if requested
            if 'tag' in steps:
                result = self.sync_dashboard_tags(dry_run=dry_run)
                tag_details = {
                    'tags_synced': result.get('tags_synced', 0),
                    'catalog_dashboards': result.get('catalog_dashboards', 0),
                    'skipped': result.get('skipped', 0),
                    'error': result.get('error')
                }
                if not result.get('success'):
                    logger.warning(f"Tag sync had issues: {result.get('error')}")
                else:
                    stats['tags_synced'] = result.get('tags_synced', 0)

            logger.info("✅ Workflow completed successfully!")
            return 0

        except Exception as e:
            logger.error(f"Workflow failed: {e}", exc_info=True)
            error_msg = str(e)
            return 1

        finally:
            # Send Slack notification after tag step (ensures upload + tag data is available)
            if slack_webhook and 'tag' in steps:
                duration_sec = int(time.time() - start_time)
                m, s = divmod(duration_sec, 60)
                duration_str = f"{m:02d}:{s:02d}"
                status = "success" if error_msg is None else "failure"
                send_slack_notification(
                    webhook_url=slack_webhook,
                    status=status,
                    stats=stats,
                    duration=duration_str,
                    error=error_msg,
                    upload_details=upload_details,
                    tag_details=tag_details
                )

    def cleanup(self):
        """Clean up generated data"""
        logger.info("🧹 Cleaning up local run data...")

        if self.data_dir.exists():
            import shutil
            shutil.rmtree(self.data_dir)
            logger.info(f"Removed {self.data_dir}")
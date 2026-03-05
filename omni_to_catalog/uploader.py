"""
BI Importer Uploader Module

Handles uploading of BI Importer CSV files to Coalesce Catalog using castor-extractor.
"""

import os
import sys
import subprocess
import shutil
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime
import requests

logger = logging.getLogger(__name__)


class BIImporterUploader:
    """Upload BI Importer CSV files to Coalesce Catalog"""

    def __init__(self, api_token: str, source_id: str, zone: str = 'US', bi_importer_dir: Optional[Path] = None):
        """
        Initialize the uploader

        Args:
            api_token: Coalesce API token
            source_id: Source ID for the Coalesce instance
            zone: Zone (US or EU), defaults to US
            bi_importer_dir: Optional directory containing BI Importer CSV files
        """
        self.api_token = api_token
        self.source_id = source_id
        self.zone = zone.upper()  # Ensure uppercase
        self.bi_importer_dir = Path(bi_importer_dir) if bi_importer_dir else Path("bi_importer")
        self.castor_upload_path = None
        self._find_castor_upload()

    def _find_castor_upload(self) -> bool:
        """Find castor-upload executable"""
        # First try shutil.which to find in PATH
        castor_path = shutil.which('castor-upload')
        if castor_path:
            logger.info(f"Found castor-upload in PATH at: {castor_path}")
            self.castor_upload_path = castor_path
            return True

        # Try to find castor-upload in various locations
        possible_paths = [
            os.path.join(os.path.dirname(sys.executable), 'castor-upload'),  # Same directory as Python
            '.venv/bin/castor-upload',  # Virtual environment
            'venv/bin/castor-upload',  # Alternative virtual environment
            os.path.expanduser('~/.local/bin/castor-upload'),  # User local bin
        ]

        for path in possible_paths:
            if os.path.exists(path):
                try:
                    # Test if the command works
                    result = subprocess.run(
                        [path, '--help'],
                        capture_output=True,
                        text=True,
                        check=False,
                        env=os.environ.copy()
                    )
                    if 'castor-upload' in result.stdout or result.returncode == 0:
                        logger.info(f"Found castor-upload at {path}")
                        self.castor_upload_path = path
                        return True
                except (FileNotFoundError, PermissionError) as e:
                    logger.debug(f"Failed to check {path}: {e}")
                    continue

        logger.warning("castor-upload not found. Please install castor-extractor package.")
        return False

    def is_available(self) -> bool:
        """Check if castor-upload is available"""
        return self.castor_upload_path is not None

    def find_csv_files(self) -> List[Path]:
        """Find all BI Importer CSV files with timestamp prefix"""
        csv_files = []

        if not self.bi_importer_dir.exists():
            logger.warning(f"BI Importer directory does not exist: {self.bi_importer_dir}")
            return csv_files

        # Look for files with timestamp prefix (required format)
        for file in self.bi_importer_dir.glob('*.csv'):
            # Check if filename starts with Unix timestamp
            parts = file.name.split('_', 1)
            if len(parts) >= 2 and parts[0].isdigit():
                csv_files.append(file)

        return sorted(csv_files)

    def upload(self, csv_files: Optional[List[Path]] = None, dry_run: bool = False) -> Dict[str, Any]:
        """
        Upload CSV files to Coalesce Catalog

        Args:
            csv_files: Optional list of CSV files to upload (auto-discovers if not provided)
            dry_run: If True, only simulate the upload

        Returns:
            Dictionary with upload results
        """
        result = {
            'success': False,
            'files_uploaded': [],
            'files_failed': [],
            'error': None
        }

        # Check if castor-upload is available
        if not self.is_available():
            result['error'] = "castor-upload not available. Please install castor-extractor."
            logger.error(result['error'])
            return result

        # Find CSV files if not provided
        csv_files = csv_files or self.find_csv_files()

        if not csv_files:
            result['error'] = "No BI Importer CSV files found"
            logger.warning(result['error'])
            return result

        logger.info(f"Found {len(csv_files)} CSV files to upload")

        # Upload each file
        for csv_file in csv_files:
            if dry_run:
                logger.info(f"[DRY RUN] Would upload: {csv_file}")
                result['files_uploaded'].append(str(csv_file))
                continue

            try:
                logger.info(f"Uploading {csv_file.name}...")

                # Construct the command
                cmd = [
                    self.castor_upload_path,
                    '-k', self.api_token,
                    '-s', self.source_id,
                    '-z', self.zone,
                    '-f', str(csv_file),
                    '-t', 'VIZ'  # Type for BI Importer dashboards
                ]

                # Run the upload command
                process_result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=False,
                    env=os.environ.copy()
                )

                if process_result.returncode == 0:
                    logger.info(f"✅ Successfully uploaded {csv_file.name}")
                    result['files_uploaded'].append(str(csv_file))
                else:
                    error_output = process_result.stderr or process_result.stdout
                    # Parse known error patterns for cleaner messages
                    if '409' in error_output and 'Conflict' in error_output:
                        friendly_msg = f"File already uploaded (409 Conflict). Run 'generate' step first to create new files."
                        logger.warning(f"⚠️ {csv_file.name}: {friendly_msg}")
                    elif '500' in error_output and 'Internal Server Error' in error_output:
                        friendly_msg = "Server error (500). The file may contain invalid data."
                        logger.error(f"❌ {csv_file.name}: {friendly_msg}")
                    else:
                        # Extract just the last line (the actual error) from tracebacks
                        lines = error_output.strip().split('\n')
                        friendly_msg = lines[-1] if lines else error_output
                        logger.error(f"❌ Failed to upload {csv_file.name}: {friendly_msg}")
                    result['files_failed'].append({
                        'file': str(csv_file),
                        'error': friendly_msg
                    })

            except Exception as e:
                logger.error(f"❌ Error uploading {csv_file.name}: {e}")
                result['files_failed'].append({
                    'file': str(csv_file),
                    'error': str(e)
                })

        # Determine overall success
        result['success'] = len(result['files_failed']) == 0 and len(result['files_uploaded']) > 0

        if result['success']:
            logger.info(f"✅ All {len(result['files_uploaded'])} files uploaded successfully")
        elif result['files_uploaded']:
            logger.warning(f"⚠️ Partial success: {len(result['files_uploaded'])} uploaded, "
                          f"{len(result['files_failed'])} failed")
        else:
            logger.error("❌ No files uploaded successfully")

        return result

    def _get_graphql_url(self) -> str:
        """Get the Coalesce Catalog GraphQL API URL based on zone"""
        if self.zone == 'US':
            return 'https://api.us.castordoc.com/public/graphql'
        return 'https://api.castordoc.com/public/graphql'

    def get_catalog_dashboards(self) -> Dict[str, str]:
        """
        Fetch dashboards from Coalesce Catalog to get their internal UUIDs.

        Returns:
            Dictionary mapping dashboard name to Coalesce UUID
        """
        url = self._get_graphql_url()
        query = '''
        query ($scope: GetDashboardsScope, $pagination: Pagination) {
            getDashboards(scope: $scope, pagination: $pagination) {
                totalCount
                data {
                    id
                    name
                }
            }
        }
        '''

        name_to_uuid = {}
        page = 0
        per_page = 100

        while True:
            try:
                response = requests.post(
                    url,
                    json={
                        'query': query,
                        'variables': {
                            'scope': {'sourceId': self.source_id},
                            'pagination': {'nbPerPage': per_page, 'page': page}
                        }
                    },
                    headers={
                        'Authorization': f'Token {self.api_token}',
                        'Content-Type': 'application/json'
                    },
                    params={'op': 'getDashboards'},
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()

                if 'errors' in data:
                    logger.warning(f"GraphQL errors fetching dashboards: {data['errors']}")
                    break

                get_dashboards = data.get('data', {}).get('getDashboards', {})
                dashboards = get_dashboards.get('data', [])
                if not dashboards:
                    break

                for db in dashboards:
                    if db.get('name') and db.get('id'):
                        name_to_uuid[db['name']] = db['id']

                if len(dashboards) < per_page:
                    break
                page += 1

            except Exception as e:
                logger.warning(f"Failed to fetch Catalog dashboards: {e}")
                break

        logger.debug(f"Fetched {len(name_to_uuid)} dashboards from Coalesce Catalog")
        return name_to_uuid

    def sync_tags(self, tag_data: List[Dict[str, str]], dry_run: bool = False) -> Dict[str, Any]:
        """
        Attach tags to dashboard entities in Coalesce Catalog via GraphQL API.

        Args:
            tag_data: List of dicts with keys: label, entityId, entityType
            dry_run: If True, only simulate the API call

        Returns:
            Dictionary with sync results
        """
        result = {
            'success': False,
            'tags_synced': 0,
            'error': None
        }

        if not tag_data:
            logger.info("No tags to sync")
            result['success'] = True
            return result

        if dry_run:
            logger.info(f"[DRY RUN] Would sync {len(tag_data)} tags")
            result['success'] = True
            result['tags_synced'] = len(tag_data)
            return result

        url = self._get_graphql_url()
        mutation = 'mutation ($tags: [BaseTagEntityInput!]!) { attachTags(data: $tags) }'

        # Process in batches of 500 (API limit)
        batch_size = 500
        total_synced = 0

        for i in range(0, len(tag_data), batch_size):
            batch = tag_data[i:i + batch_size]
            logger.info(f"Syncing tags batch {i // batch_size + 1} ({len(batch)} tags)...")

            try:
                response = requests.post(
                    url,
                    json={
                        'query': mutation,
                        'variables': {'tags': batch}
                    },
                    headers={
                        'Authorization': f'Token {self.api_token}',
                        'Content-Type': 'application/json'
                    },
                    params={'op': 'attachTags'},
                    timeout=30
                )
                response.raise_for_status()

                resp_data = response.json()
                if 'errors' in resp_data:
                    logger.warning(f"GraphQL errors: {resp_data['errors']}")
                    result['error'] = str(resp_data['errors'])
                else:
                    total_synced += len(batch)
                    logger.info(f"  Synced {len(batch)} tags")

            except requests.exceptions.HTTPError as e:
                logger.error(f"Tag sync API error: HTTP {e.response.status_code} - {e}")
                result['error'] = str(e)
            except Exception as e:
                logger.error(f"Tag sync failed: {e}")
                result['error'] = str(e)

        result['tags_synced'] = total_synced
        result['success'] = total_synced > 0 and result['error'] is None
        logger.info(f"Tag sync complete: {total_synced}/{len(tag_data)} tags synced")

        return result

    def test_connection(self) -> bool:
        """Test connection to Coalesce API"""
        if not self.is_available():
            logger.error("castor-upload not available")
            return False

        try:
            # Try running with --help to verify it's working
            cmd = [self.castor_upload_path, '--help']
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)

            if result.returncode == 0:
                logger.info("✅ castor-upload is working")
                return True
            else:
                logger.error(f"castor-upload test failed: {result.stderr}")
                return False

        except Exception as e:
            logger.error(f"Failed to test castor-upload: {e}")
            return False
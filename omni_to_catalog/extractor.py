"""
Omni API Extractor Module

Handles extraction of models, dashboards, queries, and connections from Omni API.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class OmniExtractor:
    """Extracts metadata from Omni API"""

    def __init__(self, base_url: str, api_token: str, output_dir: Optional[Path] = None):
        """
        Initialize Omni API extractor

        Args:
            base_url: Base URL for Omni API (e.g., https://company.omniapp.co)
            api_token: API authentication token
            output_dir: Optional directory to save extracted data
        """
        self.base_url = base_url.rstrip('/')
        self.api_url = f"{self.base_url}/api"
        self.output_dir = Path(output_dir) if output_dir else Path("extracted_data")
        self.session = self._create_session(api_token)
        self.extracted_data = {}

    def _create_session(self, api_token: str) -> requests.Session:
        """Create requests session with retry logic"""
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.headers.update({
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        })
        return session

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make GET request to Omni API

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            JSON response
        """
        url = f"{self.api_url}/{endpoint}"
        logger.debug(f"GET {url}")

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            # For Git endpoints, 403 is expected when Git is not configured
            if '/git' in endpoint and e.response.status_code == 403:
                logger.debug(f"Git endpoint not available for {endpoint} (403 Forbidden - this is normal)")
            # For analytics endpoints, 404 is expected when analytics are not available
            elif '/analytics' in endpoint and e.response.status_code == 404:
                logger.debug(f"Analytics not available for {endpoint} (404 Not Found - this is normal)")
            else:
                logger.error(f"Failed to fetch {endpoint}: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch {endpoint}: {e}")
            raise

    def extract_models(self) -> List[Dict[str, Any]]:
        """Extract all models from Omni"""
        logger.info("Extracting models...")
        models = []

        try:
            # Get list of models
            response = self.get("v1/models")

            # Handle both list and dict with records field
            models_list = []
            if isinstance(response, list):
                models_list = response
            elif isinstance(response, dict) and 'records' in response:
                models_list = response['records']
            else:
                logger.warning(f"Unexpected models response type: {type(response)}")
                return []

            for model in models_list:
                model_id = model.get('id')
                if model_id:
                    # Use ID as name if name is not provided
                    model_name = model.get('name') or f"Model_{model.get('modelKind', 'UNKNOWN')}_{model_id[:8]}"
                    model['name'] = model_name
                    logger.debug(f"  Extracting model: {model_name}")

                    # Get model YAML definition
                    try:
                        yaml_content = self.get(f"v1/models/{model_id}/yaml")
                        model['yaml_definition'] = yaml_content
                    except Exception as e:
                        logger.warning(f"Failed to get YAML for model {model_id}: {e}")
                        model['yaml_definition'] = None

                    # Get model Git configuration if available
                    try:
                        git_config = self.get(f"v1/models/{model_id}/git")
                        model['git_config'] = git_config
                    except Exception as e:
                        logger.debug(f"No Git config for model {model_id}: {e}")
                        model['git_config'] = None

                    models.append(model)

            logger.info(f"Extracted {len(models)} models")
            return models

        except Exception as e:
            logger.error(f"Failed to extract models: {e}")
            return []

    def fetch_content_metadata(self) -> Dict[str, Dict]:
        """
        Fetch content metadata including view counts from Omni content API.

        Returns:
            Dictionary mapping content ID to metadata including view_count
        """
        content_metadata = {}

        # Always fetch view counts from the content API
        logger.info("Fetching view counts from Omni content API...")

        # Try to fetch from content endpoint with organization scope
        try:
            all_content_items = []
            next_cursor = None
            page_count = 0
            max_pages = 10  # Safety limit

            while page_count < max_pages:
                # Request organization-scoped content with proper pagination
                # IMPORTANT: include=_count is required to get view counts
                params = {
                    "scope": "organization",
                    "pageSize": 100,
                    "include": "_count",  # This is required to get view counts!
                    "sortField": "name"  # Optional: sort by name for consistency
                }
                if next_cursor:
                    params["cursor"] = next_cursor

                response = self.get("v1/content", params=params)
                page_count += 1

                logger.debug(f"Content API response type: {type(response)}")
                if isinstance(response, dict):
                    logger.debug(f"Content API response keys: {list(response.keys())[:5]}")

                # Handle the paginated response structure
                content_items = []
                has_next_page = False

                if isinstance(response, list):
                    content_items = response
                elif isinstance(response, dict):
                    # Check for paginated response structure
                    if 'records' in response:
                        content_items = response['records']
                        # Check for pagination info
                        if 'pageInfo' in response:
                            has_next_page = response['pageInfo'].get('hasNextPage', False)
                            next_cursor = response['pageInfo'].get('nextCursor')
                    elif 'content' in response:
                        content_items = response['content']
                    elif 'items' in response:
                        content_items = response['items']
                    else:
                        # If dict has no known container key, treat values as items
                        content_items = list(response.values()) if response else []

                all_content_items.extend(content_items)

                # Check if we should continue pagination
                if not has_next_page or not next_cursor:
                    break

                logger.debug(f"Fetching next page (cursor: {next_cursor})")

            content_items = all_content_items
            logger.info(f"Fetched {len(content_items)} total content items across {page_count} page(s)")

            # Extract metadata for each content item
            for idx, item in enumerate(content_items):
                if idx == 0:
                    # Log first item to see available fields
                    logger.debug(f"First content item fields: {list(item.keys()) if isinstance(item, dict) else 'Not a dict'}")

                # Use identifier as the primary key (matches dashboard IDs)
                content_id = item.get('identifier') or item.get('id')
                if content_id:
                    # Handle nested _count structure from the API
                    view_count = 0
                    if '_count' in item and isinstance(item['_count'], dict):
                        view_count = item['_count'].get('views', 0)
                    else:
                        # Fallback to top-level fields if no _count
                        view_count = (item.get('viewCount') or
                                    item.get('view_count') or
                                    item.get('views') or
                                    item.get('totalViews') or
                                    item.get('total_views') or 0)

                    # Extract scope (should be 'organization' for dashboards)
                    scope = item.get('scope', '')

                    content_metadata[content_id] = {
                        'view_count': view_count,
                        'scope': scope,
                        'name': item.get('name', ''),
                        'last_viewed_at': item.get('lastViewedAt', item.get('last_viewed_at', '')),
                        'created_by': item.get('createdBy', item.get('created_by', '')),
                        'updated_by': item.get('updatedBy', item.get('updated_by', ''))
                    }

                    if view_count > 0:
                        logger.debug(f"Content {content_id} '{item.get('name', '')}' has {view_count} views (scope: {scope})")

            logger.info(f"Fetched metadata for {len(content_metadata)} content items")

        except Exception as e:
            logger.warning(f"Could not fetch content metadata: {e}")
            # Return empty dict on failure so extraction continues

        return content_metadata

    def fetch_user_emails(self, owner_names: set) -> Dict[str, str]:
        """
        Fetch user emails from Omni SCIM API by listing all users
        and matching by displayName to dashboard owner names.

        The dashboard owner.id and SCIM user.id are different identifiers,
        so we match on displayName instead.

        Args:
            owner_names: Set of owner display names to look up

        Returns:
            Dictionary mapping owner display name to email address
        """
        user_emails = {}
        if not owner_names:
            return user_emails

        logger.debug(f"Fetching user list from Omni SCIM API to resolve {len(owner_names)} owner emails...")

        try:
            all_users = []
            start_index = 1

            while True:
                url = f"{self.api_url}/scim/v2/users"
                params = {"count": 100, "startIndex": start_index}
                logger.debug(f"Fetching SCIM users: GET {url} (startIndex={start_index})")
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                resources = data.get('Resources', [])
                all_users.extend(resources)

                total_results = data.get('totalResults', 0)
                if start_index + len(resources) > total_results:
                    break
                start_index += len(resources)

            logger.debug(f"Fetched {len(all_users)} SCIM users")

            # Build lookup by displayName -> primary email
            for user in all_users:
                display_name = user.get('displayName', '')
                if display_name in owner_names:
                    emails = user.get('emails', [])
                    email = ''
                    for entry in emails:
                        if isinstance(entry, dict):
                            if entry.get('primary'):
                                email = entry.get('value', '')
                                break
                            elif not email:
                                email = entry.get('value', '')

                    if email:
                        user_emails[display_name] = email
                        logger.debug(f"  Matched owner '{display_name}'")

        except requests.exceptions.HTTPError as e:
            logger.warning(f"  SCIM API error listing users: HTTP {e.response.status_code} - {e}")
        except Exception as e:
            logger.warning(f"  Could not fetch SCIM users: {e}")

        logger.debug(f"Resolved emails for {len(user_emails)}/{len(owner_names)} owners")
        return user_emails

    def extract_dashboards(self) -> List[Dict[str, Any]]:
        """Extract all dashboards from Omni"""
        logger.info("Extracting dashboards...")
        dashboards = []

        # First, fetch content metadata including view counts
        content_metadata = self.fetch_content_metadata()

        try:
            # Get list of documents (dashboards)
            response = self.get("v1/documents", params={"include": "labels"})

            # Handle both list and dict with records field
            documents = []
            if isinstance(response, list):
                documents = response
            elif isinstance(response, dict) and 'records' in response:
                documents = response['records']
            else:
                logger.warning(f"Unexpected documents response type: {type(response)}")
                return []

            for doc in documents:
                # Check if it's a dashboard (has hasDashboard flag or type == 'dashboard')
                if doc.get('hasDashboard') or doc.get('type') == 'dashboard':
                    # Use identifier field for ID
                    dashboard_id = doc.get('identifier') or doc.get('id')
                    if dashboard_id:
                        logger.debug(f"  Extracting dashboard: {doc.get('name', dashboard_id)}")

                        # Add the ID to the document
                        doc['id'] = dashboard_id

                        # Add view count and other metadata if available
                        if dashboard_id in content_metadata:
                            metadata = content_metadata[dashboard_id]
                            doc['view_count'] = metadata.get('view_count', 0)
                            doc['last_viewed_at'] = metadata.get('last_viewed_at', '')
                            # Add created_by/updated_by if not already present
                            if 'created_by' not in doc:
                                doc['created_by'] = metadata.get('created_by', '')
                            if 'updated_by' not in doc:
                                doc['updated_by'] = metadata.get('updated_by', '')
                        else:
                            # Check if view count is already in the document response
                            # Some APIs include metrics directly in the document data
                            doc['view_count'] = (doc.get('viewCount') or
                                              doc.get('view_count') or
                                              doc.get('views') or
                                              doc.get('metrics', {}).get('views') or
                                              doc.get('analytics', {}).get('viewCount') or
                                              doc.get('stats', {}).get('views') or 0)

                        # Get dashboard filters
                        try:
                            filters = self.get(f"v1/dashboards/{dashboard_id}/filters")
                            doc['filters'] = filters
                        except Exception as e:
                            logger.debug(f"No filters for dashboard {dashboard_id}: {e}")
                            doc['filters'] = []

                        # Get document queries
                        try:
                            response = self.get(f"v1/documents/{dashboard_id}/queries")
                            # The API returns {'queries': [...]} structure
                            if isinstance(response, dict) and 'queries' in response:
                                doc['queries'] = response['queries']
                                logger.debug(f"    Found {len(response['queries'])} queries for dashboard")
                            else:
                                doc['queries'] = []
                                logger.debug(f"    Found 0 queries for dashboard")
                        except Exception as e:
                            logger.debug(f"Could not get queries for document {dashboard_id}: {e}")
                            doc['queries'] = []

                        # Get dashboard export (beta)
                        try:
                            export_data = self.get(f"unstable/documents/{dashboard_id}/export")
                            doc['export_data'] = export_data
                        except Exception as e:
                            logger.debug(f"Could not export dashboard {dashboard_id}: {e}")
                            doc['export_data'] = None

                        # Try to fetch individual dashboard analytics if not already found
                        # Note: This is a fallback - view counts should be fetched via content API with include=_count
                        if doc.get('view_count', 0) == 0:
                            try:
                                # Try dashboard-specific analytics endpoint as fallback
                                analytics_data = self.get(f"v1/dashboards/{dashboard_id}/analytics")
                                if analytics_data:
                                    doc['view_count'] = (analytics_data.get('viewCount') or
                                                       analytics_data.get('views') or
                                                       analytics_data.get('totalViews') or 0)
                                    if doc['view_count'] > 0:
                                        logger.info(f"    Found {doc['view_count']} views from analytics endpoint")
                            except Exception as e:
                                # Analytics endpoint may not be available, this is normal
                                logger.debug(f"Analytics endpoint not available for dashboard {dashboard_id} (this is normal)")

                        dashboards.append(doc)

            # Enrich owner data with email addresses from SCIM API
            owner_names = set()
            for doc in dashboards:
                owner = doc.get('owner', {})
                if isinstance(owner, dict) and owner.get('name'):
                    owner_names.add(owner['name'])

            if owner_names:
                user_emails = self.fetch_user_emails(owner_names)
                for doc in dashboards:
                    owner = doc.get('owner', {})
                    if isinstance(owner, dict) and owner.get('name'):
                        email = user_emails.get(owner['name'])
                        if email:
                            doc['owner']['email'] = email

            logger.info(f"Extracted {len(dashboards)} dashboards")
            return dashboards

        except Exception as e:
            logger.error(f"Failed to extract dashboards: {e}")
            return []


    def extract_connections(self) -> List[Dict[str, Any]]:
        """Extract connection configurations from Omni"""
        logger.info("Extracting connections...")

        try:
            response = self.get("v1/connections")
            # The API returns a dict with 'connections' key
            if isinstance(response, dict) and 'connections' in response:
                connections = response['connections']
            else:
                connections = response if isinstance(response, list) else []

            logger.info(f"Extracted {len(connections)} connections")
            return connections

        except Exception as e:
            logger.error(f"Failed to extract connections: {e}")
            return []

    def extract(self, mode: str = 'full') -> Dict[str, List]:
        """
        Extract all data from Omni API

        Args:
            mode: Extraction mode (only 'full' is supported)

        Returns:
            Dictionary containing extracted data
        """
        logger.info(f"Starting extraction in {mode} mode...")

        self.extracted_data = {
            'models': [],
            'dashboards': [],
            'connections': []
        }

        # Always extract everything in full mode
        self.extracted_data['connections'] = self.extract_connections()
        self.extracted_data['models'] = self.extract_models()
        self.extracted_data['dashboards'] = self.extract_dashboards()

        logger.info(f"Extraction complete: {len(self.extracted_data['models'])} models, "
                   f"{len(self.extracted_data['dashboards'])} dashboards, "
                   f"{len(self.extracted_data['connections'])} connections")

        return self.extracted_data

    def save_to_files(self, data: Optional[Dict] = None) -> Dict[str, str]:
        """
        Save extracted data to JSON files

        Args:
            data: Optional data to save (uses self.extracted_data if not provided)

        Returns:
            Dictionary of saved file paths
        """
        data = data or self.extracted_data

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

        saved_files = {}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save each data type
        for data_type, records in data.items():
            if records:
                file_path = self.output_dir / f"{data_type}.json"
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(records, f, indent=2, default=str)

                saved_files[data_type] = str(file_path)
                logger.info(f"Saved {len(records)} {data_type} to {file_path}")

        # Save metadata
        metadata = {
            'extraction_time': timestamp,
            'base_url': self.base_url,
            'record_counts': {k: len(v) for k, v in data.items()}
        }
        metadata_path = self.output_dir / 'extraction_metadata.json'
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)

        saved_files['metadata'] = str(metadata_path)

        return saved_files

    def test_connection(self) -> bool:
        """Test connection to Omni API"""
        try:
            # Try to get user info or models list as a test
            response = self.get("v1/models")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
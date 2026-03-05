# Omni to Coalesce Catalog Sync

> **Disclaimer:** This is a sample project provided as-is for reference.

A self-service GitHub Action workflow to sync Omni dashboards and models to Coalesce Catalog, making them appear in the Dashboards section.

## Key Features

### Data Extraction (Step 1)

- **Automated Extraction**: Pulls dashboards, models, queries, topics, and relationships from Omni API
- **View Counts**: Automatically fetches dashboard view counts from Omni content API
- **Owner Email Resolution**: Resolves dashboard owner emails via the Omni SCIM API
- **Dashboard Labels**: Extracts dashboard labels from Omni for tag syncing
- **Full Data Sync**: Syncs all dashboards and models in a single operation

### Data Processing & Transformation (Step 2)

- **Field Extraction**: Extracts all fields used in dashboard queries with type inference
- **Complete SQL Generation**: Generates executable SQL with CTEs from view definitions
- **CTE Lineage Parsing**: Traces field-level lineage through nested CTEs back to source tables
- **BI Importer Format**: Converts data to CSV format compatible with Coalesce BI Importer

### Upload & Integration (Step 3)

- **Automatic Upload**: Uses castor-extractor to automatically upload to Coalesce Catalog
- **Dashboard Integration**: Ensures dashboards appear in the Dashboards section
- **Note**: Coalesce Catalog runs an ingestion process once per day, so uploaded files won't appear immediately. Allow up to 24 hours for new dashboards to show up.

### Tag Sync (Step 4)

- **Label-to-Tag Sync**: Syncs Omni dashboard labels as tags in Coalesce Catalog via GraphQL API
- **Automatic UUID Mapping**: Looks up Coalesce dashboard UUIDs by name to attach tags to the correct entities
- **Note**: Tags are applied via the API directly, so if the dashboard already exists in Catalog, tag updates will appear right away.

### Automation & Monitoring

- **Scheduled Sync**: Runs daily at 8 AM UTC or on-demand via GitHub Actions
- **Slack Notifications**: Detailed Slack messages after each upload with file-level results, tag sync details, and error reporting
- **GitHub Actions Summary**: Rich job summary with per-step status table and artifact links

## Important Notes

1. **CSV File Naming**: All BI Importer CSV files MUST be prefixed with a Unix timestamp (e.g., `1771799348_dashboards.csv`). This is a strict requirement for Coalesce BI Importer.

2. **Dashboard Hierarchy**: The system creates a linear data flow hierarchy in Coalesce Catalog:
   - **VIZ_MODEL**: The workbook model that powers dashboards (semantic/modeling layer - top level)
   - **TILE**: Individual query visualizations (individual charts/tables - child of VIZ_MODEL)
   - **DASHBOARD**: The actual Omni dashboard (presentation layer - child of TILE)

3. **Generic BI Integration Limitations**: The Coalesce generic BI integration has inherent limitations that cannot be overcome:
   - No custom icons for entities (all use default icons)
   - No popularity metrics (view counts) for VIZ_MODEL or TILE entities
   - No field-level lineage for TILEs (fields only link to VIZ_MODEL)
   - Fields must be explicitly defined (not extracted from SQL)

## Requirements and Compatibility

### System Requirements

- **Python**: 3.10 or higher
- **Memory**: Minimum 4GB RAM (8GB+ recommended for large datasets)
- **Disk Space**: 2x the size of your extracted data for processing
- **Network**: Stable connection for API calls

### API Version Compatibility

- **Omni API**: v1 (REST API)
- **Coalesce BI Importer**: Generic Viz format v2
- **castor-extractor**: Latest version (auto-installed)

### Known Compatible Environments

- **GitHub Actions**: Ubuntu latest, macOS latest
- **Local Development**: macOS, Linux, WSL2 on Windows
- **Cloud Platforms**: AWS Lambda, Google Cloud Functions, Azure Functions

## Quick Start

### Prerequisites

- Python 3.10 – 3.13
- pip package manager

### 1. Setup Repository Configuration

#### Repository Variables
Add these as repository variables (Settings → Secrets and variables → Actions → Variables tab):

```
OMNI_BASE_URL         # e.g., https://your-company.omniapp.co
COALESCE_SOURCE_ID    # Your Coalesce source ID (UUID)
COALESCE_ZONE         # Zone: US or EU (defaults to US)
```

#### Repository Secrets
Add these as secrets (Settings → Secrets and variables → Actions → Secrets tab):

```
OMNI_API_TOKEN        # Your Omni API token
COALESCE_API_TOKEN    # Your Coalesce API token
```

Optional notification secret:
```
SLACK_WEBHOOK_URL     # For Slack notifications
```

### 2. Run the Workflow

#### Manual Trigger

1. Go to Actions tab in your GitHub repository
2. Select "Omni-Coalesce Catalog Sync"
3. Click "Run workflow"

#### Enabling Automated Triggers

The workflow ships with only manual trigger (`workflow_dispatch`) enabled. To enable automated runs, edit `.github/workflows/omni-coalesce-sync.yml` and uncomment the triggers you want:

```yaml
on:
  workflow_dispatch:
  # To enable scheduled runs, uncomment the next two lines:
  # schedule:
  #   - cron: '0 8 * * *'
  # To enable auto-run on push, uncomment the next three lines:
  # push:
  #   branches:
  #     - main
```

- **Schedule**: Runs daily at 8 AM UTC (adjust the cron expression as needed)
- **Push**: Runs automatically when code is pushed to `main`

### 3. Automatic Upload to Coalesce

The workflow now automatically uploads BI Importer files using castor-extractor:

The GitHub Action workflow automatically uploads the generated CSV files to Coalesce Catalog using the castor-extractor package.

**Note on timing:**

- **Upload (CSV files)**: Coalesce Catalog runs an ingestion process once per day, so uploaded files won't appear immediately. Allow up to 24 hours for new dashboards to show up in the Dashboards section.
- **Tags**: Tag sync calls the Coalesce GraphQL API directly, so if the dashboard already exists in Catalog, tag updates will appear right away.

## Workflow Architecture

### GitHub Actions Jobs

The workflow runs as 3 sequential jobs:

| Job | Description | Triggers |
|-----|-------------|----------|
| **extract-omni-metadata** | Fetches dashboards, models, queries, connections, owner emails (SCIM), and labels from Omni API | Always |
| **generate-bi-importer-files** | Converts extracted data to BI Importer CSV format (dashboards, queries, fields) | After extract |
| **upload-sync-notify** | Uploads CSV files, syncs dashboard tags, and sends Slack notification | After generate |

### Slack Notifications

When `SLACK_WEBHOOK_URL` is configured, a Slack message is sent after every upload with:

- **Status**: Success or failure with duration
- **Extracted counts**: Dashboards, models, queries, fields
- **Upload details**: Each file listed with success/failure status
- **Tag sync details**: Tags synced, dashboards found in Catalog, dashboards skipped
- **Error details**: Friendly error messages (e.g., "File already uploaded - run generate step first")

Slack notifications fire after both the upload and tag sync steps complete, so the message includes the full picture. Running `--steps extract` or `--steps generate` alone will not trigger a notification.

### Data Extracted from Omni

- **Models**: Model definitions with YAML configurations (foundation/semantic layer)
- **View Definitions**: SQL definitions from model YAML for CTE generation
- **Topics**: Topic hierarchies for model organization
- **Relationships**: Model relationships and dependencies
- **Dashboards**: All dashboard definitions and metadata
- **View Counts**: Dashboard popularity metrics
- **Queries**: Dashboard queries with complete SQL (including userEditedSQL when available)
- **Owner Emails**: Dashboard owner email addresses resolved via SCIM API (matched by display name)
- **Labels**: Dashboard labels for syncing as tags in Coalesce Catalog
- **Fields**: All fields extracted from models and dashboard queries with type inference

### Data Flow

```
Omni API → JSON Files → CSV Files → castor-extractor → Coalesce BI Importer → Dashboards Section
                                                    ↘ GraphQL API → Tags on Dashboards
```

Or more detailed:

```
Extract          Transform         Upload           Process         Display
   ↓                ↓                ↓                ↓               ↓
Omni API  →  JSON Files  →  CSV Files  →  castor-extractor  →  Coalesce Catalog
(+ SCIM)                                                              ↓
                                         Tag Sync  →  GraphQL API  → Dashboards Section
```

## Local Development

### Complete Environment Variables Reference

Configure these in your `.env` file:

#### Required Variables

| Variable | Description | Example |
| -------- | ----------- | ------- |
| `OMNI_API_TOKEN` | Your Omni API token | `omni_api_...` |
| `OMNI_BASE_URL` | Omni instance URL | `https://your-company.omniapp.co` |
| `COALESCE_API_TOKEN` | Your Coalesce API token | `clsc_...` |
| `COALESCE_SOURCE_ID` | Coalesce source ID (UUID) | `a1b2c3d4-...` |
| `COALESCE_ZONE` | Deployment zone | `US` or `EU` |

#### Optional Variables

| Variable | Description | Default | Values |
| -------- | ----------- | ------- | ------ |
| `SLACK_WEBHOOK_URL` | Slack notifications | None | Webhook URL |
| `VERBOSE` | Enable verbose logging | `false` | true/false |

### Installation

```bash
# Clone repository
git clone https://github.com/jessemarshall/coalesce_generic_viz_for_omni.git
cd coalesce_generic_viz_for_omni

# Setup environment
cp .env.example .env
# Edit .env with your API credentials and configuration

# Setup Python environment (creates .venv and installs package)
make setup
source .venv/bin/activate

# Or install manually
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

**Note:** All required dependencies including `castor-extractor` (for Coalesce uploads) and `sqlglot` (for SQL/CTE parsing) are automatically installed with the package.

### Running the Sync

```bash
# Full workflow (extract, generate, upload, tag)
make run

# Or use the CLI directly
omni-to-catalog --env-file .env

# Or as a Python module
python -m omni_to_catalog.cli --env-file .env
```

### Available Commands

```bash
make setup         # Initial setup with virtual environment
make run           # Run full sync workflow (extract, generate, upload, tag)
make extract       # Extract from Omni only
make generate      # Generate BI Importer CSV files
make upload        # Upload CSV files to Coalesce Catalog
make tag           # Sync dashboard labels as tags to Coalesce Catalog
make validate      # Test API connections
make clean         # Clean up local data

# Advanced commands
make debug-run     # Run with debug logging
```

### CLI Usage

The `omni-to-catalog` command provides flexible options:

```bash
# Run specific steps
omni-to-catalog --steps extract generate
omni-to-catalog --steps upload
omni-to-catalog --steps tag

# Validate connections only
omni-to-catalog --steps validate

# Dry run (simulate upload)
omni-to-catalog --dry-run

# Use different environment file
omni-to-catalog --env-file .env.staging

# Enable verbose/debug logging
omni-to-catalog --verbose
omni-to-catalog --debug

# Clean up local data
omni-to-catalog --cleanup
```

### Python Package API

You can also use the package programmatically:

```python
from omni_to_catalog import OmniExtractor, OmniToBIImporter, BIImporterUploader
from omni_to_catalog.orchestrator import WorkflowOrchestrator

# Use individual components
extractor = OmniExtractor(base_url, api_token)
data = extractor.extract(mode='full')

# Or use the orchestrator
orchestrator = WorkflowOrchestrator(env_file='.env')
orchestrator.run(mode='full', steps=['extract', 'generate', 'upload', 'tag'])
```

## Field-Level Lineage with CTE Support

The system includes advanced SQL parsing that traces field-level lineage through complex queries with CTEs (Common Table Expressions). This feature:

- **Parses Nested CTEs**: Handles queries with multiple levels of WITH clauses
- **Traces to Source**: Follows field references back through CTEs to original source tables
- **Handles Transformations**: Understands aggregations, date functions, and expressions
- **Accurate Lineage**: Provides complete `DATABASE.SCHEMA.TABLE.COLUMN` paths for field lineage

### Example

For a query with nested CTEs like:
```sql
WITH deduped_nodes AS (
    SELECT node_id, MIN(created_at) as create_date
    FROM warehouse.edw.node_table
    GROUP BY node_id
),
monthly_nodes AS (
    SELECT DATE_TRUNC('month', create_date) as month, COUNT(*) as count
    FROM deduped_nodes
    GROUP BY month
)
SELECT * FROM monthly_nodes
```

The `parent_columns` field will trace back to the original source:
- `month` → `['warehouse.edw.node_table.created_at']`
- `count` → `['warehouse.edw.node_table.node_id']`

This ensures proper lineage matching in Coalesce Catalog's push_lineage task.

## Dashboard Hierarchy in Coalesce Catalog

The Omni to Coalesce sync creates a sophisticated three-level hierarchy that far exceeds basic BI tool integrations. This hierarchy properly represents the complete dashboard structure, from presentation layer to semantic model to individual visualizations.

### Why the Hierarchy Matters

Traditional BI tool integrations often flatten dashboard structures, losing critical relationships between components. Our implementation preserves:

- **Semantic relationships** between dashboards and their underlying data models
- **Component structure** showing which visualizations belong to which dashboards
- **Lineage tracking** from dashboard tiles back to source database tables
- **Model reuse** patterns when multiple dashboards share the same VIZ_MODEL

### Understanding the Three-Level Hierarchy

```
VIZ_MODEL (Sales Analytics Model)               <- Semantic/modeling layer (top-level)
    ↓ parent_dashboards field
TILE (Monthly Revenue Chart)                     <- Individual visualizations
    ↓ parent_dashboards field
DASHBOARD (Sales Overview Dashboard)           <- Presentation layer/dashboard
```

This creates a linear data flow: **VIZ_MODEL → TILE → DASHBOARD**

### Entity Types

#### VIZ_MODEL

- **What it is**: The semantic/modeling layer that powers dashboards (top-level entity)
- **Purpose**: Contains the data model, business logic, and field definitions
- **Example**: Workbook model with ID `a1b2c3d4-1234-5678-abcd-ef1234567890`
- **Links to**: Nothing (top-level entity, `parent_dashboards` field is empty)
- **Parent Tables**: Contains list of source database tables (only VIZ_MODEL has this)
- **Special**: Only VIZ_MODEL entries can have fields in `dashboard_fields.csv`

#### TILE

- **What it is**: Individual query visualizations/charts
- **Purpose**: Represents specific charts, tables, or metrics (middle layer between model and dashboard)
- **Example**: "Monthly Revenue Chart" showing revenue trends by month
- **Links to**: Parent VIZ_MODEL (via `parent_dashboards` field)
- **Parent Tables**: Empty (visualizations don't directly connect to tables)

#### DASHBOARD

- **What it is**: The actual Omni dashboard that users interact with
- **Purpose**: Represents the presentation layer for organizing and displaying tiles
- **Example**: "Sales Overview Dashboard" with ID `abc12345`
- **Links to**: Parent TILE(s) (via `parent_dashboards` field)
- **Parent Tables**: Empty (presentation layer doesn't directly connect to tables)

### Example: Sales Overview Dashboard

For a dashboard named "Sales Overview Dashboard", the system creates:

1. **VIZ_MODEL entry** (top-level):

   ```yaml
   id: a1b2c3d4-1234-5678-abcd-ef1234567890
   dashboard_type: VIZ_MODEL
   name: Sales Analytics Model
   parent_dashboards: [] (empty - top-level entity)
   ```

2. **TILE entry** (middle layer):

   ```yaml
   id: f9e8d7c6-5432-1098-fedc-ba9876543210
   dashboard_type: TILE
   name: Monthly Revenue Chart
   parent_dashboards: [a1b2c3d4-1234-5678-abcd-ef1234567890] (links to VIZ_MODEL)
   ```

3. **DASHBOARD entry** (presentation layer):

   ```yaml
   id: abc12345
   dashboard_type: DASHBOARD
   name: Sales Overview Dashboard
   parent_dashboards: [f9e8d7c6-5432-1098-fedc-ba9876543210] (links to TILE)
   ```

## CSV File Formats

### dashboards.csv

Contains all three types of entries (DASHBOARD, VIZ_MODEL, TILE) in a single file:

| Field | Description | Required | Values by Type |
|-------|-------------|----------|----------------|
| created_at | ISO timestamp | No | 2024-01-15T10:30:00Z |
| dashboard_type | Entity type | Yes | DASHBOARD, VIZ_MODEL, or TILE |
| description | Description | No | Entity description |
| folder_path | Folder organization | No | /reports/sales |
| id | Unique identifier | Yes | Dashboard ID, Model ID, or Tile ID |
| name | Display name | Yes | Dashboard/Model/Tile name |
| parent_dashboards | Parent entity IDs | No | See hierarchy rules below |
| parent_tables | Source tables array (excludes CTEs) | No | ['analytics_db.public.orders'] |
| updated_at | ISO timestamp | No | 2024-02-01T15:45:00Z |
| url | Link to entity | Yes | Dashboard/Tile URL (VIZ_MODEL uses /models/{id}/ide?mode=combined) |
| user_name | Owner email (resolved via SCIM API, empty if unavailable) | No | john@example.com |
| view_count | Popularity metric | No | 150 (for DASHBOARD only) |

#### Parent Tables Handling

The `parent_tables` field contains only actual database tables in `database.schema.table` format:

- **VIZ_MODEL Only**: Only VIZ_MODEL entries have parent_tables (as they represent the semantic/data layer)
- **DASHBOARD and TILE**: These have empty parent_tables (they're presentation layers that use the VIZ_MODEL)
- **CTE Exclusion**: CTEs (Common Table Expressions) are automatically detected and excluded
- **Source Tables Only**: Only real database tables are included, not intermediate views or CTEs

Example: For a VIZ_MODEL with queries using CTEs like `WITH monthly_revenue_summary AS (SELECT FROM analytics_db.public.orders)`,
the VIZ_MODEL will have parent_tables: `['analytics_db.public.orders']`, while its child DASHBOARD and TILE entries will have empty parent_tables.

#### Parent Dashboard Rules

The `parent_dashboards` field must always be formatted as an array (e.g., `['id1','id2']`), even for single values.

This creates a linear data flow hierarchy:

- **VIZ_MODEL**: Empty (top-level entity has no parents)
- **TILE**: Array containing parent VIZ_MODEL ID (e.g., `['a1b2c3d4-1234-5678-abcd-ef1234567890']`)
- **DASHBOARD**: Array containing parent TILE ID(s) (e.g., `['f9e8d7c6-5432-1098-fedc-ba9876543210']`)

This creates the flow: `Table → VIZ_MODEL → TILE → DASHBOARD`

### dashboard_queries.csv

Links SQL queries to both TILE and DASHBOARD entries. Each TILE query is also duplicated onto its parent DASHBOARD so that query lineage is visible at the dashboard level.

| Field | Description | Required | Example |
|-------|-------------|----------|---------|
| dashboard_id | References TILE or DASHBOARD ID | Yes | TILE or DASHBOARD ID |
| dashboard_type | TILE or DASHBOARD | Yes | TILE |
| database_name | Database system | Yes | snowflake |
| text | Complete SQL with CTEs | Yes | WITH view AS (...) SELECT ... |

#### Query Duplication

Every query is written twice in the CSV:

1. Once linked to the **TILE** (the visualization that executes the query)
2. Once linked to the parent **DASHBOARD** (so the dashboard also shows query lineage)

This ensures that query-based lineage appears at both the tile and dashboard level in Coalesce Catalog.

#### Dual Lineage Behavior

**Important**: Coalesce creates two types of lineage from the BI Importer data:

1. **Hierarchical Lineage** (via `parent_dashboards` in dashboards.csv):
   - Shows the structural relationships: `Table → VIZ_MODEL → TILE → DASHBOARD`
   - Represents the logical data flow through the semantic layer

2. **Query Lineage** (via SQL parsing in dashboard_queries.csv):
   - Creates direct connections from tables referenced in SQL to TILEs and DASHBOARDs
   - For example, if a TILE's SQL contains `FROM analytics_db.public.orders`, Coalesce creates a direct line from that table to both the TILE and its parent DASHBOARD

This means you may see both:

- `orders → VIZ_MODEL → TILE → DASHBOARD` (hierarchical path)
- `orders → TILE` (direct query reference)
- `orders → DASHBOARD` (direct query reference)

This dual lineage is intentional and provides complete visibility into both the semantic model structure and the actual query execution patterns.

### dashboard_fields.csv

**Important**: Fields are ONLY supported for VIZ_MODEL entries (per Coalesce documentation)

| Field | Description | Required | Example |
|-------|-------------|----------|---------|
| child_dashboards | DASHBOARD IDs that use this field | No | ['dashboard_id_1','dashboard_id_2'] |
| dashboard_id | References VIZ_MODEL ID | Yes | VIZ_MODEL ID |
| data_type | Field data type | No | TEXT, NUMBER, DATE, BOOLEAN |
| description | Field description | No | Customer identifier |
| external_id | Unique field identifier | Yes | viz_model_id_view_field |
| is_primary_key | Primary key flag | No | False |
| label | Display label | No | Customer Id |
| name | Field name | Yes | customer_id |
| parent_columns | Source columns (traces through CTEs) | No | ['analytics_db.public.users.customer_id'] |
| role | Field role | Yes | DIMENSION or MEASURE |
| view_label | View display label | No | Users |
| view_name | Parent view name | No | users |

### Known Limitations

#### Generic BI Integration Limitations

1. **No field-level lineage for TILEs**: The BI Importer CSV format doesn't support field→TILE relationships - fields can only be linked to VIZ_MODEL entities, not to individual tiles/visualizations. Fields must be defined at the VIZ_MODEL level and the `child_dashboards` field must contain DASHBOARD IDs only.

2. **No custom icons**: Generic BI integration doesn't support custom icons for VIZ_MODEL, TILE, or DASHBOARD entities. All entities use default icons.

3. **No popularity metrics for VIZ_MODEL**: View counts and popularity metrics are only supported for DASHBOARD entities, not for VIZ_MODEL entities.

4. **No popularity metrics for TILEs**: View counts and popularity metrics cannot be tracked for individual TILE entities (visualizations/charts).

5. **No Data Product folders**: The generic visualization format does not support Data Product folder types. Only standard folder paths can be represented in the `folder_path` field.

### Debug Mode

Enable verbose/debug logging:
```bash
# Using make commands
make debug-run

# Using CLI directly
omni-to-catalog --verbose
omni-to-catalog --debug

# Using environment variable
export VERBOSE=true
make run
```

## Additional Resources

- [Coalesce BI Importer Docs](https://docs.coalesce.io/docs/catalog/castor-package/catalog-apis/bi-importer)
- [Omni API Documentation](https://docs.omniapp.co/api)
- [SQLGlot Parser Documentation](https://github.com/tobymao/sqlglot)

## Deploying to Your Org

### Step 1: Fork the repo

Fork this repo to your GitHub org. Your fork will have a `main` branch with the latest stable code. Keep `main` as the default branch — GitHub Actions scheduled triggers always run on the default branch.

> **Note:** GitHub forks of public repositories are always public. Do not commit secrets or sensitive values directly — use GitHub Secrets and Variables instead.

### Step 2: Create a `develop` branch

Create a `develop` branch from `main`. This is where you'll pull in upstream updates and make customizations before promoting to `main`.

### Step 3: Add secrets and variables

Go to **Settings → Secrets and variables → Actions** and add:

- **Secrets**: `OMNI_API_TOKEN`, `COALESCE_API_TOKEN`, `SLACK_WEBHOOK_URL` (optional)
- **Variables**: `OMNI_BASE_URL`, `COALESCE_SOURCE_ID`, `COALESCE_ZONE`

### Step 4: Enable the workflow

On your `develop` branch, edit `.github/workflows/omni-coalesce-sync.yml` and uncomment the triggers you want (schedule, push, or both). See [Enabling Automated Triggers](#enabling-automated-triggers) for details.

When you merge `develop` → `main`, the workflow triggers go with it.

### Step 5: Run the workflow

Go to **Actions → Omni-Coalesce Catalog Sync → Run workflow** to trigger a manual run and verify everything works.

### Pulling in updates

When new versions are released, pull upstream `main` into your `develop` branch:

```bash
git checkout develop
git fetch upstream
git merge upstream/main
```

Test on `develop`, then merge `develop` → `main` when ready to go live.

### Branching

- **This repo**: `main` (default) — stable releases
- **Your fork**:
  - `main` — default branch, runs scheduled and push-triggered workflows
  - `develop` — working branch for pulling upstream updates and making customizations
  - Merge `develop` → `main` to promote changes

## Project Structure

```text
omni_to_catalog/
  __init__.py              # Package exports
  cli.py                   # CLI entry point (omni-to-catalog command)
  orchestrator.py          # Workflow orchestration (extract/generate/upload/tag)
  extractor.py             # Omni API extraction (dashboards, models, queries)
  transformer.py           # CSV generation (dashboards, queries, fields)
  uploader.py              # Coalesce upload via castor-extractor
  slack_notifier.py        # Slack webhook notifications
  table_lineage_parser.py  # SQL/CTE table-level lineage parsing
  field_lineage_parser.py  # SQL/CTE field-level lineage parsing
  table_column_lookup.py   # Model column lookup for lineage resolution
.github/workflows/
  omni-coalesce-sync.yml   # GitHub Actions workflow (3 jobs)
Makefile                   # make run, make setup, make tag, etc.
pyproject.toml             # Python package definition and dependencies
.env.example               # Template for environment variables
.gitignore                 # Excludes .env, local_run_data/, .venv/
README.md                  # This file
```

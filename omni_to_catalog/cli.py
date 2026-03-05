#!/usr/bin/env python3
"""
Command-line interface for Omni-Coalesce sync

This is a thin wrapper around the orchestrator for command-line usage.
"""

import argparse
import logging
import sys
from pathlib import Path

from .orchestrator import WorkflowOrchestrator


def setup_logging(level=logging.INFO):
    """Configure logging for CLI"""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Omni to Coalesce Catalog sync tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full workflow
  omni-to-catalog

  # Run specific steps
  omni-to-catalog --steps extract generate
  omni-to-catalog --steps upload

  # Validate connections only
  omni-to-catalog --steps validate

  # Use different environment file
  omni-to-catalog --env-file .env.staging

  # Dry run (simulate upload)
  omni-to-catalog --dry-run

  # Clean up local data
  omni-to-catalog --cleanup

Environment Variables:
  OMNI_BASE_URL      - Omni API base URL
  OMNI_API_TOKEN     - Omni API authentication token
  COALESCE_API_TOKEN - Coalesce API token
  COALESCE_SOURCE_ID - Coalesce source ID
  COALESCE_ZONE      - Coalesce zone (US or EU)
  VERBOSE            - Enable verbose logging (true/false)
        """
    )

    # Workflow options
    parser.add_argument(
        "--steps",
        nargs="+",
        choices=["validate", "extract", "generate", "upload", "tag"],
        help="Specific workflow steps to run (default: all except validate)"
    )

    # Configuration options
    parser.add_argument(
        "--env-file",
        help="Path to environment file (optional, uses environment variables if not provided)"
    )

    parser.add_argument(
        "--data-dir",
        help="Directory for extracted and generated data (default: ./local_run_data)"
    )

    # Operation options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate upload without actually uploading"
    )

    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Clean up local run data and exit"
    )

    # Logging options
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )

    args = parser.parse_args()

    # Setup logging
    # Check VERBOSE environment variable from .env file
    import os
    verbose_env = os.environ.get('VERBOSE', 'false').lower() == 'true'

    if args.debug:
        setup_logging(logging.DEBUG)
    elif args.verbose or verbose_env:
        setup_logging(logging.INFO)
    else:
        # Default to INFO level for all operations to show progress
        setup_logging(logging.INFO)

    # Initialize orchestrator
    try:
        orchestrator = WorkflowOrchestrator(
            data_dir=args.data_dir,
            env_file=args.env_file
        )
    except FileNotFoundError as e:
        logging.error(str(e))
        if args.env_file:
            logging.info("Copy .env.example to .env and fill in your credentials")
        return 1
    except Exception as e:
        logging.error(f"Failed to initialize orchestrator: {e}")
        return 1

    # Handle cleanup
    if args.cleanup:
        orchestrator.cleanup()
        return 0

    # Run workflow
    try:
        result = orchestrator.run(
            mode='full',
            steps=args.steps,
            dry_run=args.dry_run
        )
        return result
    except KeyboardInterrupt:
        logging.warning("\nWorkflow interrupted by user")
        return 130
    except Exception as e:
        logging.error(f"Workflow failed: {e}")
        if args.debug:
            logging.exception("Full traceback:")
        return 1


if __name__ == "__main__":
    sys.exit(main())
# Makefile for Omni-Coalesce Sync
# Run 'make help' for available commands

.PHONY: help setup install run validate extract transform load clean test tag docker-build docker-run

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

# Default environment file
ENV_FILE ?= .env

# Suppress Python warnings (including urllib3 OpenSSL warning)
export PYTHONWARNINGS=ignore

help: ## Show this help message
	@echo "$(BLUE)Omni-Coalesce Sync - Local Development$(NC)"
	@echo ""
	@echo "$(GREEN)Available commands:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-15s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(GREEN)Examples:$(NC)"
	@echo "  make setup        # Initial setup"
	@echo "  make run          # Run full sync"
	@echo "  make validate     # Test connections"
	@echo "  make extract      # Extract from Omni only"
	@echo "  make generate     # Generate BI Importer CSV files"
	@echo "  make upload       # Upload to Coalesce Catalog"
	@echo ""

setup: ## Initial setup - create .env from example and install dependencies
	@echo "$(BLUE)Setting up local environment...$(NC)"
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "$(GREEN)Created .env file from .env.example$(NC)"; \
		echo "$(YELLOW)Please edit .env with your credentials$(NC)"; \
	else \
		echo "$(GREEN).env file already exists$(NC)"; \
	fi
	@echo "$(BLUE)Creating virtual environment...$(NC)"
	@if [ ! -d .venv ]; then \
		python3 -m venv .venv; \
		echo "$(GREEN)Virtual environment created$(NC)"; \
	else \
		echo "$(GREEN)Virtual environment already exists$(NC)"; \
	fi
	@echo "$(BLUE)Installing package in development mode...$(NC)"
	@. .venv/bin/activate && python -m pip install --upgrade pip > /dev/null 2>&1
	@. .venv/bin/activate && pip install -e .
	@echo "$(GREEN)Setup complete!$(NC)"
	@echo ""
	@echo "$(YELLOW)IMPORTANT: Activate the virtual environment before running:$(NC)"
	@echo "$(GREEN)source .venv/bin/activate$(NC)"
	@echo ""
	@echo "$(YELLOW)Then run:$(NC)"
	@echo "$(GREEN)make validate$(NC) to test connections"
	@echo "$(GREEN)make run$(NC) to run the full sync"

install: ## Install Python package
	@echo "$(BLUE)Installing Python package...$(NC)"
	@if command -v python3 > /dev/null 2>&1; then \
		python3 -m pip install -e .; \
	elif command -v python > /dev/null 2>&1; then \
		python -m pip install -e .; \
	else \
		echo "$(RED)Python is not installed. Please install Python 3.10 or higher$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)Package installed successfully$(NC)"

run: ## Run full sync workflow
	@echo "$(BLUE)Running full sync workflow...$(NC)"
	@if [ -d .venv ]; then \
		. .venv/bin/activate && python -m omni_to_catalog.cli --env-file $(ENV_FILE); \
	else \
		echo "$(YELLOW)Virtual environment not found. Running with system Python...$(NC)"; \
		python3 -m omni_to_catalog.cli --env-file $(ENV_FILE) || python -m omni_to_catalog.cli --env-file $(ENV_FILE); \
	fi

validate: ## Validate API connections
	@echo "$(BLUE)Validating API connections...$(NC)"
	@if [ -d .venv ]; then \
		. .venv/bin/activate && python -m omni_to_catalog.cli --steps validate --env-file $(ENV_FILE); \
	else \
		echo "$(YELLOW)Virtual environment not found. Running with system Python...$(NC)"; \
		python3 -m omni_to_catalog.cli --steps validate --env-file $(ENV_FILE) || python -m omni_to_catalog.cli --steps validate --env-file $(ENV_FILE); \
	fi

extract: ## Extract metadata from Omni API
	@echo "$(BLUE)Extracting Omni metadata...$(NC)"
	@if [ -d .venv ]; then \
		. .venv/bin/activate && python -m omni_to_catalog.cli --steps extract --env-file $(ENV_FILE); \
	else \
		echo "$(YELLOW)Virtual environment not found. Running with system Python...$(NC)"; \
		python3 -m omni_to_catalog.cli --steps extract --env-file $(ENV_FILE) || python -m omni_to_catalog.cli --steps extract --env-file $(ENV_FILE); \
	fi

generate: ## Generate BI Importer CSV files
	@echo "$(BLUE)Generating BI Importer CSV files...$(NC)"
	@if [ -d .venv ]; then \
		. .venv/bin/activate && python -m omni_to_catalog.cli --steps generate --env-file $(ENV_FILE); \
	else \
		echo "$(YELLOW)Virtual environment not found. Running with system Python...$(NC)"; \
		python3 -m omni_to_catalog.cli --steps generate --env-file $(ENV_FILE) || python -m omni_to_catalog.cli --steps generate --env-file $(ENV_FILE); \
	fi

upload: ## Upload BI Importer files to Coalesce Catalog
	@echo "$(BLUE)Uploading to Coalesce Catalog...$(NC)"
	@if [ -d .venv ]; then \
		. .venv/bin/activate && python -m omni_to_catalog.cli --steps upload --env-file $(ENV_FILE); \
	else \
		echo "$(YELLOW)Virtual environment not found. Running with system Python...$(NC)"; \
		python3 -m omni_to_catalog.cli --steps upload --env-file $(ENV_FILE) || python -m omni_to_catalog.cli --steps upload --env-file $(ENV_FILE); \
	fi

tag: ## Sync Omni labels as tags in Coalesce Catalog
	@echo "$(BLUE)Syncing dashboard tags...$(NC)"
	@if [ -d .venv ]; then \
		. .venv/bin/activate && python -m omni_to_catalog.cli --steps tag --env-file $(ENV_FILE); \
	else \
		echo "$(YELLOW)Virtual environment not found. Running with system Python...$(NC)"; \
		python3 -m omni_to_catalog.cli --steps tag --env-file $(ENV_FILE) || python -m omni_to_catalog.cli --steps tag --env-file $(ENV_FILE); \
	fi

clean: ## Clean up local run data
	@echo "$(YELLOW)Cleaning up local data...$(NC)"
	@if [ -d .venv ]; then \
		. .venv/bin/activate && python -m omni_to_catalog.cli --cleanup; \
	else \
		python3 -m omni_to_catalog.cli --cleanup || python -m omni_to_catalog.cli --cleanup; \
	fi
	@echo "$(GREEN)Cleanup complete$(NC)"

test: validate ## Run connection tests
	@echo "$(GREEN)Connection tests passed$(NC)"

# Advanced commands
debug-run: ## Run with debug logging
	@echo "$(BLUE)Running with debug logging...$(NC)"
	python3 -m omni_to_catalog.cli --debug --env-file $(ENV_FILE) || python -m omni_to_catalog.cli --debug --env-file $(ENV_FILE)

watch: ## Watch for changes and auto-sync
	@echo "$(BLUE)Watching for changes...$(NC)"
	@while true; do \
		make run; \
		echo "$(GREEN)Waiting 5 minutes before next sync...$(NC)"; \
		sleep 300; \
	done

# Python virtual environment
.venv: ## Create Python virtual environment
	@echo "$(BLUE)Creating virtual environment...$(NC)"
	@if command -v python3 > /dev/null 2>&1; then \
		python3 -m venv .venv; \
	elif command -v python > /dev/null 2>&1; then \
		python -m venv .venv; \
	else \
		echo "$(RED)Python is not installed. Please install Python 3.10 or higher$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)Virtual environment created$(NC)"
	@echo "$(YELLOW)Activate with: source .venv/bin/activate$(NC)"

.venv-install: .venv ## Create .venv and install package
	@echo "$(BLUE)Installing package in virtual environment...$(NC)"
	. .venv/bin/activate && pip install -e .
	@echo "$(GREEN)Package installed in .venv$(NC)"

# Check environment
check-env: ## Check environment variables
	@echo "$(BLUE)Checking environment variables...$(NC)"
	@if [ -f $(ENV_FILE) ]; then \
		echo "$(GREEN)Environment file: $(ENV_FILE)$(NC)"; \
		echo ""; \
		echo "$(YELLOW)Configured variables:$(NC)"; \
		grep -E '^[A-Z_]+=' $(ENV_FILE) | sed 's/=.*/=***/' | sort; \
	else \
		echo "$(RED)Environment file not found: $(ENV_FILE)$(NC)"; \
		echo "$(YELLOW)Run 'make setup' to create it$(NC)"; \
	fi

# Development helpers
format: ## Format Python code with black
	@echo "$(BLUE)Formatting Python code...$(NC)"
	@python3 -m pip install black > /dev/null 2>&1 || python -m pip install black > /dev/null 2>&1
	@python3 -m black omni_to_catalog/ || black omni_to_catalog/
	@echo "$(GREEN)Code formatted$(NC)"

lint: ## Lint Python code
	@echo "$(BLUE)Linting Python code...$(NC)"
	@python3 -m pip install pylint > /dev/null 2>&1 || python -m pip install pylint > /dev/null 2>&1
	@python3 -m pylint omni_to_catalog/ || pylint omni_to_catalog/ || true
	@echo "$(GREEN)Linting complete$(NC)"

# Git helpers
pre-commit: format lint test ## Run pre-commit checks
	@echo "$(GREEN)Pre-commit checks passed$(NC)"

# Default target
.DEFAULT_GOAL := help
# Makefile for Weather ETL Pipeline
# Provides convenient commands for development, testing, and deployment

.PHONY: help install test lint format clean docker-build docker-run docker-dev docker-test deploy

# Default target
help:
	@echo "Weather ETL Pipeline - Available Commands:"
	@echo ""
	@echo "Development:"
	@echo "  install          Install dependencies"
	@echo "  install-dev      Install development dependencies"
	@echo "  setup            Setup development environment"
	@echo "  clean            Clean temporary files and directories"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint             Run linting checks"
	@echo "  format           Format code with Black"
	@echo "  format-check     Check code formatting"
	@echo ""
	@echo "Testing:"
	@echo "  test             Run unit tests"
	@echo "  test-cov         Run tests with coverage"
	@echo "  test-integration Run integration tests"
	@echo "  test-all         Run all tests"
	@echo ""
	@echo "ETL Pipeline:"
	@echo "  run              Run ETL pipeline (single execution)"
	@echo "  run-scheduled    Run ETL pipeline (scheduled mode)"
	@echo "  run-dev          Run ETL pipeline in development mode"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build     Build Docker image"
	@echo "  docker-run       Run Docker container"
	@echo "  docker-dev       Run development Docker container"
	@echo "  docker-test      Run tests in Docker"
	@echo "  docker-clean     Clean Docker images and containers"
	@echo ""
	@echo "Data Management:"
	@echo "  data-download    Download sample data"
	@echo "  data-clean       Clean processed data"
	@echo "  data-validate    Validate data quality"
	@echo ""
	@echo "Documentation:"
	@echo "  docs             Generate documentation"
	@echo "  notebook         Start Jupyter notebook server"
	@echo ""

# Development Setup
install:
	pip install -r requirements.txt

install-dev: install
	pip install -r requirements-dev.txt

setup: install-dev
	mkdir -p data/raw data/processed data/temp data/backup logs reports
	cp .env.example .env
	@echo "Development environment setup complete!"
	@echo "Edit .env file with your configuration"

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov
	rm -rf dist
	rm -rf build
	rm -rf logs/*
	rm -rf data/processed/*
	rm -rf data/temp/*
	rm -rf reports/*

# Code Quality
lint:
	flake8 src/ tests/ dags/ --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 src/ tests/ dags/ --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

format:
	black src/ tests/ dags/

format-check:
	black --check --diff src/ tests/ dags/

# Testing
test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term-missing

test-integration:
	python dags/etl_pipeline.py --mode single --formats parquet csv
	@echo "Integration test completed successfully!"

test-all: test test-integration

# ETL Pipeline
run:
	python dags/etl_pipeline.py --mode single

run-scheduled:
	python dags/etl_pipeline.py --mode scheduled

run-dev:
	ENV=dev python dags/etl_pipeline.py --mode single --formats parquet csv json

# Docker Commands
docker-build:
	docker build -t weather-etl:latest .

docker-run: docker-build
	docker run --rm -v $(PWD)/data:/app/data -v $(PWD)/logs:/app/logs weather-etl:latest

docker-dev:
	docker-compose --profile dev up --build

docker-test:
	docker-compose --profile test up --build

docker-clean:
	docker system prune -f
	docker image prune -f
	docker volume prune -f

# Data Management
data-download:
	@echo "Downloading sample weather data..."
	@echo "Please ensure you have the weather data file in data/raw/weatherHistory.csv"

data-clean:
	rm -rf data/processed/*
	rm -rf data/temp/*
	rm -rf data/backup/*
	@echo "Data cleaned successfully!"

data-validate:
	python -c "
	from src.extract.extract_data import DataExtractor
	from src.utils.data_quality import DataQualityValidator
	from src.config import config
	import json
	
	# Run data quality analysis
	extractor = DataExtractor()
	df = extractor.read_csv_to_spark()
	validator = DataQualityValidator(extractor.spark)
	results = validator.run_full_validation(df)
	
	# Print summary
	print('Data Quality Validation Results:')
	print(f'Overall Valid: {results[\"overall_valid\"]}')
	print(f'Validations Passed: {results[\"summary\"][\"passed_validations\"]}/{results[\"summary\"][\"total_validations\"]}')
	
	# Save detailed report
	os.makedirs('reports', exist_ok=True)
	with open('reports/data_quality_report.json', 'w') as f:
		json.dump(results, f, indent=2, default=str)
	print('Detailed report saved to reports/data_quality_report.json')
	"

# Documentation
docs:
	@echo "Generating documentation..."
	@echo "Documentation is available in README.md and ARCHITECTURE.md"

notebook:
	jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root

# Production Commands
deploy-staging:
	@echo "Deploying to staging environment..."
	docker-compose -f docker-compose.yml -f docker-compose.staging.yml up -d

deploy-prod:
	@echo "Deploying to production environment..."
	docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# Monitoring
logs:
	tail -f logs/etl_pipeline.log

monitor:
	@echo "ETL Pipeline Monitoring:"
	@echo "========================"
	@echo "Logs: tail -f logs/etl_pipeline.log"
	@echo "Data: ls -la data/processed/"
	@echo "Docker: docker ps"

# Quick Development Workflow
dev: clean setup test run-dev
	@echo "Development workflow completed!"

# CI/CD Simulation
ci: clean lint test-cov test-integration docker-build
	@echo "CI pipeline completed successfully!"

# Full Pipeline Test
full-test: clean setup lint test-cov test-integration docker-build docker-test
	@echo "Full pipeline test completed successfully!"

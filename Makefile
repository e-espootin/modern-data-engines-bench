include .env
export

.PHONY: run build dev act

# set package root path
ROOT_PATH = $(shell pwd)


read_db:
	@echo "Connecting to in-memory DuckDB"
	duckdb --readonly $(ROOT_PATH)/.dbdir/duckdb_results.db -c "select * from duckdb_results.compare_result;"
	duckdb --readonly $(ROOT_PATH)/.dbdir/duckdb_results.db -c "select * from duckdb_results.compare_result_optimized;"
	
cleanup:
	@echo "Cleaning up"
	rm -rf $(ROOT_PATH)/output/*
	rm -rf $(ROOT_PATH)/.dbdir/*

load_sample_dataset:
	mkdir -p $(ROOT_PATH)/data && curl -L -o $(ROOT_PATH)/data/sample-transactions.zip \
  		https://www.kaggle.com/api/v1/datasets/download/ebrahimespootin/sample-transactions \
		&& unzip $(ROOT_PATH)/data/sample-transactions.zip -d $(ROOT_PATH)/data \
		&& rm -rf $(ROOT_PATH)/data/sample-transactions.zip


build:
	@echo "Building the project for workflow purpose"
	uv pip install -r requirements.txt
	cd src && uv run main.py
	

run:
	@echo "Building the project on local"
	uv pip install -r requirements.txt
	cd src && uv run main.py
	cd src && uv run -m streamlit run module_streamlit/report.py
dev:
	@echo "Running dev "
	cd src && uv run main.py

streamlit:
	@echo "Running streamlit"
	cd src && uv run -m streamlit run module_streamlit/report.py

act:
	@echo "build github workflow"
	act

include .env
export

# set package root path
ROOT_PATH = $(shell pwd)

connect_inmemory_duckdb:
	@echo "Connecting to in-memory DuckDB"
	
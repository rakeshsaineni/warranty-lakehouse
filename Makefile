.PHONY: install test lint run dbt all
install:
	pip install -e ".[dev,dbt,spark]"
test:
	pytest
lint:
	ruff check src tests dags
run:
	lakehouse run
dbt:
	cd dbt && LAKEHOUSE_DB=../data/warehouse.duckdb dbt build --profiles-dir .
all: run dbt

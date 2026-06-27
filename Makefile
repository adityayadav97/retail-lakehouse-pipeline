.PHONY: data run-local test clean dbt

data:
	python data/generate_sample_data.py

run-local: data
	python -m src.ingestion.bronze_ingest
	python -m src.transform.silver_transform
	python -m src.transform.gold_aggregate
	@echo "✅ Local lakehouse pipeline complete (bronze → silver → gold)"

dbt:
	cd dbt && dbt build --profiles-dir .

test:
	pytest tests/ -v

clean:
	rm -rf lakehouse/ spark-warehouse/ metastore_db/ derby.log
	rm -rf dbt/target dbt/logs
	@echo "🧹 cleaned generated artifacts"

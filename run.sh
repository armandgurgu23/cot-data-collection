

mkdir -p "data_cache"
poetry run python -m src.main \
    "short-term-rentals-registration" \
    "data_cache" \
    false \
    "data_cache/short-term-rentals-registration_execution_at_2024-03-15_17:57:56_last_updated_at_2024-03-15_12:00:17.parquet"
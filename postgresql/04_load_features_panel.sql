-- load_features_from_csv.sql
COPY analytics.shelf_daily_features
FROM '/import/csv/shelf_daily_features.csv'
CSV HEADER;
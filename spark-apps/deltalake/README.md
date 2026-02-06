# Delta Lake bootstrap

Initializes the Delta Lake directory with empty tables and schemas used by the
streaming jobs.

It also initializes curated ML tables:
- `delta/curated/features_store`
- `delta/curated/predictions`

## Notes
- Simulated time: not used (bootstrap only).

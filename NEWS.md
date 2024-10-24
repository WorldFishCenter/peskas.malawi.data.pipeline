# peskas.malawi.data.pipeline 1.2.0

### New features

Added `export_landings()` function to export the validated landings data to a MongoDB collection dedicated to applications.

### Bug fixes

MongoDB connection was reordering the columns of the dataframes. This was fixed by adding a column order.


# peskas.malawi.data.pipeline 1.1.0

### New features

- The flow of Malawi SSF landings is implemented along the following core functions:
  - `ingest_landings()`: Ingests the raw data from the Malawi SSF landings.
  - `preprocess_landings()`: Preprocesses the raw data from the Malawi SSF landings.
  - `validate_landings()`: Validates the raw data from the Malawi SSF landings.
  
### Breaking changes

- Data are no more stored in GCP bucket as in the previuos Peskas framework, but are now stored in a lighter MongoDB database.


# peskas.malawi.pipeline 1.0.0

* Initial CRAN submission.

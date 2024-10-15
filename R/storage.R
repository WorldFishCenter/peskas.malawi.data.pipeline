#' Retrieve Data from MongoDB
#'
#' This function connects to a MongoDB database and retrieves all documents from a specified collection,
#' maintaining the original column order if available.
#'
#' @param connection_string A character string specifying the MongoDB connection URL. Default is NULL.
#' @param collection_name A character string specifying the name of the collection to query. Default is NULL.
#' @param db_name A character string specifying the name of the database. Default is NULL.
#'
#' @return A data frame containing all documents from the specified collection, with columns ordered
#'         as they were when the data was originally pushed to MongoDB.
#'
#' @keywords storage
#'
#' @examples
#' \dontrun{
#' # Retrieve data from a MongoDB collection
#' result <- mdb_collection_pull(
#'   connection_string = "mongodb://localhost:27017",
#'   collection_name = "my_collection",
#'   db_name = "my_database"
#' )
#' }
#'
#' @export
mdb_collection_pull <- function(connection_string = NULL, collection_name = NULL, db_name = NULL) {
  # Connect to the MongoDB collection
  collection <- mongolite::mongo(collection = collection_name, db = db_name, url = connection_string)

  # Retrieve the metadata document
  metadata <- collection$find(query = '{"type": "metadata"}')

  # Retrieve all data documents
  data <- collection$find(query = '{"type": {"$ne": "metadata"}}')

  if (nrow(metadata) > 0 && "columns" %in% names(metadata)) {
    stored_columns <- metadata$columns[[1]]

    # Ensure all stored columns exist in the data
    for (col in stored_columns) {
      if (!(col %in% names(data))) {
        data[[col]] <- NA
      }
    }

    # Reorder columns to match stored order, and include any extra columns at the end
    data <- data[, c(stored_columns, setdiff(names(data), stored_columns))]
  }

  return(data)
}

#' Upload Data to MongoDB and Overwrite Existing Content
#'
#' This function connects to a MongoDB database, removes all existing documents
#' from a specified collection, and then inserts new data. It also stores the
#' original column order to maintain data structure consistency.
#'
#' @param data A data frame containing the data to be uploaded.
#' @param connection_string A character string specifying the MongoDB connection URL.
#' @param collection_name A character string specifying the name of the collection.
#' @param db_name A character string specifying the name of the database.
#'
#' @return The number of data documents inserted into the collection (excluding the order document).
#'
#' @keywords storage
#'
#' @examples
#' \dontrun{
#' # Upload and overwrite data in a MongoDB collection
#' result <- mdb_collection_push(
#'   data = processed_legacy_landings,
#'   connection_string = "mongodb://localhost:27017",
#'   collection_name = "my_collection",
#'   db_name = "my_database"
#' )
#' }
#'
#' @export
mdb_collection_push <- function(data = NULL, connection_string = NULL, collection_name = NULL, db_name = NULL) {
  # Connect to the MongoDB collection
  collection <- mongolite::mongo(
    collection = collection_name,
    db = db_name,
    url = connection_string
  )

  # Remove all existing documents in the collection
  collection$remove("{}")

  # Create a metadata document with column information
  metadata <- list(
    type = "metadata",
    columns = names(data),
    timestamp = Sys.time()
  )

  # Insert the metadata document first
  collection$insert(metadata)

  # Insert the new data
  collection$insert(data)

  # Return the number of documents in the collection (excluding the metadata document)
  return(collection$count() - 1)
}

#' Get metadata tables
#'
#' Get Metadata tables from Google sheets. This function downloads
#' the tables include information about the fishery.
#'
#' The parameters needed in `conf.yml` are:
#'
#' ```
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' @param log_threshold The logging threshold level. Default is logger::DEBUG.
#'
#' @export
#' @keywords storage
#'
#' @examples
#' \dontrun{
#' # Ensure you have the necessary configuration in conf.yml
#' metadata_tables <- get_metadata()
#' }
get_metadata <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  logger::log_info("Authenticating for google drive")
  googlesheets4::gs4_auth(
    path = conf$storage$google$options$service_account_key,
    use_oob = TRUE
  )
  logger::log_info("Downloading metadata tables")

  tables <-
    conf$metadata$google_sheets$tables %>%
    rlang::set_names() %>%
    purrr::map(~ googlesheets4::range_read(
      ss = conf$metadata$google_sheets$sheet_id,
      sheet = .x,
      col_types = "c"
    ))

  tables
}

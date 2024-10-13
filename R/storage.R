#' Retrieve Data from MongoDB
#'
#' This function connects to a MongoDB database and retrieves all documents from a specified collection.
#'
#' @param connection_string A character string specifying the MongoDB connection URL. Default is NULL.
#' @param collection_name A character string specifying the name of the collection to query. Default is NULL.
#' @param db_name A character string specifying the name of the database. Default is NULL.
#'
#' @return A data frame containing all documents from the specified collection.
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
  data <- mongolite::mongo(collection = collection_name, db = db_name, url = connection_string)
  data$find()
}

#' Upload Data to MongoDB and Overwrite Existing Content
#'
#' This function connects to a MongoDB database, removes all existing documents
#' from a specified collection, and then inserts new data.
#'
#' @param data A data frame containing the data to be uploaded.
#' @param connection_string A character string specifying the MongoDB connection URL.
#' @param collection_name A character string specifying the name of the collection.
#' @param db_name A character string specifying the name of the database.
#'
#' @return The number of documents inserted.
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

  # Insert the new data
  result <- collection$insert(data)

  # Return the number of documents inserted
  return(result)
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

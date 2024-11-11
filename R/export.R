#' Export Selected Landings Data
#'
#' This function retrieves validated landings data from a MongoDB collection,
#' selects specific columns, and uploads the selected data to another MongoDB collection
#' for use in summary applications.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return This function does not return a value. It processes the data and uploads
#'   the selected fields to a MongoDB collection in the app database.
#'
#' @details
#' The function performs the following main operations:
#' 1. Retrieves validated landings data from the validated MongoDB collection.
#' 2. Standardizes the 'gillnets' list column.
#' 3. Selects specific columns from the validated data, including:
#'    - Basic information (form name, submission ID, survey ID, landing date)
#'    - Location data (sample district, landing site, latitude, longitude)
#'    - Fishing details (number of fishers, trip length, gear)
#'    - Catch information (usage, taxon, price, weight, price per kg in USD)
#' 4. Uploads the selected data to the summary MongoDB collection in the app database.
#'
#' @note This function requires a configuration file to be present and readable by the
#'   'read_config' function, which should provide MongoDB connection details and
#'   collection names for both the pipeline and app databases.
#'
#' @examples
#' \dontrun{
#' export_landings()
#' }
#'
#' @keywords workflow export
#' @export
export_landings <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  validated_data <-
    mdb_collection_pull(
      collection_name = conf$storage$mongodb$database$pipeline$collection_name$validated,
      db_name = conf$storage$mongodb$database$pipeline$name,
      connection_string = conf$storage$mongodb$connection_string
    ) |>
    dplyr::as_tibble() %>%
    standardize_list_column("gillnets")


  selected <-
    validated_data %>%
    dplyr::select(
      1:4,
      "sample_district",
      "landing_site",
      "lat",
      "lon",
      "n_fishers",
      "n_boats",
      "trip_length",
      "gear",
      "catch_usage",
      "catch_taxon",
      "catch_price",
      "catch_kg",
      "price_kg"
    )


  # upload validated outputs
  logger::log_info("Uploading selected data to mongodb")
  mdb_collection_push(
    data = selected,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = conf$storage$mongodb$database$app$collection_name$summary,
    db_name = conf$storage$mongodb$database$app$name
  )
}

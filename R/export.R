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

#' Export Matched Vessel Tracks with Landing Data
#'
#' Retrieves merged landing and vessel tracking data from MongoDB, processes track points
#' in parallel, joins with landing data, and exports the combined dataset back to MongoDB.
#' The function handles the entire pipeline from data retrieval to aggregation and storage.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return None. Processes data and uploads results to MongoDB.
#'
#' @details
#' The function performs these main operations:
#' 1. Retrieves validated landings data from MongoDB
#' 2. Extracts unique trip IDs
#' 3. Fetches vessel tracking points in parallel using the PDS API
#' 4. Joins tracking data with landing information
#' 5. Processes and aggregates the combined data:
#'    - Removes unnecessary columns
#'    - Aggregates positions to 10-minute intervals
#'    - Calculates mean positions for each interval
#' 6. Uploads the processed data to MongoDB
#'
#' @section Data Processing:
#' The function aggregates tracking data by:
#' * Rounding timestamps to 10-minute intervals
#' * Computing mean positions (latitude/longitude) for each interval
#' * Grouping by trip, vessel, and catch information
#'
#' @note
#' Requires appropriate MongoDB connection details and PDS API credentials in the
#' configuration file accessed via read_config().
#'
#'
#' @examples
#' \dontrun{
#' # Export tracks with default settings
#' export_matched_tracks()
#'
#' # Export tracks with custom logging level
#' export_matched_tracks(log_threshold = logger::INFO)
#' }
#'
#' @seealso
#' \code{\link{read_config}} for configuration handling
#' \code{\link{get_trip_points}} for PDS API interaction
#'
#' @keywords workflow export
#'
#' @export
export_matched_tracks <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  merged_trips <-
    mdb_collection_pull(
      collection_name = conf$storage$mongodb$database$pipeline$collection_name$merged,
      db_name = conf$storage$mongodb$database$pipeline$name,
      connection_string = conf$storage$mongodb$connection_string
    ) |>
    dplyr::as_tibble()

  matched_trips <-
    merged_trips %>%
    dplyr::pull(.data$Trip) %>%
    unique()

  # Set up parallel processing
  future::plan(future::multisession, workers = future::availableCores() - 1)
  on.exit(future::plan(future::sequential), add = TRUE)

  # Fetch tracks using parallel processing
  tracks <- furrr::future_map_dfr(
    .x = matched_trips,
    .f = get_trip_points,
    token = conf$ingestion$pds$token,
    secret = conf$ingestion$pds$secret
  )

  matched_trips_tracks <-
    merged_trips %>%
    dplyr::select(
      "submission_id", "survey_id", "vessel_type", "gear",
      "catch_taxon", "catch_kg", "Trip"
    ) %>%
    dplyr::left_join(tracks, by = "Trip") %>%
    dplyr::select(-c(
      "Boat", "Speed (M/S)", "Range (Meters)",
      "Heading", "Boat Name", "Community"
    )) %>%
    dplyr::mutate(time = lubridate::floor_date(.data$Time, "10 minutes")) %>%
    dplyr::group_by(
      .data$submission_id, .data$survey_id, .data$Trip, .data$time,
      .data$gear, .data$vessel_type, .data$catch_taxon, .data$catch_kg
    ) %>%
    dplyr::summarise(
      lat = mean(.data$Lat),
      lon = mean(.data$Lng)
    ) %>%
    dplyr::ungroup()

  mdb_collection_push(
    data = matched_trips_tracks,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = conf$storage$mongodb$database$app$collection_name$matched_tracks,
    db_name = conf$storage$mongodb$database$app$name
  )
}

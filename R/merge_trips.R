#' Merge Fishing Trips Data with PDS records
#'
#' @description
#' This function merges fishing trip tracking data with landing records by matching
#' IMEI numbers and landing dates. It only merges records where there is exactly one
#' trip and one landing per day for a given IMEI to ensure accurate matching.
#'
#' @param log_threshold Logging level threshold. Defaults to logger::DEBUG.
#'
#' @details
#' The function performs the following steps:
#' 1. Pulls preprocessed landing data from MongoDB
#' 2. Retrieves deployed device metadata
#' 3. Fetches trip tracking data from PDS (Pelagic Data Systems)
#' 4. Validates IMEI numbers
#' 5. Identifies unique trips per day
#' 6. Merges landing records with tracking data
#' 7. Uploads merged results back to MongoDB
#'
#' @return None (invisible). Results are written directly to MongoDB.
#'
#' @examples
#' \dontrun{
#' merge_trips()
#' merge_trips(log_threshold = logger::INFO)
#' }
#'
#' @section Data Requirements:
#' - MongoDB connection with preprocessed landing data
#' - Valid PDS API credentials in configuration
#' - Deployed device metadata with IMEI numbers
#'
#'
#' @section Warning:
#' This function assumes that trips ending on the same day as landings correspond to
#' each other. This assumption may not hold if multiple trips occur on the same day.
#'
#' @keywords workflow ingestion
#'
#' @export
merge_trips <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  # get raw landings from mongodb
  preprocessed_data <- mdb_collection_pull(
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$preprocessed,
    db_name = conf$storage$mongodb$database$pipeline$name,
    connection_string = conf$storage$mongodb$connection_string
  ) |>
    dplyr::as_tibble() %>%
    standardize_list_column("gillnets")

  deployed_imeis <-
    get_metadata()$devices %>%
    dplyr::rename(tracker_imei = "IMEI")

  trips <- get_trips(
    token = conf$ingestion$pds$token,
    secret = conf$ingestion$pds$secret,
    dateFrom = "2024-01-01",
    dateTo = Sys.Date(),
    imeis = deployed_imeis$tracker_imei,
    deviceInfo = TRUE,
    withLastSeen = FALSE
  ) %>%
    dplyr::rename(imei = "IMEI") %>%
    dplyr::mutate(
      imei = as.character(.data$imei),
      landing_date = lubridate::as_date(.data$Ended, tz = "Africa/Blantyre")
    )

  logger::log_info("Validating IMEIs...")
  imei_alerts <- preprocessed_data$tracker_imei %>%
    rlang::set_names(preprocessed_data$survey_id) %>%
    purrr::imap(
      validate_this_imei,
      deployed_imeis$tracker_imei
    ) %>%
    purrr::map_dfr(tibble::as_tibble) %>%
    dplyr::mutate(survey_id = as.character(.data$survey_id))


  logger::log_info("Idenitfy unique trips per day by imei")
  landings <- preprocessed_data %>%
    dplyr::left_join(imei_alerts, by = "survey_id") %>%
    dplyr::mutate(landing_date = lubridate::as_date(.data$landing_date)) %>%
    dplyr::group_by(.data$landing_date, .data$imei) %>%
    dplyr::mutate(unique_trip_per_day = dplyr::n() == 1) %>%
    split(.$unique_trip_per_day)

  pds <-
    trips %>%
    # We assume the landing date to be the same as the date when the trip ended
    dplyr::mutate(landing_date = lubridate::as_date(.data$Ended)) %>%
    dplyr::group_by(.data$landing_date, .data$imei) %>%
    dplyr::mutate(unique_trip_per_day = dplyr::n() == 1) %>%
    dplyr::ungroup() %>%
    split(.$unique_trip_per_day)

  logger::log_info("Merging datasets...")
  # Only join when we have one landing and one tracking per day, otherwise we
  # cannot do guarantee that the landing corresponds to a trip
  merged_trips <-
    dplyr::full_join(
      landings$`TRUE`,
      pds$`TRUE`,
      by = c("landing_date", "imei", "unique_trip_per_day")
    ) %>%
    dplyr::filter(!is.na(.data$Trip) & !is.na(.data$survey_id)) %>%
    dplyr::mutate(
      Started = lubridate::with_tz(lubridate::as_datetime(.data$Started), tz = "Africa/Blantyre"),
      Ended = lubridate::with_tz(lubridate::as_datetime(.data$Ended), tz = "Africa/Blantyre")
    ) %>%
    dplyr::ungroup()

  logger::log_info("Uploading merged data to mongodb")
  # upload preprocessed landings
  mdb_collection_push(
    data = merged_trips,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$merged_trips,
    db_name = conf$storage$mongodb$database$pipeline$name
  )

}


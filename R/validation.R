#' Validate Fisheries Data
#'
#' This function imports and validates preprocessed fisheries data from a MongoDB collection.
#' It performs a series of validation checks to ensure data integrity, including checks on dates,
#' fisher counts, boat numbers, catch weights, and IMEI numbers. The function then compiles the
#' validated data and corresponding alert flags, which are subsequently uploaded back to MongoDB.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#' @return This function does not return a value. It processes the data and uploads the validated
#'         results to a MongoDB collection in the pipeline database.
#'
#' @details
#' The function performs the following main operations:
#' 1. Pulls preprocessed landings data from the preprocessed MongoDB collection.
#' 2. Validates the data for consistency and accuracy, focusing on:
#'    - Date validation
#'    - Number of fishers
#'    - Number of boats
#'    - Catch weights and prices
#'    - IMEI numbers
#' 3. Generates a validated dataset that integrates the results of the validation checks.
#' 4. Creates alert flags to identify and track any data issues discovered during validation.
#' 5. Merges the validated data with additional metadata, such as survey details and landing site information.
#' 6. Uploads the validated dataset to the validated MongoDB collection.
#'
#' @note This function requires a configuration file to be present and readable by the 'read_config' function,
#'       which should provide MongoDB connection details and parameters for validation.
#'
#' @examples
#' \dontrun{
#' validate_landings()
#' }
#'
#' @keywords workflow validation
#' @export
validate_landings <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  preprocessed_data <-
    mdb_collection_pull(
      collection_name = conf$storage$mongodb$database$pipeline$collection_name$preprocessed,
      db_name = conf$storage$mongodb$database$pipeline$name,
      connection_string = conf$storage$mongodb$connection_string
    ) |>
    dplyr::as_tibble()

  deployed_imeis <-
    get_metadata()$devices %>%
    dplyr::rename(tracker_imei = "IMEI")

  logger::log_info("Validating IMEIs...")
  imei_alerts <- preprocessed_data$tracker_imei %>%
    rlang::set_names(preprocessed_data$survey_id) %>%
    purrr::imap(
      validate_this_imei,
      deployed_imeis$tracker_imei
    ) %>%
    purrr::map_dfr(tibble::as_tibble) %>%
    dplyr::mutate(survey_id = as.character(.data$survey_id))


  validation_output <-
    list(
      dates_alert = validate_dates(data = preprocessed_data),
      fishers_alert = validate_nfishers(data = preprocessed_data, k = conf$validation$k_nfishers),
      nboats_alert = validate_nboats(data = preprocessed_data, k = conf$validation$k_nboats),
      catch_price_alert = validate_pricekg(data = preprocessed_data, k = 5)
    )

  validated_vars <-
    validation_output %>%
    purrr::map(~ dplyr::select(.x, !dplyr::contains("alert"))) %>%
    purrr::reduce(dplyr::left_join, by = c("form_name", "survey_id"))


  # validated data
  validated_data <-
    preprocessed_data %>%
    dplyr::select(-c(names(validated_vars)[3:ncol(validated_vars)])) %>%
    dplyr::left_join(validated_vars, by = c("form_name", "survey_id")) %>%
    dplyr::relocate("n_fishers", "n_boats", .before = "n_women") %>%
    dplyr::relocate("landing_date", .before = "submission_date") %>%
    dplyr::relocate("price_kg_USD", .after = "catch_kg")


  # alerts data
  alert_flags <-
    validation_output %>%
    purrr::map(~ dplyr::select(.x, "form_name", "survey_id", dplyr::contains("alert"))) %>%
    purrr::reduce(dplyr::full_join, by = c("form_name", "survey_id")) %>%
    tidyr::unite(col = "alert_number", dplyr::contains("alert"), sep = "-", na.rm = TRUE)


  # upload validated outputs
  logger::log_info("Uploading validated data to mongodb")
  mdb_collection_push(
    data = validated_data,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$validated,
    db_name = conf$storage$mongodb$database$pipeline$name
  )
}

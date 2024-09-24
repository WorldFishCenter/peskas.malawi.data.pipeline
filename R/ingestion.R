#' Ingest Surveys from Kobotoolbox into MongoDB
#'
#' This function downloads survey data from Kobotoolbox for a specific project, processes it into a tidy format,
#' and uploads the processed data to a MongoDB database. The function logs the progress at various stages.
#' Configuration parameters such as the URL, asset ID, username, and password are read from a configuration file
#' using `read_config()`.
#'
#' @param log_threshold The (standard Apache log4j) log level used as a threshold for the logging infrastructure.
#'   See [logger::log_levels] for more details. Default is `logger::DEBUG`.
#'
#' @keywords workflow
#'
#' @return This function performs side effects (downloads data, processes it, and uploads to MongoDB) and does not return a value.
#'
#' @details
#' The function performs the following steps:
#' \enumerate{
#'   \item Sets the logging threshold to the specified level.
#'   \item Reads configuration parameters using `read_config()`.
#'   \item Logs the start of the data download process.
#'   \item Downloads raw survey data from Kobotoolbox using `KoboconnectR::kobotools_kpi_data()`.
#'   \item Checks for duplicate submission IDs to ensure data integrity.
#'   \item Logs the start of data conversion to a tabular format.
#'   \item Processes the raw data into a tidy tibble using `process_survey()`.
#'   \item Logs the start of the data upload process to MongoDB.
#'   \item Uploads the processed data to a MongoDB collection using `mdb_collection_push()`.
#' }
#'
#' @examples
#' \dontrun{
#' # Ensure that your configuration file is correctly set up with the necessary parameters.
#' ingest_surveys()
#' }
#' @export
ingest_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  logger::log_info("Downloading WCS Fish Catch Survey Kobo data...")
  data_raw <-
    KoboconnectR::kobotools_kpi_data(
      url = "kf.kobotoolbox.org",
      assetid = conf$ingestion$koboform$asset_id,
      uname = conf$ingestion$koboform$username,
      pwd = conf$ingestion$koboform$password,
      encoding = "UTF-8"
    )$results

  # Check that submissions are unique in case there is overlap in the pagination
  if (dplyr::n_distinct(purrr::map_dbl(data_raw, ~ .$`_id`)) != length(data_raw)) {
    stop("Number of submission ids not the same as number of records")
  }

  logger::log_info("Converting catch surveys to tabular format...")
  raw_surveys <- purrr::map_dfr(data_raw, process_survey)

  logger::log_info("Uploading raw data to MongoDB")
  # Upload preprocessed landings
  mdb_collection_push(
    data = raw_surveys,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$raw,
    db_name = conf$storage$mongodb$database$pipeline$name
  )
}

#' Recursively Flatten a Nested List with Concatenated Names
#'
#' The `flatten_with_names` function recursively traverses a nested list and flattens it into a single-level list.
#' The names of the elements in the resulting list are formed by concatenating the names at each level,
#' separated by dots. This is useful for transforming deeply nested data structures into a flat format
#' suitable for data frames or tibbles.
#'
#' @param x A nested list to be flattened.
#' @param parent_name A character string representing the name prefix from the parent recursion level.
#' Defaults to an empty string.
#'
#' @return A named list where each element corresponds to a leaf node in the original nested list,
#' and the names reflect the path to that node.
#'
#' @examples
#' nested_list <- list(
#'   group = list(
#'     subgroup = list(
#'       item1 = 1,
#'       item2 = 2
#'     ),
#'     item3 = 3
#'   ),
#'   item4 = 4
#' )
#' flatten_with_names(nested_list)
#' # Expected output:
#' # $group.subgroup.item1
#' # [1] 1
#' #
#' # $group.subgroup.item2
#' # [1] 2
#' #
#' # $group.item3
#' # [1] 3
#' #
#' # $item4
#' # [1] 4
#'
#' @export
flatten_with_names <- function(x, parent_name = "") {
  if (is.list(x)) {
    if (length(x) == 0) {
      # Return an empty list when x is an empty list
      list()
    } else {
      x %>%
        purrr::imap(~ flatten_with_names(
          .x,
          if (parent_name == "") .y else paste0(parent_name, ".", .y)
        )) %>%
        purrr::reduce(c)
    }
  } else {
    purrr::set_names(list(x), parent_name)
  }
}

#' Process Survey Data into a Tidy Tibble
#'
#' The `process_survey` function processes a survey data structure, which may contain multiple vessels
#' and catches, and converts it into a tidy tibble suitable for analysis and merging with other surveys.
#' It handles the hierarchical nature of the data by flattening nested lists and properly aligning survey-level,
#' vessel-level, and catch-level information.
#'
#' @param x A list representing a single survey, containing survey-level data and nested vessel data.
#'
#' @return A tibble where each row represents a catch from a vessel within the survey, including survey-level data,
#' vessel-level data, and catch-level data. The tibble includes columns `vessel_number` and `catch_number` to identify
#' the vessel and catch within the survey.
#'
#' @examples
#' \dontrun{
#' # Assume 'single_survey' is a survey data list obtained from the API
#' tidy_survey <- process_survey(single_survey)
#' head(tidy_survey)
#'
#' # To process multiple surveys stored in a list 'surveys':
#' all_surveys <- purrr::map_dfr(surveys, process_survey)
#'}
#' @export
process_survey <- function(x) {
  # Flatten the entire survey data
  flattened_x <- flatten_with_names(x)

  # Extract survey-level data (excluding vessel data)
  survey_data <- flattened_x[!grepl("^group_vessel_data", names(flattened_x))]

  # Process vessel data
  vessels <- x$group_vessel_data

  if (is.null(vessels)) {
    # If no vessels, return survey data only
    survey_data %>%
      dplyr::as_tibble()
  } else {
    # Process each vessel and include vessel number
    vessel_dfs <- seq_along(vessels) %>%
      purrr::map_dfr(~ {
        vessel_idx <- .x
        vessel_data <- vessels[[.x]]
        flattened_vessel <- flatten_with_names(vessel_data)
        # Remove catch data from vessel-level data
        flattened_vessel <- flattened_vessel[!grepl("^group_vessel_data.group_catch", names(flattened_vessel))]
        # Add vessel number
        flattened_vessel <- c(flattened_vessel, vessel_number = vessel_idx)

        # Process catches for each vessel
        catches <- vessel_data$`group_vessel_data/group_catch`
        if (is.null(catches)) {
          # If no catches, create a placeholder
          dplyr::tibble(
            fish_species = NA_character_,
            weight_kg = NA_character_,
            weight_type = NA_character_,
            value_species = NA_character_,
            value_type = NA_character_,
            catch_number = NA_integer_
          ) %>%
            dplyr::mutate(vessel_number = vessel_idx) %>%
            dplyr::bind_cols(dplyr::as_tibble(flattened_vessel))
        } else {
          # Process each catch and include catch number
          catch_dfs <- seq_along(catches) %>%
            purrr::map_dfr(~ {
              catch_idx <- .x
              catch <- catches[[.x]]
              flattened_catch <- flatten_with_names(catch)
              # Clean up catch variable names
              names(flattened_catch) <- sub("^group_vessel_data.group_catch.", "", names(flattened_catch))
              # Add catch number
              flattened_catch <- c(flattened_catch, catch_number = catch_idx)
              dplyr::as_tibble(flattened_catch)
            })
          # Repeat vessel data for each catch
          vessel_df <- dplyr::as_tibble(flattened_vessel) %>%
            dplyr::slice(rep(1, nrow(catch_dfs)))
          # Combine vessel data with catches
          dplyr::bind_cols(vessel_df, catch_dfs)
        }
      })
    # Repeat survey data for each row in vessel data
    survey_df <- dplyr::as_tibble(survey_data) %>%
      dplyr::slice(rep(1, nrow(vessel_dfs)))
    # Combine survey data with vessel and catch data
    dplyr::bind_cols(survey_df, vessel_dfs) %>%
      dplyr::rename(survey_id = "_id")
  }
}

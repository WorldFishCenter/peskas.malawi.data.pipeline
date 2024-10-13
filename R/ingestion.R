#' Ingest Survey landings from Kobotoolbox into MongoDB
#'
#' This function downloads survey landings data from Kobotoolbox for multiple assets, processes them into a tidy format,
#' and uploads the processed data to a MongoDB database. The function logs the progress at various stages.
#' Configuration parameters such as the URL, asset IDs, usernames, and passwords are read from a configuration file
#' using `read_config()`.
#'
#' @param log_threshold The (standard Apache log4j) log level used as a threshold for the logging infrastructure.
#'   See [logger::log_levels] for more details. Default is `logger::DEBUG`.
#'
#' @keywords workflow ingestion
#'
#' @return This function performs side effects (downloads data, processes it, and uploads to MongoDB) and does not return a value.
#'
#' @details
#' The function performs the following steps:
#' \enumerate{
#'   \item Sets the logging threshold to the specified level.
#'   \item Reads configuration parameters using `read_config()`.
#'   \item Sets up a parallel processing plan using `future::plan()`.
#'   \item Downloads and processes data for each asset ID using `process_asset()`.
#'   \item Renames specific columns for consistency across different forms.
#'   \item Uploads the processed data to a MongoDB collection using `mdb_collection_push()`.
#' }
#'
#' @examples
#' \dontrun{
#' # Define asset information
#' asset_info <- dplyr::tibble(
#'   asset_id = c("asset1", "asset2"),
#'   form_name = c("Form A", "Form B")
#' )
#'
#' # Ingest survey lnadings
#' ingest_landings()
#' }
#' @export
ingest_landings <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  conf <- read_config()

  mongo_uri <- conf$storage$mongodb$connection_string
  logger::log_debug(paste("MongoDB URI class:", class(mongo_uri)))
  logger::log_debug(paste("MongoDB URI length:", nchar(mongo_uri)))
  logger::log_debug(paste("MongoDB URI prefix:", substr(mongo_uri, 1, 20), "..."))

  asset_info <- dplyr::tibble(
    asset_id = c(
      conf$ingestion$koboform$asset1,
      conf$ingestion$koboform$asset2,
      conf$ingestion$koboform$asset3,
      conf$ingestion$koboform$asset4,
      conf$ingestion$koboform$asset5
    ),
    form_name = c(
      "Malawi SSF",
      "FISHERIES eCAS DATA",
      "FieldDataApp-2024",
      "FieldDataApp-2024A",
      "FieldDataApp-2023F"
    )
  )

  # Set up parallel processing plan
  future::plan("multisession", workers = as.integer(length(future::availableWorkers()) / 2))

  logger::log_info("Processing each asset ID...")

  # Now, process each asset_id and collect results into a list
  raw_surveys_list <- purrr::map2(
    asset_info$asset_id,
    asset_info$form_name,
    ~ {
      data <- process_asset(.x, conf)
      if (!is.null(data)) {
        data$form_name <- .y
      }
      data
    }
  )

  # Name the list elements using form names
  names(raw_surveys_list) <- asset_info$form_name

  # Rename columns for consistency across different forms except "Malawi SSF"
  renamed_raw <-
    raw_surveys_list %>%
    purrr::imap(~ if (.y != "Malawi SSF") {
      .x %>%
        dplyr::rename_with(~ stringr::str_replace(., "vessels/", "group_vessel_data/"))
    } else {
      .x # Keep "Malawi SSF" unchanged
    })

  # Combine all forms into a single tibble
  combined_surveys <- dplyr::bind_rows(renamed_raw, .id = "form_name")

  logger::log_info("Uploading raw data to MongoDB")
  # Debug: Print MongoDB connection string again before uploading
  logger::log_debug(paste("MongoDB URI class before upload:", class(mongo_uri)))
  logger::log_debug(paste("MongoDB URI length before upload:", nchar(mongo_uri)))

  # Upload preprocessed landings
  tryCatch(
    {
      result <- mdb_collection_push(
        data = combined_surveys,
        connection_string = mongo_uri,
        collection_name = conf$storage$mongodb$database$pipeline$collection_name$raw,
        db_name = conf$storage$mongodb$database$pipeline$name
      )
      logger::log_info(paste("Ingestion process completed successfully. Inserted", result$nInserted, "documents."))
    },
    error = function(e) {
      logger::log_error(paste("Error in ingest_landings:", e$message))
      logger::log_debug(paste("Error call:", e$call))
      logger::log_debug("Traceback:")
      logger::log_debug(traceback())
      stop(e)
    }
  )


  logger::log_info("Ingestion process completed successfully.")
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
#' @keywords ingestion
#'
#' @examples
#' \dontrun{
#' # Assume 'single_survey' is a survey data list obtained from the API
#' tidy_survey <- process_survey(single_survey)
#' head(tidy_survey)
#'
#' # To process multiple surveys stored in a list 'surveys':
#' all_surveys <- purrr::map_dfr(surveys, process_survey)
#' }
#' @export
process_survey <- function(x) {
  # Flatten the entire survey data
  flattened_x <- flatten_with_names(x)

  # Replace NULLs with NAs in the flattened data
  flattened_x <- purrr::map(flattened_x, ~ if (is.null(.x)) NA else .x)

  # Extract survey-level data (excluding vessel data)
  survey_data <- flattened_x[!grepl("^(group_vessel_data|vessels)", names(flattened_x))]

  # Replace NULLs with NAs in survey_data
  survey_data <- purrr::map(survey_data, ~ if (is.null(.x)) NA else .x)

  # Detect which key contains vessel data
  vessel_key <- if (!is.null(x$group_vessel_data)) {
    "group_vessel_data"
  } else if (!is.null(x$vessels)) {
    "vessels"
  } else {
    NULL
  }

  if (is.null(vessel_key)) {
    # If no vessels, return survey data only
    return(dplyr::as_tibble(survey_data))
  } else {
    # Get the vessels list
    vessels <- x[[vessel_key]]

    # Process each vessel and include vessel number
    vessel_dfs <- seq_along(vessels) %>%
      purrr::map_dfr(~ {
        vessel_idx <- .x
        vessel_data <- vessels[[.x]]

        # Flatten the vessel data
        flattened_vessel <- flatten_with_names(vessel_data)
        # Replace NULLs with NAs in vessel data
        flattened_vessel <- purrr::map(flattened_vessel, ~ if (is.null(.x)) NA else .x)

        # Remove catch and gillnet data from vessel-level data
        catch_prefix <- if (vessel_key == "group_vessel_data") {
          "group_vessel_data.group_catch"
        } else {
          "vessels.fish_repeat"
        }

        gillnet_prefix <- if (vessel_key == "group_vessel_data") {
          "group_vessel_data.group_gillnets"
        } else {
          "vessels.group_gillnets"
        }

        flattened_vessel <- flattened_vessel[!grepl(paste0("^(", catch_prefix, "|", gillnet_prefix, ")"), names(flattened_vessel))]

        # Add vessel number
        flattened_vessel[["vessel_number"]] <- vessel_idx

        # Process gillnets for each vessel
        gillnets <- if (vessel_key == "group_vessel_data") {
          vessel_data$`group_vessel_data/group_gillnets`
        } else {
          vessel_data$`vessels/group_gillnets`
        }

        if (!is.null(gillnets)) {
          # Process each gillnet and include gillnet number
          gillnet_dfs <- seq_along(gillnets) %>%
            purrr::map_dfr(~ {
              gillnet_idx <- .x
              gillnet <- gillnets[[.x]]
              flattened_gillnet <- flatten_with_names(gillnet)
              # Clean up gillnet variable names
              names(flattened_gillnet) <- sub(".*gillnets.", "", names(flattened_gillnet))
              # Add gillnet number
              flattened_gillnet[["gillnet_number"]] <- gillnet_idx
              dplyr::as_tibble(flattened_gillnet)
            })
          # Assign gillnets data as a nested tibble
          flattened_vessel[["gillnets"]] <- list(gillnet_dfs)
        } else {
          # No gillnets data
          flattened_vessel[["gillnets"]] <- list(NULL)
        }

        # Process catches for each vessel
        catches <- if (vessel_key == "group_vessel_data") {
          vessel_data$`group_vessel_data/group_catch`
        } else {
          vessel_data$`vessels/fish_repeat`
        }

        if (is.null(catches)) {
          # If no catches, create a placeholder
          catch_df <- dplyr::tibble(
            fish_species = NA_character_,
            weight = NA_character_,
            weight_type = NA_character_,
            value_species = NA_character_,
            value_type = NA_character_,
            catch_number = NA_integer_
          )

          # Combine vessel data with placeholder catch
          vessel_df <- dplyr::as_tibble(flattened_vessel)
          dplyr::bind_cols(vessel_df, catch_df)
        } else {
          # Process each catch and include catch number
          catch_dfs <- seq_along(catches) %>%
            purrr::map_dfr(~ {
              catch_idx <- .x
              catch <- catches[[.x]]
              flattened_catch <- flatten_with_names(catch)
              # Replace NULLs with NAs in catch data
              flattened_catch <- purrr::map(flattened_catch, ~ if (is.null(.x)) NA else .x)

              # Clean up catch variable names
              if (vessel_key == "group_vessel_data") {
                names(flattened_catch) <- sub("^group_vessel_data.group_catch.", "", names(flattened_catch))
              } else {
                names(flattened_catch) <- sub("^vessels.fish_repeat.group_species.", "", names(flattened_catch))
              }

              # Add catch number
              flattened_catch[["catch_number"]] <- catch_idx
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
    dplyr::bind_cols(survey_df, vessel_dfs)
  }
}


#' Process Data for a Single Asset ID
#'
#' The `process_asset` function downloads data from Kobotoolbox for a given asset ID, processes each submission
#' using the `process_survey` function, and compiles the results into a single tibble. It handles errors gracefully
#' by using a safe version of the processing function.
#'
#' @param asset_id A character string representing the asset ID to process.
#' @param conf A list containing configuration parameters such as URL, username, and password for Kobotoolbox.
#'
#' @return A tibble containing processed survey data for the specified asset ID, or `NULL` if no data is available.
#'
#' @keywords ingestion
#'
#' @examples
#' \dontrun{
#' # Assuming 'conf' is a configuration list obtained from read_config()
#' asset_data <- process_asset("asset123", conf)
#' }
#' @export
process_asset <- function(asset_id, conf) {
  # Define a safe version of process_survey to handle errors
  safe_process_survey <- purrr::possibly(process_survey, otherwise = NULL)

  # Download data for the asset ID
  data_raw <- KoboconnectR::kobotools_kpi_data(
    url = "kf.kobotoolbox.org",
    assetid = asset_id,
    uname = conf$ingestion$koboform$username,
    pwd = conf$ingestion$koboform$password,
    encoding = "UTF-8"
  )$results

  if (length(data_raw) > 0) {
    # Add asset_id to each submission
    data_raw <- lapply(data_raw, function(submission) {
      submission$asset_id <- asset_id
      submission
    })

    # Process submissions in parallel
    asset_data <- furrr::future_map_dfr(data_raw, function(submission) {
      # Process the survey
      result <- safe_process_survey(submission)
      # Add the asset_id to the result
      if (!is.null(result)) {
        result <- result %>% dplyr::mutate(asset_id = submission$asset_id)
      }
      result
    }) %>%
      dplyr::rename(submission_id = "_id")
  } else {
    asset_data <- NULL
  }
  return(asset_data)
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
#' @keywords ingestion
#'
#' @examples
#' \dontrun{
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
#' }
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
    # Replace NULL values with NA
    if (is.null(x)) x <- NA
    purrr::set_names(list(x), parent_name)
  }
}


#' Retrieve Trip Details from Pelagic Data API
#'
#' This function retrieves trip details from the Pelagic Data API for a specified time range,
#' with options to filter by IMEIs and include additional information.
#'
#' @param token Character string. The API token for authentication.
#' @param secret Character string. The API secret for authentication.
#' @param dateFrom Character string. Start date in 'YYYY-MM-dd' format.
#' @param dateTo Character string. End date in 'YYYY-MM-dd' format.
#' @param imeis Character vector. Optional. Filter by IMEI numbers.
#' @param deviceInfo Logical. If TRUE, include device IMEI and ID fields in the response. Default is FALSE.
#' @param withLastSeen Logical. If TRUE, include device last seen date in the response. Default is FALSE.
#' @param tags Character vector. Optional. Filter by trip tags.
#'
#' @return A data frame containing trip details.
#' @keywords workflow ingestion
#' @examples
#' \dontrun{
#' trips <- get_trips(
#'   token = "your_token",
#'   secret = "your_secret",
#'   dateFrom = "2020-05-01",
#'   dateTo = "2020-05-03",
#'   imeis = c("123456789", "987654321"),
#'   deviceInfo = TRUE,
#'   withLastSeen = TRUE,
#'   tags = c("tag1", "tag2")
#' )
#' }
#'
#' @export
#'
get_trips <- function(token = NULL,
                      secret = NULL,
                      dateFrom = NULL,
                      dateTo = NULL,
                      imeis = NULL,
                      deviceInfo = FALSE,
                      withLastSeen = FALSE,
                      tags = NULL) {
  # Base URL
  base_url <- paste0("https://analytics.pelagicdata.com/api/", token, "/v1/trips/", dateFrom, "/", dateTo)

  # Build query parameters
  query_params <- list()
  if (!is.null(imeis)) {
    query_params$imeis <- paste(imeis, collapse = ",")
  }
  if (deviceInfo) {
    query_params$deviceInfo <- "true"
  }
  if (withLastSeen) {
    query_params$withLastSeen <- "true"
  }
  if (!is.null(tags)) {
    query_params$tags <- paste(tags, collapse = ",")
  }

  # Build the request
  req <- httr2::request(base_url) %>%
    httr2::req_headers(
      "X-API-SECRET" = secret,
      "Content-Type" = "application/json"
    ) %>%
    httr2::req_url_query(!!!query_params)

  # Perform the request
  resp <- req %>% httr2::req_perform()

  # Check for HTTP errors
  if (httr2::resp_status(resp) != 200) {
    stop("Request failed with status: ", httr2::resp_status(resp), "\n", httr2::resp_body_string(resp))
  }

  # Read CSV content
  content_text <- httr2::resp_body_string(resp)
  trips_data <- readr::read_csv(content_text, show_col_types = FALSE)

  return(trips_data)
}



#' Retrieve Trip Points from Pelagic Data API
#'
#' This function retrieves trip points from the Pelagic Data API for a specified time range,
#' with options to filter by IMEIs and include additional information.
#'
#' @param token Character string. The API token for authentication.
#' @param secret Character string. The API secret for authentication.
#' @param dateFrom Character string. Start date in 'YYYY-MM-dd' format.
#' @param dateTo Character string. End date in 'YYYY-MM-dd' format.
#' @param imeis Character vector. Optional. Filter by IMEI numbers.
#' @param deviceInfo Logical. If TRUE, include device IMEI and ID fields in the response. Default is FALSE.
#' @param errant Logical. If TRUE, include errant GPS points in the response. Default is FALSE.
#' @param withLastSeen Logical. If TRUE, include device last seen date in the response. Default is FALSE.
#' @param tags Character vector. Optional. Filter by trip tags.
#'
#' @return A data frame containing trip points data.
#'
#' @keywords workflow ingestion
#' @examples
#' \dontrun{
#' trip_points <- get_trip_points(
#'   token = "your_token",
#'   secret = "your_secret",
#'   dateFrom = "2020-05-01",
#'   dateTo = "2020-05-02",
#'   imeis = c("123456789", "987654321"),
#'   deviceInfo = TRUE,
#'   errant = TRUE,
#'   withLastSeen = TRUE,
#'   tags = c("tag1", "tag2")
#' )
#' }
#'
#' @export
get_trip_points <- function(token = NULL,
                            secret = NULL,
                            dateFrom = NULL,
                            dateTo = NULL,
                            imeis = NULL,
                            deviceInfo = FALSE,
                            errant = FALSE,
                            withLastSeen = FALSE,
                            tags = NULL) {
  # Base URL
  base_url <- paste0("https://analytics.pelagicdata.com/api/", token, "/v1/points/", dateFrom, "/", dateTo)

  # Build query parameters
  query_params <- list()
  if (!is.null(imeis)) {
    query_params$imeis <- paste(imeis, collapse = ",")
  }
  if (deviceInfo) {
    query_params$deviceInfo <- "true"
  }
  if (errant) {
    query_params$errant <- "true"
  }
  if (withLastSeen) {
    query_params$withLastSeen <- "true"
  }
  if (!is.null(tags)) {
    query_params$tags <- paste(tags, collapse = ",")
  }

  # Build the request
  req <- httr2::request(base_url) %>%
    httr2::req_headers(
      "X-API-SECRET" = secret,
      "Content-Type" = "application/json"
    ) %>%
    httr2::req_url_query(!!!query_params)

  # Perform the request
  resp <- req %>% httr2::req_perform()

  # Check for HTTP errors
  if (httr2::resp_status(resp) != 200) {
    stop("Request failed with status: ", httr2::resp_status(resp), "\n", httr2::resp_body_string(resp))
  }

  # Read CSV content
  content_text <- httr2::resp_body_string(resp)
  trip_points_data <- readr::read_csv(content_text, show_col_types = FALSE)

  return(trip_points_data)
}

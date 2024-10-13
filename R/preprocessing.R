#' Preprocess Landings Data
#'
#' This function imports, preprocesses, and cleans legacy landings data from a MongoDB collection.
#' It performs various data cleaning and transformation operations, including column renaming,
#' removal of unnecessary columns, generation of unique identifiers, and data type conversions.
#' The processed data is then uploaded back to MongoDB.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return This function does not return a value. Instead, it processes the data and uploads
#'   the result to a MongoDB collection in the pipeline database.
#'
#' @details
#' The function performs the following main operations:
#' 1. Pulls raw data from the raw data MongoDB collection.
#' 2. Removes unnecessary columns and standardizes list columns.
#' 3. Renames and reorganizes columns for clarity and consistency.
#' 4. Generates unique 'survey_id' field.
#' 5. Merges and consolidates data from various form versions.
#' 6. Processes gear-specific effort data.
#' 7. Handles GPS coordinates.
#' 8. Converts data types and fixes nested column fields.
#' 9. Recode variables and drop trailing spaces to ensure consistency and clarity.
#' 10. Uploads the processed data to the preprocessed MongoDB collection.
#'
#' @note This function requires a configuration file to be present and readable by the
#'   'read_config' function, which should provide MongoDB connection details.
#'
#' @keywords workflow preprocessing
#' @examples
#' \dontrun{
#' preprocess_landings()
#' }
#' @export
preprocess_landings <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  # get raw landings from mongodb
  raw_dat <- mdb_collection_pull(
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$raw,
    db_name = conf$storage$mongodb$database$pipeline$name,
    connection_string = conf$storage$mongodb$connection_string
  ) |>
    dplyr::as_tibble() %>%
    standardize_list_column("gillnets")

  core_data <-
    raw_dat |>
    dplyr::mutate(survey_id = paste(.data$submission_id, .data$vessel_number, .data$catch_number, sep = "-")) |>
    dplyr::rename_with(~ stringr::str_remove(., "group_location/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "group_vessel_data/group_vessel/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "group_vessel_data/group_gear/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "group_vessel_data/group_trade/")) |>
    dplyr::select(-dplyr::starts_with("_"))

  merged_data <-
    core_data %>%
    dplyr::transmute(
      .data$form_name,
      .data$submission_id,
      .data$survey_id,
      landing_date = dplyr::coalesce(.data$landing_date, .data$date_of_landing),
      submission_date = .data$today,
      .data$sample_district,
      landing_site = .data$landing_beach,
      .data$sample_stratum,
      .data$sample_day,
      gps_coordinates = dplyr::coalesce(.data$gps_location, .data$gps_location_001),
      fishing_today = dplyr::coalesce(.data$fishing_today, .data$fishing),
      why_not_fishing = dplyr::coalesce(.data$why_not_fishing, .data$why_not, .data$if_other),
      n_boats = dplyr::coalesce(.data$n_vessels, .data$total_landings),
      .data$vessel_type,
      n_fishers = .data$crew_number,
      n_women = .data$crew_female,
      trip_length = .data$hours_fished,
      tracker_imei = .data$imei_number,
      gear = dplyr::coalesce(.data$gear_type, .data$gear_type_other),
      mesh_size_mm = dplyr::coalesce(.data$gear_mesh_size, .data$gear_mesh_size_mm),
      gear_depth = dplyr::coalesce(.data$gear_depth, .data$gear_depth_m),
      .data$ gillnets,
      n_hauls = .data$num_hauls,
      trader_sex = dplyr::coalesce(.data$trader_sex, .data$buyer_sex, .data$`group_vessel_data/market/buyer_sex`),
      trader_transport_mode = dplyr::coalesce(.data$transport_mode, .data$`group_vessel_data/market/trans`, .data$`group_vessel_data/market/transothers`), # to encode
      food_destination = .data$`group_vessel_data/market/dest`,
      catch_price = .data$value_species,
      catch_price_type = .data$value_type,
      catch_usage = .data$catch_use,
      catch_taxon = .data$fish_species,
      catch_taxon_other = .data$fish_species_other, # to encode
      catch_kg = dplyr::coalesce(.data$weight_kg, .data$weight),
      .data$weight_type
    )

  meffort <-
    core_data %>%
    dplyr::transmute(
      .data$form_name,
      .data$survey_id,
      mosquito_n_sets = dplyr::case_when(
        .data$form_name == "FISHERIES eCAS DATA" ~ .data$mosquito_effort_sets,
        .data$form_name == "FieldDataApp-2024A" ~ NA_character_,
        .data$form_name == "FieldDataApp-2024" ~ NA_character_,
        .data$form_name == "FieldDataApp-2023F" ~ .data$`group_vessel_data/gear_data/mosquito_effort`,
        TRUE ~ NA_character_
      )
    )

  llffort <-
    core_data %>%
    dplyr::transmute(
      .data$form_name,
      .data$survey_id,
      longline_n_hooks = dplyr::case_when(
        .data$form_name == "FISHERIES eCAS DATA" ~ .data$longline_effort_hooks,
        .data$form_name == "FieldDataApp-2024A" ~ NA_character_,
        .data$form_name == "FieldDataApp-2024" ~ .data$`group_vessel_data/gear_data/longline_effort`,
        .data$form_name == "FieldDataApp-2023F" ~ .data$`group_vessel_data/gear_data/longline_effort`,
        TRUE ~ NA_character_
      ),
      longline_n_hrs = dplyr::case_when(
        .data$form_name == "FISHERIES eCAS DATA" ~ .data$longline_effort_hrs,
        .data$form_name == "FieldDataApp-2024A" ~ NA_character_,
        .data$form_name == "FieldDataApp-2024" ~ NA_character_,
        .data$form_name == "FieldDataApp-2023F" ~ NA_character_,
        TRUE ~ NA_character_
      )
    )

  fteffort <-
    core_data %>%
    dplyr::transmute(
      .data$form_name,
      .data$survey_id,
      fishtrap_n_hauls = dplyr::case_when(
        .data$form_name == "FISHERIES eCAS DATA" ~ .data$fish_trap_effort_hauls,
        .data$form_name == "FieldDataApp-2024A" ~ .data$fish_trap_effort,
        .data$form_name == "FieldDataApp-2024" ~ NA_character_,
        .data$form_name == "FieldDataApp-2023F" ~ .data$fish_trap_effort,
        TRUE ~ NA_character_
      )
    )

  hlffort <-
    core_data %>%
    dplyr::transmute(
      .data$form_name,
      .data$survey_id,
      handline_n_hooks = dplyr::case_when(
        .data$form_name == "FISHERIES eCAS DATA" ~ .data$handline_effort_hooks,
        .data$form_name == "FieldDataApp-2024A" ~ NA_character_,
        .data$form_name == "FieldDataApp-2024" ~ NA_character_,
        .data$form_name == "FieldDataApp-2023F" ~ NA_character_,
        TRUE ~ NA_character_
      ),
      handline_n_hrs = dplyr::case_when(
        .data$form_name == "FISHERIES eCAS DATA" ~ .data$handline_effort_hrs,
        .data$form_name == "FieldDataApp-2024A" ~ NA_character_,
        .data$form_name == "FieldDataApp-2024" ~ NA_character_,
        .data$form_name == "FieldDataApp-2023F" ~ .data$`group_vessel_data/gear_data/handline_effort`,
        TRUE ~ NA_character_
      )
    )


  keffort <-
    core_data %>%
    dplyr::transmute(
      .data$form_name,
      .data$survey_id,
      kambuzi_n_sets = dplyr::case_when(
        .data$form_name == "FISHERIES eCAS DATA" ~ .data$kambuzi_effort_sets,
        .data$form_name == "FieldDataApp-2024A" ~ NA_character_,
        .data$form_name == "FieldDataApp-2024" ~ NA_character_,
        .data$form_name == "FieldDataApp-2023F" ~ .data$`group_vessel_data/gear_data/kambuzi_seine_effort`,
        TRUE ~ NA_character_
      )
    )

  ceffort <-
    core_data %>%
    dplyr::transmute(
      .data$form_name,
      .data$survey_id,
      chilimira_n_hauls = dplyr::case_when(
        .data$form_name %in% c("Malawi SSF", "FieldDataApp-2024A") ~ .data$chilimira_hauls,
        .data$form_name == "FISHERIES eCAS DATA" ~ .data$chilimira_effort_hauls,
        TRUE ~ NA_character_
      ),
      chilimira_n_hrs = dplyr::case_when(
        .data$form_name %in% c("FieldDataApp-2024", "FieldDataApp-2023F") ~ .data$chilimira_effort,
        TRUE ~ NA_character_
      )
    )


  ceffort <-
    core_data %>%
    dplyr::transmute(
      .data$form_name,
      .data$survey_id,
      chilimira_n_hauls = dplyr::case_when(
        .data$form_name == "FISHERIES eCAS DATA" ~ .data$chilimira_effort_hauls,
        .data$form_name %in% c("Malawi SSF", "FieldDataApp-2024", "FieldDataApp-2024A", "FieldDataApp-2023F") ~ .data$chilimira_hauls,
        TRUE ~ NA_character_
      ),
      chilmira_n_hrs = dplyr::case_when(
        .data$form_name %in% c("FieldDataApp-2024", "FieldDataApp-2023F") ~ .data$chilimira_effort,
        TRUE ~ NA_character_
      )
    )

  cheffort <-
    core_data %>%
    dplyr::transmute(
      .data$form_name,
      .data$survey_id,
      chikwekwesa_n_sets = dplyr::case_when(
        .data$form_name == "FISHERIES eCAS DATA" ~ NA_character_,
        .data$form_name == "FieldDataApp-2024" ~ .data$chikwekwesa_effort,
        .data$form_name == "FieldDataApp-2023F" ~ .data$chikwekwesa_effort,
        TRUE ~ NA_character_
      ),
      chikwekwesa_length = dplyr::case_when(
        .data$form_name == "FieldDataApp-2024A" ~ .data$chikwekwesa_effort,
        TRUE ~ NA_character_
      )
    )

  weffort <-
    core_data %>%
    dplyr::transmute(
      .data$form_name,
      .data$survey_id,
      wogo_n_sets = dplyr::case_when(
        .data$form_name %in% c("FieldDataApp-2023F") ~ .data$wogo_effort,
        TRUE ~ NA_character_
      )
    )

  oeffort <-
    core_data %>%
    dplyr::transmute(
      .data$form_name,
      .data$survey_id,
      other_n_hrs = dplyr::case_when(
        .data$form_name %in% c("FieldDataApp-2024", "FieldDataApp-2023F") ~ .data$`group_vessel_data/gear_data/other_gear_effort`,
        TRUE ~ NA_character_
      )
    )


  gears <- list(
    merged_data = merged_data %>% dplyr::select("form_name", "submission_id", "survey_id", "gear", "n_hauls"),
    meffort = meffort,
    llffort = llffort,
    fteffort = fteffort,
    hlffort = hlffort,
    keffort = keffort,
    ceffort = ceffort,
    cheffort = cheffort,
    weffort = weffort,
    oeffort = oeffort
  ) %>%
    purrr::reduce(dplyr::full_join, by = c("form_name", "survey_id")) %>%
    dplyr::transmute(.data$form_name, .data$submission_id, .data$survey_id, .data$gear, .data$n_hauls,
      gear_hrs = dplyr::coalesce(!!!dplyr::select(., dplyr::ends_with("n_hrs"))),
      gear_n_sets = dplyr::coalesce(!!!dplyr::select(., dplyr::ends_with("n_sets"))),
      gear_n_hooks = dplyr::coalesce(!!!dplyr::select(., dplyr::ends_with("n_hooks"))),
      gear_n_hauls = dplyr::coalesce(!!!dplyr::select(., dplyr::ends_with("n_hauls"))),
      gear_length = dplyr::coalesce(!!!dplyr::select(., dplyr::ends_with("length")))
    ) %>%
    dplyr::select(-.data$n_hauls)

  final_dataset <-
    dplyr::full_join(merged_data, gears, by = c("form_name", "submission_id", "survey_id", "gear")) %>%
    tidyr::separate(.data$gps_coordinates,
      into = c("lat", "lon", "drop1", "drop2"),
      sep = " "
    ) %>%
    dplyr::select(-c("drop1", "drop2", "n_hauls")) # drop hauls number as already captured in gear data


  # fix fields and column order
  preprocessed_fields <-
    final_dataset %>%
    dplyr::mutate(
      submission_id = as.character(.data$submission_id),
      survey_id = as.character(.data$survey_id),
      landing_date = lubridate::as_datetime(.data$landing_date),
      submission_date = lubridate::as_datetime(.data$submission_date),
      dplyr::across(.cols = c(
        "n_boats",
        "n_fishers",
        "n_women",
        "trip_length",
        "mesh_size_mm",
        "catch_price",
        "catch_kg",
        "lat",
        "lon",
        "gear_depth",
        "gear_hrs",
        "gear_n_hooks",
        "gear_n_sets",
        "gear_length",
        "gear_n_hauls"
      ), ~ as.numeric(.x)),
      price_kg_USD = ifelse(.data$catch_price_type == "total", .data$catch_price / .data$catch_kg, .data$catch_price),
      price_kg_USD = .data$price_kg_USD * 0.0005764
    ) %>%
    dplyr::distinct() %>%
    dplyr::select(
      "form_name":"tracker_imei",
      dplyr::starts_with("gear"),
      "mesh_size_mm",
      "gillnets",
      "trader_sex":"catch_usage",
      "catch_taxon":"weight_type",
      "price_kg_USD"
    ) %>%
    # fix nested column fields (gillnets)
    dplyr::mutate(
      gillnets = purrr::map(
        .data$gillnets,
        ~ if (is.null(.x)) {
          NULL
        } else {
          # Ensure all necessary variables exist
          required_vars <- c("gillnet_mesh", "gillnet_mesh_mm", "gillnet_length", "gillnet_length_m", "gillnet_number", "net_type")
          for (var in required_vars) {
            if (!var %in% names(.x)) {
              .x[[var]] <- NA
            }
          }
          .x %>%
            # Coalesce 'gillnet_mesh_mm' and 'gillnet_mesh'
            dplyr::mutate(
              gillnet_mesh_mm = dplyr::coalesce(gillnet_mesh_mm, gillnet_mesh),
              # Coalesce 'gillnet_length_m' and 'gillnet_length'
              gillnet_length_m = dplyr::coalesce(gillnet_length_m, gillnet_length)
            ) %>%
            # Convert specified columns to numeric
            dplyr::mutate(
              dplyr::across(
                .cols = c("gillnet_length_m", "gillnet_mesh_mm", "gillnet_number"),
                .fns = as.numeric
              ),
              # Ensure 'net_type' is character
              net_type = as.character(net_type)
            ) %>%
            # Remove redundant columns
            dplyr::select(-gillnet_length, -gillnet_mesh)
        }
      )
    )

  # recode variables and drop trailing spaces
  preprocessed_landings <-
    preprocessed_fields %>%
    dplyr::mutate(
      vessel_type = dplyr::case_when(
        .data$vessel_type == "B+E" ~ "motorised boat",
        .data$vessel_type == "B-E" ~ "unmotorised boat",
        .data$vessel_type == "B+E with Dugout Canoe" ~ "motorised dugout canoe",
        .data$vessel_type == "Plunked Canoe" ~ "unmotorised plunked Canoe",
        .data$vessel_type == "B+E with Plank Canoe" ~ "motorised plank canoe",
        .data$vessel_type == "Dugout Canoe" ~ "unmotorised dugout canoe",
        TRUE ~ .data$vessel_type
      ),
      gear = dplyr::case_when(
        .data$gear == "other gear" ~ "other_gear",
        .data$gear == "Chambo Seine (Wogo)" ~ "Chambo Seine",
        TRUE ~ .data$gear
      ),
      trader_transport_mode = dplyr::case_when(
        .data$trader_transport_mode == "1" ~ "bicycle_motorcycle",
        .data$trader_transport_mode == "2" ~ "canoe_boat",
        .data$trader_transport_mode == "3" ~ "motor_vehicle",
        .data$trader_transport_mode == "4" ~ "others",
        TRUE ~ .data$trader_transport_mode
      ),
      why_not_fishing = dplyr::case_when(
        .data$why_not_fishing %in% c("wind other", "other wind") ~ "wind",
        .data$why_not_fishing == "other rain" ~ "rain",
        .data$why_not_fishing == "rain wind" ~ "wind rain",
        .data$why_not_fishing == "rain wind" ~ "wind rain",
        TRUE ~ .data$why_not_fishing
      ),
      catch_taxon = tolower(.data$catch_taxon),
      catch_taxon = dplyr::case_when(
        .data$catch_taxon == "other-tilapia" ~ "other_tilapia",
        .data$catch_taxon == "nocatch" ~ "no_catch",
        TRUE ~ .data$catch_taxon
      ),
      catch_taxon_other = tolower(.data$catch_taxon_other),
      catch_taxon_other = trimws(.data$catch_taxon_other),
      food_destination = tolower(.data$food_destination),
      food_destination = trimws(.data$food_destination)
    )


  logger::log_info("Uploading preprocessed data to mongodb")
  # upload preprocessed landings
  mdb_collection_push(
    data = preprocessed_landings,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$preprocessed,
    db_name = conf$storage$mongodb$database$pipeline$name
  )
}


#' Standardize a List-Column in a Data Frame
#'
#' This function standardizes a specified list-column in a data frame, ensuring that all elements within the column have a consistent structure. It is particularly useful when dealing with data frames containing nested lists or data frames that may have inconsistent structures due to data serialization or deserialization processes.
#'
#' @param data A data frame containing the list-column to standardize.
#' @param column_name A string specifying the name of the list-column to standardize.
#'
#' @details
#' The function iterates over each element of the specified list-column and standardizes its structure as follows:
#' \itemize{
#'   \item If an element is a data frame, it remains unchanged.
#'   \item If an element is an empty list (i.e., \code{list()}), it is converted to \code{NULL}.
#'   \item If an element is a list containing a single data frame, it extracts and returns that data frame.
#'   \item If an element is a list of multiple data frames, it combines them into a single data frame using \code{dplyr::bind_rows()}.
#'   \item If an element is \code{NULL}, it remains \code{NULL}.
#'   \item Any other types of elements are set to \code{NULL}.
#' }
#'
#' This standardization ensures that the list-column can be unnested without errors, facilitating consistent data processing and analysis.
#'
#' @return A data frame with the specified list-column standardized, ready for unnesting.
#'
#' @seealso \code{\link[dplyr]{bind_rows}}, \code{\link[tidyr]{unnest}}
#'
#' @keywords preprocessing
#' @examples
#' \dontrun{
#' # Load necessary libraries
#' library(dplyr)
#' library(tidyr)
#'
#' # Sample data frame with inconsistent 'gillnets' column
#' core_data <- data.frame(
#'   submission_id = c(1, 2, 3),
#'   gillnets = list(
#'     data.frame(gillnet_length = 100, gillnet_mesh = 50), # Data frame
#'     list(data.frame(gillnet_length = 150, gillnet_mesh = 60)), # List containing a data frame
#'     list() # Empty list
#'   ),
#'   stringsAsFactors = FALSE
#' )
#'
#' # Apply the function to standardize the 'gillnets' column
#' core_data <- standardize_list_column(core_data, "gillnets")
#'
#' # Unnest the 'gillnets' column
#' gillnets_data <- core_data %>%
#'   select(submission_id, gillnets) %>%
#'   unnest(gillnets, keep_empty = TRUE)
#'
#' # View the result
#' print(gillnets_data)
#' }
#' @export
standardize_list_column <- function(data, column_name) {
  data[[column_name]] <- lapply(data[[column_name]], function(x) {
    if (is.data.frame(x)) {
      # Keep data frames as they are
      return(x)
    } else if (is.list(x)) {
      if (length(x) == 0) {
        # Convert empty lists to NULL
        return(NULL)
      } else if (length(x) == 1 && is.data.frame(x[[1]])) {
        # Flatten lists containing a single data frame
        return(x[[1]])
      } else if (all(sapply(x, is.data.frame))) {
        # Combine list of data frames into one data frame
        return(dplyr::bind_rows(x))
      } else {
        # Unexpected structure; set to NULL or handle accordingly
        return(NULL)
      }
    } else if (is.null(x)) {
      # Keep NULL values as is
      return(NULL)
    } else {
      # For any other types, set to NULL
      return(NULL)
    }
  })
  return(data)
}

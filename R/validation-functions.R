#' Generate an Alert Vector Based on Outlier Detection
#'
#' This function uses the `univOutl::LocScaleB()` function to detect outliers in a numeric vector
#' and generates an alert vector based on the results.
#'
#' @param x Numeric vector where outliers will be checked
#' @param no_alert_value Value to put in the output when there is no alert (x is within bounds)
#' @param alert_if_larger Alert value for when x is above the bounds found by `univOutl::LocScaleB()`
#' @param alert_if_smaller Alert value for when x is below the bounds found by `univOutl::LocScaleB()`
#' @param ... Additional arguments passed to `univOutl::LocScaleB()`
#'
#' @return A vector of the same length as x, containing alert values or no_alert_value
#'
#' @details
#' The function first checks if the input vector contains only NA or zero values, or if the median
#' absolute deviation is zero. In these cases, it returns NA_real_. Otherwise, it uses
#' `univOutl::LocScaleB()` to determine the bounds and generates alerts accordingly.
#'
#' @keywords validation
#' @export
alert_outlier <- function(x,
                          no_alert_value = NA_real_,
                          alert_if_larger = no_alert_value,
                          alert_if_smaller = no_alert_value,
                          ...) {
  algo_args <- list(...)

  # Helper function to check if everything is NA or zero
  all_na_or_zero <- function(x) {
    isTRUE(all(is.na(x) | x == 0))
  }

  # If everything is NA or zero there is nothing to compute
  if (all_na_or_zero(x)) {
    return(NA_real_)
  }
  # If the median absolute deviation is zero we shouldn't be using this algo
  if (stats::mad(x, na.rm = T) <= 0) {
    return(NA_real_)
  }
  # If weights are specified and they are all NA or zero
  if (!is.null(algo_args$weights)) {
    if (all_na_or_zero(algo_args$weights)) {
      return(NA_real_)
    }
  }

  bounds <- univOutl::LocScaleB(x, ...) %>%
    magrittr::extract2("bounds")

  if (isTRUE(algo_args$logt)) bounds <- exp(bounds) - 1

  dplyr::case_when(
    x < bounds[1] ~ alert_if_smaller,
    x > bounds[2] ~ alert_if_larger,
    TRUE ~ no_alert_value
  )
}


#' Validate Landing Dates
#'
#' This function checks the validity of the `landing_date` in the provided dataset by comparing
#' it to the `submission_date`. If the `landing_date` is after the `submission_date`, an alert
#' is triggered and the `landing_date` is set to NA for those records.
#'
#' @param data A data frame containing the `landing_date` and `submission_date` columns.
#'
#' @return A data frame with three columns:
#'   - `survey_id`: The unique identifier for each survey
#'   - `landing_date`: The original date if valid, otherwise NA
#'   - `alert_date`: A numeric value (1) indicating an invalid date, or NA if the date is valid
#'
#' @keywords validation
#' @export
#'
validate_dates <- function(data = NULL) {
  data %>%
    dplyr::transmute(
      .data$form_name,
      .data$survey_id,
      .data$landing_date,
      .data$submission_date,
      alert_date = ifelse(.data$landing_date > .data$submission_date, 1, NA_real_)
    ) %>%
    dplyr::select(-"submission_date") %>%
    dplyr::mutate(
      landing_date = dplyr::case_when(
        is.na(alert_date) ~ .data$landing_date,
        TRUE ~ as.Date(NA_real_)
      )
    )
}

#' Validate Number of Fishers
#'
#' This function validates the `n_fishers` column in the provided dataset using the `alert_outlier`
#' function to detect outliers. If an outlier is detected, an alert is triggered and the `n_fishers`
#' value is set to NA.
#'
#' @param data A data frame containing the `n_fishers` column.
#' @param k A numeric value used in the LocScaleB function for outlier detection.
#'
#' @return A data frame with three columns:
#'   - `survey_id`: The unique identifier for each survey
#'   - `n_fishers`: The original number of fishers if valid, otherwise NA
#'   - `alert_n_fishers`: A numeric value (2) indicating an outlier, or NA if the value is valid
#'
#' @keywords validation
#' @export
validate_nfishers <- function(data = NULL, k = NULL) {
  data %>%
    dplyr::transmute(
      .data$form_name,
      .data$survey_id,
      .data$n_fishers,
      alert_n_fishers_neg = ifelse(.data$n_fishers < 0, 2, NA_real_),
      n_fishers = ifelse(is.na(.data$alert_n_fishers_neg), .data$n_fishers, NA_real_),
      alert_n_fishers = alert_outlier(
        x = .data$n_fishers,
        alert_if_larger = 2, logt = TRUE, k = k
      ),
      alert_n_fishers = dplyr::coalesce(.data$alert_n_fishers, .data$alert_n_fishers_neg)
    ) %>%
    dplyr::select(-"alert_n_fishers_neg") %>%
    dplyr::mutate(n_fishers = ifelse(is.na(.data$alert_n_fishers), .data$n_fishers, NA_real_))
}

#' Validate Number of Boats
#'
#' This function validates the `n_boats` column in the provided dataset using the `alert_outlier`
#' function to detect outliers. If an outlier is detected, an alert is triggered and the `n_boats`
#' value is set to NA.
#'
#' @param data A data frame containing the `n_boats` column.
#' @param k A numeric value used in the LocScaleB function for outlier detection.
#'
#' @return A data frame with three columns:
#'   - `survey_id`: The unique identifier for each survey
#'   - `n_boats`: The original number of boats if valid, otherwise NA
#'   - `alert_n_boats`: A numeric value (3) indicating an outlier, or NA if the value is valid
#'
#'
#' @keywords validation
#' @export
validate_nboats <- function(data = NULL, k = NULL) {
  data %>%
    dplyr::transmute(
      .data$form_name,
      .data$survey_id,
      alert_n_boats_neg = ifelse(.data$n_boats < 0, 3, NA_real_),
      n_boats = ifelse(is.na(.data$alert_n_boats_neg), .data$n_boats, NA_real_),
      alert_n_boats = alert_outlier(
        x = .data$n_boats,
        alert_if_larger = 3, logt = TRUE, k = k
      ),
      alert_n_boats = dplyr::coalesce(.data$alert_n_boats, .data$alert_n_boats_neg)
    ) %>%
    dplyr::select(-"alert_n_boats_neg") %>%
    dplyr::mutate(n_boats = ifelse(is.na(.data$alert_n_boats), .data$n_boats, NA_real_))
}


#' Get Catch Bounds
#'
#' This function calculates the upper bounds for catch data based on gear type, catch name,
#' and weight type using the LocScaleB function for outlier detection.
#'
#' @param data A data frame containing columns: gear, catch_taxon, weight_type, and catch_kg.
#' @param k A numeric value used in the LocScaleB function for outlier detection.
#'
#' @return A data frame with columns: gear, catch_taxon, weight_type, and upper.up (upper bound).
#'
#' @details
#' The function filters out rows where catch_taxon is "0" or "no_catch". It then splits the data
#' based on the interaction of gear, catch_taxon, and weight_type. The get_bounds() function is
#' applied to each group's catch_kg values. The resulting upper bounds are then combined and
#' transformed back from log scale.
#'
#' @keywords validation
#' @export
get_catch_bounds <- function(data = NULL, k = NULL) {
  data %>%
    dplyr::select("gear", "catch_taxon", "weight_type", "catch_kg") %>%
    dplyr::filter(!.data$catch_taxon == "0", !.data$catch_taxon == "no_catch") %>%
    split(interaction(.$gear, .$catch_taxon, .$weight_type)) %>%
    purrr::discard(~ nrow(.) == 0) %>%
    purrr::map(~ get_bounds(.x$catch_kg, k = k)) %>%
    dplyr::bind_rows(.id = "gear_catch") %>%
    dplyr::mutate(upper.up = exp(.data$upper.up)) %>%
    tidyr::separate_wider_delim(col = "gear_catch", delim = ".", names = c("gear", "catch_taxon", "weight_type")) %>%
    dplyr::distinct() %>%
    dplyr::select(-"lower.low")
}

#' Validate Catch Data
#'
#' This function validates the catch data by comparing it to calculated upper bounds from the
#' get_catch_bounds function. If a catch value exceeds the upper bound, an alert is triggered
#' and the catch value is set to NA.
#'
#' @param data A data frame containing catch data with columns: survey_id, gear, catch_taxon, weight_type, and catch_kg.
#' @param k A numeric value used in the get_catch_bounds function for outlier detection.
#'
#' @return A data frame with columns:
#'   - `form_name`: The name of the form associated with the survey
#'   - `survey_id`: The unique identifier for each survey
#'   - `catch_kg`: The original catch weight if valid, otherwise NA
#'   - `alert_catch`: A numeric value (4) indicating an outlier, or NA if the value is valid
#'
#' @details
#' The function first calculates catch bounds using get_catch_bounds(). It then joins these bounds
#' with the input data and compares each catch_kg value to its corresponding upper bound. If the
#' catch_kg value is greater than or equal to the upper bound, an alert is set and the catch_kg
#' value is changed to NA.
#'
#' @keywords validation
#' @export
validate_catch <- function(data = NULL, k = NULL) {
  bounds <- get_catch_bounds(data = data, k = k)

  data %>%
    dplyr::select("form_name", "survey_id", "gear", "catch_taxon", "weight_type", "catch_kg") %>%
    dplyr::left_join(bounds, by = c("gear", "catch_taxon", "weight_type")) %>%
    dplyr::rowwise() |>
    dplyr::mutate(
      alert_catch = ifelse(.data$catch_kg >= .data$upper.up, 4, NA_real_),
      catch_kg = ifelse(is.na(.data$alert_catch), .data$catch_kg, NA_real_)
    ) %>%
    dplyr::ungroup() |>
    dplyr::select(-c("upper.up", "gear", "catch_taxon", "weight_type"))
}

#' Get Catch Price Bounds
#'
#' This function calculates the upper and lower bounds for catch price data based on catch taxon,
#' using the LocScaleB function for outlier detection.
#'
#' @param data A data frame containing columns: catch_taxon, catch_price_type, and price_kg_USD.
#' @param k A numeric value used in the LocScaleB function for outlier detection.
#'
#' @return A data frame with columns: catch_taxon, lower.low (lower bound), and upper.up (upper bound).
#'
#' @details
#' The function filters out rows where catch_taxon is "0" or "no_catch". It then splits the data
#' based on catch_taxon and applies the get_bounds() function to each group's price_kg_USD values.
#' The resulting bounds are then combined and transformed back from log scale.
#'
#' @keywords validation
#' @export
get_pricekg_bounds <- function(data = NULL, k = NULL) {
  data %>%
    dplyr::select("catch_taxon", "catch_price_type", "price_kg_USD") %>%
    dplyr::filter(!.data$catch_taxon == "0", !.data$catch_taxon == "no_catch") %>%
    split(.$catch_taxon) %>%
    purrr::discard(~ nrow(.) == 0) %>%
    purrr::map(~ get_bounds(.x$price_kg_USD, k = k)) %>%
    dplyr::bind_rows(.id = "catch_taxon") %>%
    dplyr::mutate(
      lower.low = exp(.data$lower.low),
      upper.up = exp(.data$upper.up)
    )
}

#' Validate Catch Price per Kilogram
#'
#' This function validates the price per kilogram of catch data by comparing it to calculated
#' upper and lower bounds from the get_pricekg_bounds function. If a price value is outside
#' the bounds, an alert is triggered and the price, catch weight, and total catch price values
#' are set to NA.
#'
#' @param data A data frame containing catch data with columns: form_name, survey_id, catch_taxon,
#'             price_kg_USD, catch_price, and catch_kg.
#' @param k A numeric value used in the get_pricekg_bounds function for outlier detection.
#'
#' @return A data frame with columns:
#'   - `form_name`: The name of the form associated with the survey
#'   - `survey_id`: The unique identifier for each survey
#'   - `price_kg_USD`: The original price per kg if valid, otherwise NA
#'   - `catch_kg`: The original catch weight if price is valid, otherwise NA
#'   - `catch_price`: The original total catch price if price per kg is valid, otherwise NA
#'   - `alert_price`: A numeric value (4) indicating an outlier, or NA if the value is valid
#'
#' @details
#' The function first calculates price bounds using get_pricekg_bounds(). It then joins these bounds
#' with the input data and compares each price_kg_USD value to its corresponding upper and lower bounds.
#' If the price_kg_USD value is outside the bounds, an alert is set and the price_kg_USD, catch_kg,
#' and catch_price values are changed to NA.
#'
#' @keywords validation
#' @export
validate_pricekg <- function(data = NULL, k = NULL) {
  bounds <- get_pricekg_bounds(data = data, k = k)

  data %>%
    dplyr::select("form_name", "survey_id", "catch_taxon", "price_kg_USD", "catch_price", "catch_kg") %>%
    dplyr::left_join(bounds, by = "catch_taxon") %>%
    dplyr::rowwise() |>
    dplyr::mutate(
      alert_price = ifelse(.data$price_kg_USD >= .data$upper.up, 4, NA_real_),
      alert_price = ifelse(.data$price_kg_USD <= .data$lower.low, 4, NA_real_),
      price_kg_USD = ifelse(is.na(.data$alert_price), .data$price_kg_USD, NA_real_),
      catch_kg = ifelse(is.na(.data$alert_price), .data$catch_kg, NA_real_),
      catch_price = ifelse(is.na(.data$alert_price), .data$catch_price, NA_real_)
    ) %>%
    dplyr::ungroup() |>
    dplyr::select(-c("lower.low", "upper.up", "catch_taxon"))
}

#' Validate IMEI Number
#'
#' This function validates a single IMEI number against a list of valid IMEIs.
#'
#' @param this_imei The IMEI number to validate.
#' @param this_id The survey ID associated with the IMEI (optional).
#' @param valid_imeis A vector of valid IMEI numbers to check against.
#'
#' @return A list with three elements:
#'   - `imei`: The validated IMEI if found, otherwise NA
#'   - `alert_number`: An alert code (1, 2, or 3) if validation fails, otherwise NA
#'   - `survey_id`: The provided survey ID
#'
#' @details
#' Alert codes:
#' - 1: IMEI is too short (less than 5 digits)
#' - 2: Multiple valid IMEIs match the provided IMEI
#' - 3: No valid IMEI matches the provided IMEI
#'
#' @keywords validation
#' @export
validate_this_imei <- function(this_imei, this_id = NULL, valid_imeis) {
  this_id <- as.character(this_id)

  # If imei is NA there is nothing to validate
  if (is.na(this_imei)) {
    out <- list(imei = NA_character_, alert_number = NA_integer_, survey_id = this_id)
    return(out)
  }

  # Zero seems to be used for no IMEI as well
  if (this_imei == "0") {
    out <- list(imei = NA_character_, alert_number = NA_integer_, survey_id = this_id)
    return(out)
  }

  # If the IMEI is negative it was probably a typo
  this_imei <- as.numeric(this_imei)
  if (this_imei < 0) this_imei <- this_imei * -1

  # Optimistically we need at least 5 digits to work with and that might be
  if (this_imei < 9999) {
    out <- list(imei = NA_character_, alert_number = 1, survey_id = this_id)
    return(out)
  }

  # If a valid IMEI is found replace it
  imei_regex <- paste0(as.character(this_imei), "$")
  imei_matches <- stringr::str_detect(valid_imeis, imei_regex)
  n_matches <- sum(imei_matches)
  if (n_matches == 1) {
    list(imei = valid_imeis[imei_matches], alert_number = NA_integer_, survey_id = this_id)
  } else if (n_matches > 1) {
    list(imei = NA_character_, alert_number = 2, survey_id = this_id)
  } else if (n_matches == 0) {
    list(imei = NA_character_, alert_number = 3, survey_id = this_id)
  }
}

#' Get Bounds for Outlier Detection
#'
#' This function calculates the bounds for outlier detection using the LocScaleB method.
#'
#' @param x A numeric vector. The data for which bounds are to be calculated.
#' @param k An optional numeric value. Controls the sensitivity of the outlier detection method.
#'
#' @return A numeric vector of bounds for the input data.
#'
#' @examples
#' get_bounds(c(1, 2, 3, 4, 5), k = 3)
#'
#' @export
get_bounds <- function(x = NULL, k = NULL) {
  univOutl::LocScaleB(x, logt = TRUE, k = k) %>%
    magrittr::extract2("bounds")
}

#' Add timestamp and sha string to a file name
#'
#' An alternative to version data is to name it using the sha (unique
#' identifier) of the code using to generate or process the data and the time at
#' which the data was generated or processed. This function adds this
#' information, a version identifier, to a file name (character string)
#'
#' @param filename Path sans extension of the file to version
#' @param extension Extension of the file
#' @param sha_nchar Number of characters from the SHA to use as the version
#'   identifier
#' @param sep Characters separating the version identifier from the file name
#'
#' @return A character string with the file name and the version identifier
#' @export
#'
#' @details
#'
#' The SHA information is retrieved using [git2r::sha]. If the code is not
#' running in a context aware of a git repository (for example when code is
#' running inside a container) then this function attempts to get the sha from
#' the environment variable `GITHUB_SHA`. If both of these methods fail, no sha
#' versioning is added.
#'
#' @keywords helper
#' @examples
#' if (git2r::in_repository()) {
#'   add_version("my_file", "csv")
#' }
add_version <- function(filename, extension = "", sha_nchar = 7, sep = "__") {
  # Git sha are 40 characters long
  stopifnot(sha_nchar <= 40)

  version <- format(Sys.time(), "%Y%m%d%H%M%S")

  if (git2r::in_repository()) {
    commit_sha <- substr(git2r::sha(git2r::last_commit()), 1, sha_nchar)
    version <- paste(version, commit_sha, sep = "_")
  } else if (Sys.getenv("GITHUB_SHA") != "") {
    # If not in a git repository (for example when code is running inside a
    # container) get the sha from an environment variable if available
    commit_sha <- substr(Sys.getenv("GITHUB_SHA"), 1, sha_nchar)
    version <- paste(version, commit_sha, sep = "_")
  }

  # If the extension comes without dot, add one
  if (nchar(extension) > 0 & substr(extension, 1, 1) != ".") {
    extension <- paste0(".", extension)
  }

  paste0(filename, sep, version, sep, extension)
}


#' Read configuration file
#'
#' Reads configuration file in `config.yml` and adds some logging lines. Wrapped
#' for convenience
#'
#' @return the environment parameters
#'
#' @keywords helper
#' @export
#'
read_config <- function() {
  logger::log_info("Loading configuration file...")
  pars <- config::get(
    config = Sys.getenv("R_CONFIG_ACTIVE", "default"),
    file = system.file("config.yml", package = "peskas.malawi.data.pipeline")
  )
  logger::log_info("Using configuration: {attr(pars, 'config')}")

  # Explicitly set the MongoDB connection string from the environment variable
  pars$storage$mongodb$connection_string <- Sys.getenv("MONGODB_CONNECTION_STRING")

  # Debug information for MongoDB connection string
  logger::log_debug(paste("MongoDB URI class:", class(pars$storage$mongodb$connection_string)))
  logger::log_debug(paste("MongoDB URI length:", nchar(pars$storage$mongodb$connection_string)))
  if (nchar(pars$storage$mongodb$connection_string) > 0) {
    logger::log_debug(paste("MongoDB URI prefix:", substr(pars$storage$mongodb$connection_string, 1, 20), "..."))
  } else {
    logger::log_warn("MongoDB connection string is empty or not set")
  }

  # Ensure MongoDB connection string is set
  if (is.null(pars$storage$mongodb$connection_string) || pars$storage$mongodb$connection_string == "") {
    stop("MongoDB connection string is not set in the configuration or environment variable.")
  }

  logger::log_debug("Running with parameters (sensitive info redacted):")
  redacted_pars <- pars
  redacted_pars$storage$mongodb$connection_string <- "<REDACTED>"
  logger::log_debug(paste(capture.output(str(redacted_pars)), collapse = "\n"))

  pars
}

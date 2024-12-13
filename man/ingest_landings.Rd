% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ingestion.R
\name{ingest_landings}
\alias{ingest_landings}
\title{Ingest Survey landings from Kobotoolbox into MongoDB}
\usage{
ingest_landings(log_threshold = logger::DEBUG)
}
\arguments{
\item{log_threshold}{The (standard Apache log4j) log level used as a threshold for the logging infrastructure.
See \link[logger:log_levels]{logger::log_levels} for more details. Default is \code{logger::DEBUG}.}
}
\value{
This function performs side effects (downloads data, processes it, and uploads to MongoDB) and does not return a value.
}
\description{
This function downloads survey landings data from Kobotoolbox for multiple assets, processes them into a tidy format,
and uploads the processed data to a MongoDB database. The function logs the progress at various stages.
Configuration parameters such as the URL, asset IDs, usernames, and passwords are read from a configuration file
using \code{read_config()}.
}
\details{
The function performs the following steps:
\enumerate{
\item Sets the logging threshold to the specified level.
\item Reads configuration parameters using \code{read_config()}.
\item Sets up a parallel processing plan using \code{future::plan()}.
\item Downloads and processes data for each asset ID using \code{process_asset()}.
\item Renames specific columns for consistency across different forms.
\item Uploads the processed data to a MongoDB collection using \code{mdb_collection_push()}.
}
}
\examples{
\dontrun{
# Define asset information
asset_info <- dplyr::tibble(
  asset_id = c("asset1", "asset2"),
  form_name = c("Form A", "Form B")
)

# Ingest survey lnadings
ingest_landings()
}
}
\keyword{ingestion}
\keyword{workflow}

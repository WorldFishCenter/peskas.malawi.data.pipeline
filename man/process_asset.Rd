% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ingestion.R
\name{process_asset}
\alias{process_asset}
\title{Process Data for a Single Asset ID}
\usage{
process_asset(asset_id, conf)
}
\arguments{
\item{asset_id}{A character string representing the asset ID to process.}

\item{conf}{A list containing configuration parameters such as URL, username, and password for Kobotoolbox.}
}
\value{
A tibble containing processed survey data for the specified asset ID, or \code{NULL} if no data is available.
}
\description{
The \code{process_asset} function downloads data from Kobotoolbox for a given asset ID, processes each submission
using the \code{process_survey} function, and compiles the results into a single tibble. It handles errors gracefully
by using a safe version of the processing function.
}
\examples{
\dontrun{
# Assuming 'conf' is a configuration list obtained from read_config()
asset_data <- process_asset("asset123", conf)
}
}
\keyword{ingestion}

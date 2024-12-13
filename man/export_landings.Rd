% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/export.R
\name{export_landings}
\alias{export_landings}
\title{Export Selected Landings Data}
\usage{
export_landings(log_threshold = logger::DEBUG)
}
\arguments{
\item{log_threshold}{Logging threshold level (default: logger::DEBUG)}
}
\value{
This function does not return a value. It processes the data and uploads
the selected fields to a MongoDB collection in the app database.
}
\description{
This function retrieves validated landings data from a MongoDB collection,
selects specific columns, and uploads the selected data to another MongoDB collection
for use in summary applications.
}
\details{
The function performs the following main operations:
\enumerate{
\item Retrieves validated landings data from the validated MongoDB collection.
\item Standardizes the 'gillnets' list column.
\item Selects specific columns from the validated data, including:
\itemize{
\item Basic information (form name, submission ID, survey ID, landing date)
\item Location data (sample district, landing site, latitude, longitude)
\item Fishing details (number of fishers, trip length, gear)
\item Catch information (usage, taxon, price, weight, price per kg in USD)
}
\item Uploads the selected data to the summary MongoDB collection in the app database.
}
}
\note{
This function requires a configuration file to be present and readable by the
'read_config' function, which should provide MongoDB connection details and
collection names for both the pipeline and app databases.
}
\examples{
\dontrun{
export_landings()
}

}
\keyword{export}
\keyword{workflow}

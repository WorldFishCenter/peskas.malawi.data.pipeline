FROM rocker/r-ver

# Install imports
RUN install2.r --error --skipinstalled \
    config \
    dplyr \
    furrr \
    future \
    httr2 \
    KoboconnectR \
    logger \
    lubridate \
    magrittr \
    mongolite \
    purrr \
    readr \
    rlang \
    stringr \
    tibble \
    tidyr \
    univOutl \
    glue \
    lifecycle \
    remotes

# Install suggests
RUN install2.r --error --skipinstalled \
    git2r \
    googlesheets4

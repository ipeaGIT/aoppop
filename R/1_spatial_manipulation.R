# year <- tar_read(years)[1]
download_census_tracts <- function(year) {
  cli::cli_inform("Baixando setores censitários do ano de {.val {year}}")

  census_tracts <- NULL

  while (is.null(census_tracts)) {
    census_tracts <- geobr::read_census_tract(
      "all",
      year = year,
      simplified = FALSE,
      showProgress = getOption("TARGETS_SHOW_PROGRESS")
    )
  }

  if (year == 2022) {
    census_tracts <- dplyr::mutate(
      census_tracts,
      code_tract = as.character(code_tract)
    )
  }

  census_tracts <- dplyr::select(census_tracts, c(code_tract, geom))

  cli::cli_inform("Corrigindo eventuais problemas topológicos")

  census_tracts <- sf::st_make_valid(census_tracts)

  return(census_tracts)
}

# year <- tar_read(years)[1]
download_statistical_grid <- function(year) {
  if (year == 2010) {
    statistical_grid <- NULL

    while (is.null(statistical_grid)) {
      statistical_grid <- geobr::read_statistical_grid(
        "all",
        year = year,
        showProgress = getOption("TARGETS_SHOW_PROGRESS")
      )
    }

    statistical_grid <- statistical_grid[,
      c("ID_UNICO", "MASC", "FEM", "POP", "DOM_OCU", "geom")
    ]

    return(statistical_grid)
  } else {
    return(NULL)
  }
}

download_urban_concentrations <- function() {
  urban_concentrations <- geobr::read_urban_concentrations(simplified = FALSE)
  urban_concentrations <- sf::st_make_valid(urban_concentrations)
  urban_concentrations <- dplyr::summarize(
    urban_concentrations,
    geom = sf::st_union(geom),
    .by = c(code_urban_concentration, name_urban_concentration)
  )

  return(urban_concentrations)
}

download_pop_arrangements <- function() {
  pop_arrangements <- geobr::read_pop_arrangements(simplified = FALSE)
  pop_arrangements <- sf::st_make_valid(pop_arrangements)
  pop_arrangements <- dplyr::summarize(
    pop_arrangements,
    geom = sf::st_union(geom),
    pop_total_2010 = sum(pop_total_2010),
    .by = c(code_pop_arrangement, name_pop_arrangement)
  )

  return(pop_arrangements)
}

download_immediate_regions <- function() {
  immediate_regions <- geobr::read_immediate_region(
    year = 2017,
    simplified = FALSE
  )
  immediate_regions <- sf::st_make_valid(immediate_regions)

  return(immediate_regions)
}

# concs <- tar_read(urban_concentrations)
# arrangs <- tar_read(pop_arrangements)
merge_pop_units <- function(concs, arrangs) {
  # a population arrangement is a group of 2+ cities in which there is a strong
  # integration of population groups, either due to commuting flows that cross
  # different cities or due to the contiguity of their urban areas. these
  # population arrangements are defined by IBGE.
  #
  # urban concentrations are population arrangements or individual cities with
  # more than 100k residents.
  #
  # not all population arrangements are urban concentrations (those that have
  # less than 100k residents), and not all urban concentrations are population
  # arrangements (those that are composed of only one city).
  #
  # here, we call "population units" (pop_units) the union of all population
  # arrangements and urban concentrations.
  #
  # ps: the population arrangements of Ponta Porã (code 5006606) and Sant'Ana do
  # Livramento (4317103) have less than 100k residents in the dataset, but they
  # have more than 100k when accounting the residents of the cities in other
  # countries (in Paraguai and Uruguai, respectively), so they are actually
  # classified as urban concentrations. we manually remove them from
  # 'small_arrangs' below

  all_concs <- concs
  names(all_concs) <- c("code_pop_unit", "name_pop_unit", "geom")
  all_concs$type <- "urban_concentration"

  small_arrangs <- arrangs[arrangs$pop_total_2010 < 100000, ]
  small_arrangs <- dplyr::select(
    small_arrangs,
    code_pop_unit = code_pop_arrangement,
    name_pop_unit = name_pop_arrangement
  )
  small_arrangs$type <- "population_arrangement"
  small_arrangs <- subset(
    small_arrangs,
    !code_pop_unit %in% c(5006606, 4317103)
  )

  pop_units <- rbind(all_concs, small_arrangs)

  pop_units <- pop_units[order(pop_units$code_pop_unit), ]
  pop_units$treated_name <- treat_name(pop_units)
  pop_units$tar_group <- seq_len(nrow(pop_units))

  return(pop_units)
}

remove_small_crumbs <- function(geom) {
  exploded_geom <- sf::st_cast(geom, "POLYGON")
  areas <- as.numeric(sf::st_area(exploded_geom))
  too_small <- areas < 105332

  filtered_exploded_geom <- exploded_geom[!too_small]
  filtered_geom <- sf::st_union(filtered_exploded_geom)
  filtered_geom_sf <- sf::st_sf(filtered_geom)

  return(filtered_geom_sf)
}

treat_name <- function(pop_units) {
  treated <- tolower(pop_units$name_pop_unit)
  treated <- gsub("\\´", "'", treated)
  treated <- iconv(treated, from = "UTF-8", to = "ASCII//TRANSLIT")

  treated <- gsub("regiao imediata de ", "", treated)

  is_international <- grepl("\\/brasil", treated)
  international <- treated[is_international]
  international <- strsplit(international, " - ")
  brazilian_city <- vapply(
    international,
    FUN.VALUE = character(1),
    FUN = function(x) x[grepl("\\/brasil$", x)]
  )
  brazilian_city <- sub("internacional de ", "", brazilian_city, fixed = TRUE)
  brazilian_city <- sub("\\/brasil", "", brazilian_city)
  treated[is_international] <- brazilian_city

  treated <- gsub("\\/[a-z]{2}$", "", treated)
  treated <- gsub("\\/[a-z]{2} ", " ", treated)

  treated <- gsub("\\/", "_", treated)
  treated <- gsub(" ", "_", treated, fixed = TRUE)

  is_immediate_region <- pop_units$type == "immediate_region"
  treated[is_immediate_region] <- paste0("ri_", treated[is_immediate_region])

  return(treated)
}

# census_tracts <- tar_read(census_tracts)[[1]]
# pop_units <- tar_read(pop_units)
subset_pop_units_tracts <- function(census_tracts, pop_units) {
  # using a slightly larger buffer just to make sure that all census tracts that
  # intersect with a grid cell are included, otherwise we would lose information
  # when reaggregating data from the tracts to the grid
  intersections <- sf::st_intersects(
    census_tracts,
    sf::st_buffer(sf::st_union(pop_units), 3000)
  )
  do_intersect <- lengths(intersections) > 0

  filtered_tracts <- census_tracts[do_intersect, ]

  return(filtered_tracts)
}

# res <- tar_read(h3_res)[1]
# pop_unit <- tar_read(pop_units)[23, ]
create_hex_grid <- function(res, pop_unit) {
  cli::cli_inform(
    "Creating hex grid at {.val res {res}} for {.val {pop_unit$treated_name}}"
  )

  original_crs <- sf::st_crs(pop_unit)

  pop_unit_wgs <- sf::st_transform(pop_unit, 4326)
  pop_urban_cells <- h3o::sfc_to_cells(sf::st_geometry(pop_unit_wgs), res)[[1]]

  pop_urban_grid <- sf::st_sf(
    h3_address = as.character(pop_urban_cells),
    geometry = sf::st_as_sfc(pop_urban_cells)
  )

  pop_urban_grid <- sf::st_transform(pop_urban_grid, original_crs)

  # TODO: REPENSAR ESSE CAMINHO AQUI - TIRAR O 2010 HARDCODED. MAS BOTAR O QUE?
  # 2022 NÃO EXISTE AINDA (NA VERDADE, AS POP_UNITS SÃO DE 2015)

  hex_dir <- file.path(
    Sys.getenv("USERS_DATA_PATH"),
    "Proj_acess_oport/data/acesso_oport_v2/hex_grids_polys",
    glue::glue("res_{res}"),
    "2010"
  )

  if (!dir.exists(hex_dir)) {
    dir.create(hex_dir)
  }

  basename <- glue::glue(
    "{pop_unit$code_pop_unit}_{pop_unit$treated_name}.parquet"
  )
  filepath <- file.path(hex_dir, basename)

  arrow::write_parquet(pop_urban_grid, filepath)

  return(filepath)
}

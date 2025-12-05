download_census_tracts <- function(yyyy) {
  census_tracts <- geobr::read_census_tract(
    "all",
    year = yyyy,
    simplified = FALSE,
    showProgress = getOption("TARGETS_SHOW_PROGRESS")
  )
  
  census_tracts <- sf::st_make_valid(census_tracts)

  return(census_tracts)
}

download_statistical_grid <- function(yyyy) {
  statistical_grid <- geobr::read_statistical_grid(
    "all",
    year = yyyy,
    showProgress = getOption("TARGETS_SHOW_PROGRESS")
  )
  
  statistical_grid <- statistical_grid[
    ,
    c("ID_UNICO", "MASC", "FEM", "POP", "DOM_OCU", "geom")
  ]
  
  return(statistical_grid)
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
    population = sum(pop_total_2010),
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
  
  small_arrangs <- arrangs[arrangs$population < 100000, ]
  small_arrangs <- dplyr::select(
    small_arrangs,
    code_pop_unit = code_pop_arrangement,
    name_pop_unit = name_pop_arrangement
  )
  small_arrangs$type <- "population_arrangement"
  small_arrangs <- subset(
    small_arrangs,
    ! code_pop_unit %in% c(5006606, 4317103)
  )
  
  pop_units <- rbind(all_concs, small_arrangs)
  
  pop_units <- pop_units[order(pop_units$code_pop_unit), ]
  pop_units$treated_name <- treat_name(pop_units)
  pop_units$tar_group <- 1:nrow(pop_units)
  
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
  brazilian_city <- sub("internacional de ", "", brazilian_city)
  brazilian_city <- sub("\\/brasil", "", brazilian_city)
  treated[is_international] <- brazilian_city
  
  treated <- gsub("\\/[a-z]{2}$", "", treated)
  treated <- gsub("\\/[a-z]{2} ", " ", treated)
  
  treated <- gsub("\\/", "_", treated)
  treated <- gsub(" ", "_", treated)
  
  is_immediate_region <- pop_units$type == "immediate_region"
  treated[is_immediate_region] <- paste0("ri_", treated[is_immediate_region])
  
  return(treated)
}

# census_tracts <- tar_read(census_tracts)
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

# res <- tar_read(h3_resolutions)[1]
# pop_unit <- tar_read(pop_units)[1, ]
create_hex_grid <- function(res, pop_unit) {
  original_crs <- sf::st_crs(pop_unit)
  
  pop_unit_wgs <- sf::st_transform(pop_unit, 4326)
  pop_urban_cells <- h3jsr::polygon_to_cells(pop_unit_wgs, res)
  
  # cell_to_polygon() may error if we send too many h3 addresses to it (some of
  # our biggest pop_units may be covered by several thousands or close to 1/2
  # million hexs). so if it errors, we split the cell list in two and merge the
  # outputs
  
  if (res == 9 && as.numeric(sf::st_area(pop_unit)) > 100000000000) {
    skip_first_try <- TRUE
  } else {
    pop_urban_grid <- tryCatch(
      h3jsr::cell_to_polygon(pop_urban_cells, simple = FALSE),
      error = function(cnd) cnd
    )
  }
  
  if (exists("skip_first_try") || inherits(pop_urban_grid, "error")) {
    cells <- pop_urban_cells[[1]]
    n_cells <- lengths(pop_urban_cells)
    
    n_batches <- ceiling(n_cells / 800000)
    
    batch_indices <- if (n_batches == 1) {
      1:n_cells
    } else {
      indices_cuts <- cut(1:n_cells, breaks = n_batches)
      split(1:n_cells, indices_cuts)
    }
    
    cell_groups <- lapply(batch_indices, function(is) cells[is])
    cell_groups_sf <- lapply(
      cell_groups,
      function(hexs) h3jsr::cell_to_polygon(hexs, simple = FALSE)
    )
    
    pop_urban_grid <- do.call(rbind, cell_groups_sf)
  }
  
  pop_urban_grid <- sf::st_transform(pop_urban_grid, original_crs)
  
  hex_dir <- file.path(
    "../../data/acesso_oport_v2/hex_grids_polys",
    paste0("res_", res),
    "2010"
  )
  if (!dir.exists(hex_dir)) dir.create(hex_dir, recursive = TRUE)
  
  basename <- paste0(pop_unit$code_pop_unit, "_", pop_unit$treated_name, ".rds")
  filepath <- file.path(hex_dir, basename)
  
  saveRDS(pop_urban_grid, filepath)
  
  return(filepath)
}
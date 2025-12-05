prepare_census_data <- function(yyyy) {
  # census variables:
  #   - Dom2_V002 -> total residents count
  #   - DomRend_V003 -> total income
  #   - Pess3_V002:V006 -> population count by color/race
  #   - Pess13_V023:Pess13_V134 -> population count by age group
  
  if(yyyy==2010){
    
    dom <- censobr::read_tracts(year = yyyy, dataset = "Domicilio")
    dom <- dplyr::select(
      dom,
      code_tract, code_muni, code_state, domicilio02_V002
    )
    
    dom_renda <- censobr::read_tracts(year = yyyy, dataset = "DomicilioRenda")
    dom_renda <- dplyr::select(dom_renda, c(code_tract, V003))
    
    # FIXME: censobr Pessoa table is missing the tract 250010605000004.
    pess <- censobr::read_tracts(year = yyyy, dataset = "Pessoa")
    pess <- dplyr::select(
      pess,
      code_tract, pessoa03_V002:pessoa03_V006, pessoa13_V023:pessoa13_V134
    )
    
    census_data <- dplyr::left_join(dom, dom_renda, by = "code_tract")
    census_data <- dplyr::left_join(census_data, pess, by = "code_tract")
    
    # I don't think using expressions like this is very dplyr-idiomatic. the
    # more idiomatic way of calculating the totals like this would be to use
    # rowwise(), I suppose, but arrow dplyr queries don't support it
    
    expr <- parse(text = paste(paste0("pessoa13_V0", 23:39), collapse = "+"))
    census_data <- dplyr::mutate(census_data, idade_0a5 = eval(expr))
    
    expr <- parse(text = paste(paste0("pessoa13_V0", 40:48), collapse = "+"))
    census_data <- dplyr::mutate(census_data, idade_6a14 = eval(expr))
    
    expr <- parse(text = paste(paste0("pessoa13_V0", 49:52), collapse = "+"))
    census_data <- dplyr::mutate(census_data, idade_15a18 = eval(expr))
    
    expr <- parse(text = paste(paste0("pessoa13_V0", 53:58), collapse = "+"))
    census_data <- dplyr::mutate(census_data, idade_19a24 = eval(expr))
    
    expr <- parse(text = paste(paste0("pessoa13_V0", 59:73), collapse = "+"))
    census_data <- dplyr::mutate(census_data, idade_25a39 = eval(expr))
    
    expr <- parse(
      text = paste(
        paste0("pessoa13_V", formatC(74:103, width = 3, flag = "0")),
        collapse = "+"
      )
    )
    census_data <- dplyr::mutate(census_data, idade_40a69 = eval(expr))
    
    expr <- parse(text = paste(paste0("pessoa13_V", 104:134), collapse = "+"))
    census_data <- dplyr::mutate(census_data, idade_70mais = eval(expr))
    
    census_data <- dplyr::select(
      census_data,
      cod_setor = code_tract,
      cod_muni = code_muni,
      cod_uf = code_state,
      renda_total = V003,
      moradores_total = domicilio02_V002,
      cor_branca = pessoa03_V002,
      cor_preta = pessoa03_V003,
      cor_amarela = pessoa03_V004,
      cor_parda = pessoa03_V005,
      cor_indigena = pessoa03_V006,
      dplyr::starts_with("idade")
    )
    
    # total population count will be derived from statistical grid data, not from
    # census data. from the census data, we want the proportions of each
    #
    # FIXME: there are tracts with 0 moredores_total and non negligibles
    # age_total and race_total. investigate the difference between these variables
    # again.
    
    cols <- names(census_data)
    age_cols <- cols[grepl("^idade_", cols)]
    race_cols <- cols[grepl("^cor_", cols)]
    
    expr <- parse(text = paste(age_cols, collapse = "+"))
    census_data <- dplyr::mutate(census_data, age_total = eval(expr))
    census_data <- dplyr::mutate(
      census_data,
      dplyr::across(
        dplyr::starts_with("idade"),
        list(prop = ~ . / age_total)
      )
    )
    
    expr <- parse(text = paste(race_cols, collapse = "+"))
    census_data <- dplyr::mutate(census_data, race_total = eval(expr))
    census_data <- dplyr::mutate(
      census_data,
      dplyr::across(
        dplyr::starts_with("cor"),
        list(prop = ~ . / race_total)
      )
    )
    
    census_data <- dplyr::select(
      census_data,
      dplyr::starts_with("cod"),
      renda_total,
      moradores_total,
      age_total,
      race_total,
      dplyr::contains("prop")
    )
    
  }
  
  if(yyyy==2022){
    
    # to do
  }
  
  # data variables may contain NAs (the equivalent of the Xs that appear on IBGE
  # raw tables), which we substitute by 0s. we collect the dataframe because
  # this is easier to do with data.table
  
  census_data <- dplyr::collect(census_data)
  
  int_cols <- setdiff(
    names(census_data),
    c("cod_setor", "cod_municipio", "cod_uf")
  )
  
  for (col in int_cols) {
    census_data[is.na(get(col)), (col) := 0]
  }
  
  # we can remove tracts that don't have any population on them, because they
  # will not affect the aggregation processes from the census tracts to the
  # statistical grid and, therefore, from the grid to the hexagons
  
  census_data <- census_data[
    moradores_total > 0 | age_total > 0 | race_total > 0
  ]
  
  census_data[, c("moradores_total", "age_total", "race_total") := NULL]
  
  return(census_data[])
}

# census_tracts <- tar_read(pop_units_tracts)
# census_data <- tar_read(census_data)
merge_census_tracts_data <- function(census_tracts, census_data) {
  tracts_with_data <- dplyr::left_join(
    census_tracts,
    census_data,
    by = dplyr::join_by(code_tract == cod_setor)
  )
  tracts_with_data <- dplyr::filter(tracts_with_data, !is.na(renda_total))
  
  return(tracts_with_data)
}

# tracts_with_data <- tar_read(tracts_with_data)
# pop_unit <- tar_read(pop_units)[1, ]
filter_tracts_with_data <- function(tracts_with_data, pop_unit) {
  intersections <- sf::st_intersects(
    tracts_with_data,
    sf::st_buffer(pop_unit, 2000)
  )
  do_intersect <- lengths(intersections) > 0
  
  individual_tracts <- tracts_with_data[do_intersect, ]
  
  # some tracts overlap, which create problems when later reaggregating census
  # data to hex grid (population count would be overcounted). we remove these
  # overlapping bits with st_difference(). the problem is that this function may
  # cause some very-hard-to-track topology errors, in the format
  # "TopologyException: side location conflict at x y".
  #
  # in order to deal with this, we assume that the function may throw an error.
  # if it doesn't, great, we use the result. if it does, we use a buffer of 0
  # meters to fix the geometries and then run the difference again.
  #
  # most of the urban concentrations will be fixed by this, but not all. when
  # they are not fixed even by the buffer, we track down the points that cause
  # the problem and manually fix the tracts around them.
  #
  # after all this cropping, the final result may contain some linestrings, so
  # we make sure the output consists only of polygons by using
  # st_collection_extract().
  #
  # FIXME: worry about this warning?
  #   - although coordinates are longitude/latitude, st_difference assumes that
  #     they are planar
  
  individual_tracts_or_error <- tryCatch(
    sf::st_difference(individual_tracts),
    error = function(cnd) cnd
  )
  
  if (inherits(individual_tracts_or_error, "error")) {
    individual_tracts <- sf::st_buffer(individual_tracts, 0)
    
    individual_tracts_or_error <- tryCatch(
      sf::st_difference(individual_tracts),
      error = function(cnd) cnd
    )
  } else {
    individual_tracts <- sf::st_collection_extract(
      individual_tracts_or_error,
      "POLYGON"
    )
    individual_tracts <- sf::st_make_valid(individual_tracts)
    
    return(individual_tracts)
  }
  
  while (inherits(individual_tracts_or_error, "error")) {
    split_error_message <- strsplit(individual_tracts_or_error$message, " ")
    bad_point_x <- as.numeric(split_error_message[[1]][6])
    bad_point_y <- as.numeric(split_error_message[[1]][7])
    bad_point <- sf::st_sfc(
      sf::st_point(c(bad_point_x, bad_point_y)),
      crs = sf::st_crs(individual_tracts)
    )
    
    individual_tracts <- pontual_fix(individual_tracts, bad_point)
    
    individual_tracts_or_error <- tryCatch(
      sf::st_difference(individual_tracts),
      error = function(cnd) cnd
    )
  }
  
  individual_tracts <- sf::st_collection_extract(
    individual_tracts_or_error,
    "POLYGON"
  )
  individual_tracts <- sf::st_make_valid(individual_tracts)
  
  return(individual_tracts)
}

pontual_fix <- function(x, point) {
  buffer_around_point <- sf::st_buffer(point, 100)
  
  intersecting_x <- sf::st_intersects(x, buffer_around_point)
  do_intersect <- lengths(intersecting_x) > 0
  bad_x <- x[do_intersect, ]
  
  fixed_bad_x <- custom_difference(bad_x)
  
  # remove bad polygons from object and bind fixed polygons to it
  
  fixed_x <- rbind(x[!do_intersect, ], fixed_bad_x)
  
  return(fixed_x)
}

custom_difference <- function(x) {
  pb <- progress::progress_bar$new(
    total = nrow(x),
    format = "[:bar] :current/:total (:percent) eta: :eta"
  )
  
  treated_x <- x
  treated_x <- sf::st_make_valid(x)
  
  to_remove <- numeric()
  
  for (i in 1:nrow(x)) {
    to_clip <- treated_x[i, ]
    
    mask <- treated_x[-c(i, to_remove), ]
    
    # suppressed message:
    #   - although coordinates are longitude/latitude, st_overlaps assumes
    #     that they are planar
    
    suppressMessages(overlaps <- sf::st_overlaps(mask, to_clip))
    do_overlap <- lengths(overlaps) > 0
    mask <- mask[do_overlap, ]
    
    if (nrow(mask) > 0) {
      mask <- sf::st_union(mask)
      mask <- sf::st_make_valid(mask)
      
      if (length(mask) > 1) {
        # sometimes (if unioned mask has a linestring), st_make_valid results in
        # an empty geometrycollection, so we union the object again just to make
        # our mask consists of only one geometry)
        mask <- sf::st_union(mask)
        mask <- sf::st_make_valid(mask)
      }
      
      # using dimension = "polygon" comes from this issue:
      # https://github.com/r-spatial/sf/issues/1944
      
      clipped <- sf::st_difference(to_clip, mask, dimensions = "polygon")
      
      if (nrow(clipped) == 0) {
        # for some reason, there are tracts completely covered by others. in
        # this case, we keep the covered tracts in the data and later clip the
        # ones that overlap it
        next
      }
      
      to_remove <- c(to_remove, i)
      
      treated_x <- rbind(treated_x, clipped)
    }
    
    pb$tick()
  }
  
  treated_x <- treated_x[-to_remove, ]
  
  return(treated_x)
}

# stat_grid <- tar_read(statistical_grid_with_pop)
# tracts_with_data <- tar_read(individual_tracts_with_data, branch = 1)[[1]]
filter_individual_stat_grids <- function(stat_grid, tracts_with_data) {
  tracts_with_data <- sf::st_make_valid(tracts_with_data)
  
  # we first subset the statistical grid using st_intersects() because it is way
  # faster than st_intersection() (just using the latter would yield the same
  # result, but would take much longer)
  
  unified_tracts <- sf::st_union(tracts_with_data)
  unified_tracts <- sf::st_make_valid(unified_tracts)
  
  intersections <- sf::st_intersects(stat_grid, unified_tracts)
  do_intersect <- lengths(intersections) > 0
  
  filtered_grid <- stat_grid[do_intersect, ]
  
  # finally, we have to intersect the statistical grid with the census tracts
  # that cover the urban concentrations. that's because we want to consider only
  # the population in the portions of the grid cell that in fact intersect with
  # the tracts. we use the 'tracts_with_data' object to make sure that we're not
  # considering tracts that don't have any population
  # suppressed warning: 
  #  - attribute variables are assumed to be spatially constant throughout all
  #    geometries
  
  filtered_grid$area_before <- as.numeric(sf::st_area(filtered_grid))
  
  suppressWarnings(
    filtered_grid <- sf::st_intersection(filtered_grid, unified_tracts)
  )
  
  filtered_grid$area_after <- as.numeric(sf::st_area(filtered_grid))
  filtered_grid <- dplyr::mutate(
    filtered_grid,
    share_of_orig_area = area_after / area_before
  )
  
  # the intersection operation may result in polygons so small that they become
  # empty geometrycollections after st_make_valid(). we remove them, otherwise
  # they generate NaN values when aggregating data to the statistical grid
  
  filtered_grid <- sf::st_make_valid(filtered_grid)
  filtered_grid <- filtered_grid[!sf::st_is_empty(filtered_grid), ]
  
  filtered_grid <- dplyr::mutate(
    filtered_grid,
    pop_count = POP * share_of_orig_area,
    men = MASC * share_of_orig_area,
    women = FEM * share_of_orig_area,
    occup_hholds = DOM_OCU * share_of_orig_area
  )
  
  filtered_grid <- dplyr::select(
    filtered_grid,
    ID_UNICO, pop_count, men, women, occup_hholds
  )
  
  return(filtered_grid)
}
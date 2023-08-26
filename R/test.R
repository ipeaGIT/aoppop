# tracts_with_data <- tar_read(tracts_with_data)
# urban_conc <- tar_read(individual_urban_concentrations)[1, ]
filter_tracts_with_data <- function(tracts_with_data, urban_conc) {
  tracts_with_data <- sf::st_sf(tracts_with_data)
  
  urban_conc <- sf::st_transform(urban_conc, sf::st_crs(tracts_with_data))
  
  intersections <- sf::st_intersects(
    tracts_with_data,
    sf::st_buffer(urban_conc, 2000)
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
# tracts_with_data <- tar_read(individual_tracts_with_data)[[1]]
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
  # that cover the urban concentrations, otherwise we would not properly count
  # the total population in each intersection between grid cells and tracts when
  # aggregating census data to the statistical grid. we use the 'grid_with_data'
  # object to make sure that we're not considering tracts that don't have any
  # population
  # suppressed warning: 
  #  - attribute variables are assumed to be spatially constant throughout all
  #    geometries
  
  suppressWarnings(
    filtered_grid <- sf::st_intersection(filtered_grid, unified_tracts)
  )
  
  # the intersection operation may result in polygons so small that they become
  # empty geometrycollections after st_make_valid(). we remove them, otherwise
  # they generate NaN values when aggregating data to the statistical grid
  
  filtered_grid <- sf::st_make_valid(filtered_grid)
  filtered_grid <- filtered_grid[!sf::st_is_empty(filtered_grid), ]
  
  return(filtered_grid)
}

# stat_grid <- tar_read(individual_stat_grids)[[1]]
# tracts_with_data <- tar_read(individual_tracts_with_data)[[1]]
aggregate_data_to_stat_grid <- function(stat_grid,
                                        tracts_with_data) {
  cols_to_drop <- c(
    "zone", "code_muni", "name_muni", "name_neighborhood", "code_neighborhood",
    "code_subdistrict", "name_subdistrict", "code_district", "name_district",
    "code_state", "cod_uf", "cod_muni"
  )
  tracts_with_data <- tracts_with_data[
    ,
    setdiff(names(tracts_with_data), cols_to_drop)
  ]
  
  grid_tracts_intersection <- sf::st_intersection(
    stat_grid,
    tracts_with_data
  )
  
  # st make valid changes geometry so we have to calculate grid cell and census
  # tracts areas after making the geometries valid. if we don't, our final
  # population (calculated from the group proportions and by the intersect
  # areas) will be different to the sum of the population in the grid cells
  
  grid_tracts_intersection <- sf::st_make_valid(grid_tracts_intersection)
  grid_tracts_intersection$intersect_area <- as.numeric(
    sf::st_area(grid_tracts_intersection)
  )
  
  data.table::setDT(grid_tracts_intersection)
  
  grid_tracts_intersection[
    ,
    tract_total_area := sum(intersect_area),
    by = code_tract
  ]
  grid_tracts_intersection[
    ,
    grid_total_area := sum(intersect_area),
    by = ID_UNICO
  ]
  
  grid_tracts_intersection[, prop_grid_area := intersect_area / grid_total_area]
  grid_tracts_intersection[
    ,
    prop_tract_area := intersect_area / tract_total_area
  ]
  
  # to calculate the total income in each intersection, we have to calculate the
  # population in each intersection (from the statistical grid population
  # count), calculate the total population count in each tract and then
  # calculate the share of the total tract population count in each
  # intersection. income is distributed according to this share - i.e. we assume
  # that income is evenly distributed among individuals living in the same tract
  
  grid_tracts_intersection[, intersect_population := prop_grid_area * POP]
  grid_tracts_intersection[
    ,
    tract_population := sum(intersect_population),
    by = code_tract
  ]
  grid_tracts_intersection[
    ,
    intersect_income := renda_total * intersect_population / tract_population
  ]
  
  # to calculate the population count by race and age group in each
  # intersection, we just need to multiply the population in each intersection
  # by the proportions of each group, previously calculated
  
  group_prop_cols <- setdiff(
    names(tracts_with_data),
    c("code_tract", "renda_total", "geom", "tract_total_area")
  )
  new_colnames <- paste0("intersect_", sub("_prop", "", group_prop_cols))
  
  grid_tracts_intersection[
    ,
    (new_colnames) := lapply(.SD, function(x) x * intersect_population),
    .SDcols = group_prop_cols
  ]
  
  # the total population count per group and total income in each grid cell is
  # the sum of each of the variables calculated above by grid cell
  
  cols_to_sum <- c("intersect_income", new_colnames)
  final_colnames <- sub("intersect_", "", cols_to_sum)
  
  grid_with_data <- grid_tracts_intersection[
    ,
    append(list(pop_total = POP[1]), lapply(.SD, sum)),
    by = ID_UNICO,
    .SDcols = cols_to_sum
  ]
  
  data.table::setnames(grid_with_data, old = cols_to_sum, new = final_colnames)
  
  data.table::setDT(stat_grid)
  grid_with_data[stat_grid, on = "ID_UNICO", geom := i.geom]
  
  grid_with_data <- sf::st_sf(grid_with_data)
  
  # some sanity checks
  
  if (nrow(grid_with_data) < nrow(stat_grid)) {
    stop("aggregated grid has less cells than raw grid")
  }
  
  total_pop <- sum(grid_with_data$pop_total)
  total_pop_age <- sum(grid_with_data$idade_0a5) +
    sum(grid_with_data$idade_6a14) +
    sum(grid_with_data$idade_15a18) +
    sum(grid_with_data$idade_19a24) +
    sum(grid_with_data$idade_25a39) +
    sum(grid_with_data$idade_40a69) +
    sum(grid_with_data$idade_70mais)
  total_pop_color <- sum(grid_with_data$cor_branca) +
    sum(grid_with_data$cor_preta) +
    sum(grid_with_data$cor_amarela) +
    sum(grid_with_data$cor_parda) +
    sum(grid_with_data$cor_indigena)
  
  if (!dplyr::near(total_pop, total_pop_age)) {
    stop("total pop by age is different than total pop")
    format(total_pop_age, nsmall = 10)
    format(total_pop, nsmall = 10)
  }
  
  if (!dplyr::near(total_pop, total_pop_color)) {
    stop("total pop by age is different than total pop")
    format(total_pop_color, nsmall = 10)
    format(total_pop, nsmall = 10)
  }
  
  return(grid_with_data)
}
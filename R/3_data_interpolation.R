# urban_concentration_tracts <- tar_read(urban_concentration_tracts)
# processed_census_data <- tar_read(processed_census_data)
merge_census_tracts_data <- function(urban_concentration_tracts,
                                     processed_census_data) {
  data.table::setDT(urban_concentration_tracts)
  
  tracts_with_data <- merge(
    urban_concentration_tracts,
    processed_census_data,
    by.x = "code_tract",
    by.y = "cod_setor"
  )
  
  return(tracts_with_data)
}

# tracts_with_data <- tar_read(tracts_with_data)
# stat_grid <- tar_read(urban_concentration_stat_grid)
agg_census_data_to_grid <- function(tracts_with_data, stat_grid) {
  cols_to_drop <- c(
    "zone", "code_muni", "name_muni", "name_neighborhood", "code_neighborhood",
    "code_subdistrict", "name_subdistrict", "code_district", "name_district",
    "code_state", "cod_uf", "cod_muni"
  )
  tracts_with_data[, (cols_to_drop) := NULL]
  
  tracts_with_data <- sf::st_sf(tracts_with_data)
  tracts_with_data$tract_total_area <- as.numeric(sf::st_area(tracts_with_data))
  
  stat_grid$grid_total_area <- as.numeric(sf::st_area(stat_grid))
  
  tictoc::tic()
  grid_tracts_intersection <- parallel_intersection_in_batches(
    stat_grid[1:1000, ],
    tracts_with_data,
    n_batches = 1,
    n_cores_union = min(1, getOption("TARGETS_N_CORES")),
    n_cores_inter = min(2, getOption("TARGETS_N_CORES"))
  )
  tictoc::toc()
  
  grid_tracts_intersection$intersect_area <- as.numeric(
    sf::st_area(grid_tracts_intersection)
  )
  
  data.table::setDT(grid_tracts_intersection)
  grid_tracts_intersection[, prop_grid_area := intersect_area / grid_total_area]
  grid_tracts_intersection[
    ,
    prop_tract_area := intersect_area / tract_total_area]
  
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
  # obs: grid_with_data may contain less grid cells than stat_grid. that's because
  # we filter the census tracts before (in the processed_census_data target) to
  # only include tracts with residents. so we may have some grid cells that fall
  # within these tracts and are consequently left off from the intersection
  # output
  
  cols_to_sum <- c("intersect_income", new_colnames)
  final_colnames <- sub("intersect_", "", cols_to_sum)
  
  grid_with_data <- grid_tracts_intersection[
    ,
    append(list(pop_total = POP[1]), lapply(.SD, sum)),
    by = ID_UNICO,
    .SDcols = cols_to_sum
  ]
  
  data.table::setnames(grid_with_data, old = cols_to_sum, new = final_colnames)

}

parallel_intersection_in_batches <- function(x,
                                             y,
                                             n_batches,
                                             n_cores_union,
                                             n_cores_inter) {
  if (n_batches == 1) {
    batch_indices = list(1:nrow(x))
  } else {
    indices_cuts <- cut(1:nrow(x), breaks = n_batches)
    batch_indices <- split(1:nrow(x), indices_cuts)
  }
  
  intersections_list <- lapply(
    batch_indices,
    function(is) {
      unified_x <- parallel_union(x[is, ], n_cores_union)
      
      intersecting_y <- sf::st_intersects(y, unified_x)
      filtered_y <- y[lengths(intersecting_y) > 0, ]

      parallel_intersection(x[is, ], filtered_y, n_cores_inter)
    }
  )
  
  intersected_obj <- do.call(rbind, intersections_list)
  intersected_obj <- sf::st_make_valid(intersected_obj)
  
  return(intersected_obj)
}
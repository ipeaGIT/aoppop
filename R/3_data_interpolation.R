# stat_grid <- tar_read(individual_stat_grids)[[1]]
# tracts_with_data <- tar_read(individual_tracts_with_data)[[1]]
# manual_parallelization <- TRUE
aggregate_data_to_stat_grid <- function(stat_grid,
                                        tracts_with_data,
                                        manual_parallelization) {
  cols_to_drop <- c(
    "zone", "code_muni", "name_muni", "name_neighborhood", "code_neighborhood",
    "code_subdistrict", "name_subdistrict", "code_district", "name_district",
    "code_state", "cod_uf", "cod_muni"
  )
  tracts_with_data <- tracts_with_data[
    ,
    setdiff(names(tracts_with_data), cols_to_drop)
  ]
  
  grid_tracts_intersection <- if (manual_parallelization) {
    parallel_intersection(
      stat_grid,
      tracts_with_data,
      n_cores = getOption("TARGETS_N_CORES")
    )
  } else {
    sf::st_intersection(stat_grid, tracts_with_data)
  }
  
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

# large_grids <- tar_read(large_stat_grids_with_data)
# small_grids <- tar_read(small_stat_grids_with_data)
# large_grids_indices <- tar_read(large_stat_grids_indices)
bind_stat_grids <- function(large_grids, small_grids, large_grids_indices) {
  # we want to make a larger list of individual statistical grids, in which the
  # large grids are inserted exactly where they were before being separated
  
  all_grids <- small_grids
  
  for (i in seq.int(1, length(large_grids_indices))) {
    all_grids <- append(
      all_grids,
      list(large_grids[[i]]),
      after = large_grids_indices[i] - 1
    )
  }
  
  names(all_grids)[large_grids_indices] <- names(large_grids)
  
  # sanity check
  if (!identical(all_grids[large_grids_indices], large_grids)) {
    stop("Statistical grids merged in wrong position!")
  }
  
  return(all_grids)
}

parallel_intersection <- function(x, y, n_cores) {
  if (n_cores == 1) {
    batch_indices = list(1:nrow(x))
  } else {
    indices_cuts <- cut(1:nrow(x), breaks = n_cores)
    batch_indices <- split(1:nrow(x), indices_cuts)
  }
  
  future::plan(future.callr::callr, workers = n_cores)
  
  intersections_list <- furrr::future_map(
    batch_indices,
    function(is) {
      unified_x <- sf::st_union(x[is, ])
      
      intersecting_y <- sf::st_intersects(y, unified_x)
      filtered_y <- y[lengths(intersecting_y) > 0, ]
      
      sf::st_intersection(x[is, ], filtered_y)
    },
    .options = furrr::furrr_options(seed = TRUE, globals = c("x", "y")),
    .progress = getOption("TARGETS_SHOW_PROGRESS")
  )
  
  future::plan(future::sequential)
  
  # the class of the geom column of each list element may be different,
  # depending whether the intersection result in polygon or multipolygon
  # objects. so we check if they are different, and, if so, we cast it to the
  # same class
  
  intersected_obj <- do.call(rbind, intersections_list)
  
  return(intersected_obj)
}

parallel_union <- function(object, n_cores) {
  if (n_cores == 1) {
    batch_indices = list(1:nrow(object))
  } else {
    indices_cuts <- cut(1:nrow(object), breaks = n_cores)
    batch_indices <- split(1:nrow(object), indices_cuts)
  }
  
  future::plan(future.callr::callr, workers = n_cores)
  
  unified_object_list <- furrr::future_map(
    batch_indices,
    function(is) {
      x <- sf::st_union(object[is, ])
      x <- sf::st_make_valid(x)
      x
    },
    .options = furrr::furrr_options(seed = TRUE, globals = "object"),
    .progress = getOption("TARGETS_SHOW_PROGRESS")
  )
  
  future::plan(future::sequential)
  
  unified_object <- Reduce(
    function(obj1, obj2) {
      x <- sf::st_union(obj1, obj2)
      x <- sf::st_make_valid(x)
    },
    unified_object_list
  )
  
  return(unified_object)
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
      if (n_batches > 1) {
        unified_x <- parallel_union(x[is, ], n_cores_union)
        
        intersecting_y <- sf::st_intersects(y, unified_x)
        y <- y[lengths(intersecting_y) > 0, ]
      }

      parallel_intersection(x[is, ], y, n_cores_inter)
    }
  )
  
  intersected_obj <- do.call(rbind, intersections_list)
  intersected_obj <- sf::st_make_valid(intersected_obj)
  
  return(intersected_obj)
}
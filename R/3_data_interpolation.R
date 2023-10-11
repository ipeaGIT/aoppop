# stat_grid <- tar_read(individual_stat_grids, 1)[[1]]
# tracts_with_data <- tar_read(individual_tracts_with_data, 1)[[1]]
# manual_parallelization <- FALSE
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
  
  grid_tracts_intersection[, intersect_population := prop_grid_area * pop_count]
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
  final_colnames <- c("renda", sub("intersect_", "", cols_to_sum[-1]))
  
  grid_with_data <- grid_tracts_intersection[
    ,
    append(
      list(pop_total = pop_count[1], homens = men[1], mulheres = women[1]),
      lapply(.SD, sum)
    ),
    by = ID_UNICO,
    .SDcols = cols_to_sum
  ]
  
  data.table::setnames(grid_with_data, old = cols_to_sum, new = final_colnames)
  
  # using data.table native left join would later result in an issue with some
  # of the stat_grids (particularly São Roque's) in which the spatial objects
  # would not be valid. related to
  # https://github.com/Rdatatable/data.table/issues/4217
  
  grid_with_data <- dplyr::left_join(
    grid_with_data,
    stat_grid[, c("ID_UNICO", "geom")],
    by = "ID_UNICO"
  )
  
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

# pop_unit <- subset(tar_read(pop_units), tar_group == 1)
# stat_grid <- tar_read(stat_grids_with_data)[[1]]
# hex_grid_path <- tar_read(hex_grids, branches = 1)[[1]]
# res <- 7
# manual_parallelization <- FALSE
aggregate_data_to_hexagons <- function(pop_unit,
                                       stat_grid,
                                       hex_grid_path,
                                       res,
                                       manual_parallelization) {
  hex_grid <- readRDS(hex_grid_path)
  
  # filter out hexagons that don't intersect with the grid cells, otherwise they
  # would significantly slow down the intersection process
  
  unified_grid <- sf::st_union(stat_grid)
  unified_grid <- sf::st_make_valid(unified_grid)
  
  intersecting_hexs <- sf::st_intersects(hex_grid, unified_grid)
  do_intersect <- lengths(intersecting_hexs) > 0
  
  filtered_hex_grid <- hex_grid[do_intersect, ]
  
  # similarly, filter out stat grid cells that don't intersect with the
  # hexagons. this happens because the individual statistical grids are
  # generated from the census tracts. tracts that intersect with a buffer of 3
  # km around the population units were kept to make sure that we would not lose
  # any data, so we can have some grid cells that are very far from the actual
  # urban concentrations, especially in more rural areas.
  
  unified_filtered_hexs <- sf::st_union(filtered_hex_grid)
  
  intersecting_grid_cells <- sf::st_intersects(stat_grid, unified_filtered_hexs)
  do_intersect <- lengths(intersecting_grid_cells) > 0
  
  filtered_stat_grid <- stat_grid[do_intersect, ]
  
  # st_intersection can be quite slow with the datasets we have. so we first
  # find which grid cells are fully contained by the hexagons, create a dataset
  # with these cells and filter them out from the grid. then, we use these
  # further filtered grid to calculate the intersections.
  
  contains <- sf::st_contains_properly(
    filtered_hex_grid,
    filtered_stat_grid
  )
  
  containing_hexs <- data.table::data.table(
    h3_address = filtered_hex_grid$h3_address,
    contains = unclass(contains)
  )
  containing_hexs <- containing_hexs[
    ,
    .(cell_index = contains[[1]]),
    by = h3_address
  ]
  containing_hexs[, ID_UNICO := filtered_stat_grid[cell_index, ]$ID_UNICO]
  containing_hexs[, cell_index := NULL]
  
  # again, using dplyr's left_join() instead of data.table's because the latter
  # can mess up with the object's attributes, which end up making the object
  # invalid
  
  containing_hexs <- dplyr::left_join(
    containing_hexs,
    filtered_stat_grid,
    by = "ID_UNICO"
  )
  containing_hexs <- sf::st_sf(containing_hexs)
  sf::st_geometry(containing_hexs) <- "geometry"
  
  contained_cells_indices <- unlist(contains)
  further_filtered_stat_grid <- filtered_stat_grid[-contained_cells_indices, ]
  further_filtered_stat_grid <- sf::st_sf(further_filtered_stat_grid)
  
  hex_grid_intersection <- if (manual_parallelization) {
    if (res == 9 && nrow(stat_grid) > 10000) {
      parallel_intersection_in_batches(
        filtered_hex_grid,
        further_filtered_stat_grid,
        n_batches = 2,
        n_cores_union = 4,
        n_cores_inter = getOption("TARGETS_N_CORES")
      )
    } else {
      parallel_intersection(
        filtered_hex_grid,
        further_filtered_stat_grid,
        n_cores = getOption("TARGETS_N_CORES")
      )
    }
  } else {
    sf::st_intersection(filtered_hex_grid, further_filtered_stat_grid)
  }
  
  # we bind the contained cells dataset with the intersection dataset before
  # proceeding with the calculations.
  
  intersections <- rbind(containing_hexs, hex_grid_intersection)
  
  intersections <- sf::st_make_valid(intersections)
  intersections$intersect_area <- as.numeric(sf::st_area(intersections))
  
  data.table::setDT(intersections)
  
  # we also change the name of some columns and calculate the proportion of each
  # age, race and income group in each intersection.
  
  data.table::setnames(
    intersections,
    old = c("pop_total", "homens", "mulheres", "renda"),
    new = c(
      "grid_cell_total_pop",
      "grid_cell_men",
      "grid_cell_women",
      "grid_cell_income"
    )
  )
  
  colnames <- names(intersections)
  
  age_cols <- colnames[grepl("idade", colnames)]
  age_prop_cols <- paste0(age_cols, "_prop")
  
  intersections[
    ,
    (age_prop_cols) := lapply(.SD, function(x) x / grid_cell_total_pop),
    .SDcols = age_cols
  ]
  
  race_cols <- colnames[grepl("cor", colnames)]
  race_prop_cols <- paste0(race_cols, "_prop")
  
  intersections[
    ,
    (race_prop_cols) := lapply(.SD, function(x) x / grid_cell_total_pop),
    .SDcols = race_cols
  ]
  
  intersections[, c(age_cols, race_cols) := NULL]
  
  # the rest of the process (the calculations themselves) are the same conducted
  # in aggregate_data_to_stat_grid()
  
  intersections[, stat_cell_total_area := sum(intersect_area), by = ID_UNICO]
  intersections[, prop_stat_cell_area := intersect_area / stat_cell_total_area]
  
  # calculating total income in each intersection. we assume that income is
  # evenly distributed among individuals living in the same statistical grid
  # cell, so it's proportional to the share of the cell in the intersection.
  
  intersections[
    ,
    `:=`(
      intersect_population = prop_stat_cell_area * grid_cell_total_pop,
      intersect_men = prop_stat_cell_area * grid_cell_men,
      intersect_women = prop_stat_cell_area * grid_cell_women
    )
  ]
  intersections[, intersect_income := prop_stat_cell_area * grid_cell_income]
  
  # to calculate the population count by race and age group in each
  # intersection, we just need to multiply the population in each intersection
  # by the proportions of each group, previously calculated
  
  group_prop_cols <- c(age_prop_cols, race_prop_cols)
  new_colnames <- paste0("intersect_", sub("_prop", "", group_prop_cols))
  
  intersections[
    ,
    (new_colnames) := lapply(.SD, function(x) x * intersect_population),
    .SDcols = group_prop_cols
  ]
  
  # the total population count, count per group and total income in each grid
  # cell are the sum of each of the variables calculated above by hexagon
  
  pop_cols <- paste0("intersect_", c("population", "men", "women"))
  
  cols_to_sum <- c(pop_cols, "intersect_income", new_colnames)
  final_colnames <- sub("intersect_", "", cols_to_sum)
  final_colnames[1:4] <- c("pop_total", "homens", "mulheres", "renda_total")
  
  hexs_with_data <- intersections[
    ,
    lapply(.SD, sum),
    by = h3_address,
    .SDcols = cols_to_sum
  ]
  
  data.table::setnames(hexs_with_data, old = cols_to_sum, new = final_colnames)
  
  # finally, since we used filtered_hex_grid, and not hex_grid, to calculate the
  # intersections, we need to merge our dataset "with data" with the complete
  # hex grid. we fill the columns of hexagons "without data" with the
  # appropriate values.
  
  data_cols <- setdiff(names(hexs_with_data), "h3_address")
  
  full_hexs_with_data <- dplyr::left_join(
    hex_grid,
    hexs_with_data,
    by = "h3_address"
  )
  
  data.table::setDT(full_hexs_with_data)
  full_hexs_with_data[is.na(pop_total), (data_cols) := 0]
  full_hexs_with_data[, renda_per_capita := renda_total / pop_total]
  
  full_hexs_with_data <- sf::st_sf(full_hexs_with_data)
  
  # save object as RDS and return the path
  
  hex_dir <- file.path(
    "../../data/acesso_oport_v2/hex_grids_with_data",
    paste0("res_", res),
    "2010"
  )
  if (!dir.exists(hex_dir)) dir.create(hex_dir, recursive = TRUE)
  
  basename <- paste0(pop_unit$code_pop_unit, "_", pop_unit$treated_name, ".rds")
  filepath <- file.path(hex_dir, basename)
  
  saveRDS(full_hexs_with_data, filepath)
  
  return(filepath)
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
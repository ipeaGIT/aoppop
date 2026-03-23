# tracts -> stat grid ----------------------------------------------------

# n_pop_unit <- 1 # 240
# year <- 2022
# pop_unit <- tar_read(pop_units)[n_pop_unit, ]
# stat_grid <- tar_read(individual_stat_grids, branches = n_pop_unit + ifelse(year == 2010, 0, 376))[[1]]
# tracts_with_data <- tar_read(individual_tracts_with_data, branches = n_pop_unit + ifelse(year == 2010, 0, 376))[[1]]
aggregate_data_to_small_stat_grid <- function(
  year,
  pop_unit,
  stat_grid,
  tracts_with_data
) {
  cli::cli_inform(
    paste(
      "Bringing data to statistical grid for",
      "unit {.val {pop_unit$treated_name}} and year {.val {year}}"
    )
  )

  if (nrow(stat_grid) > 10000) {
    return(NULL)
  }

  tracts_with_data <- prepare_tracts_for_intersection(tracts_with_data)

  # suppressed warning:
  #   - attribute variables are assumed to be spatially constant throughout all geometries
  #
  # ps: again we may have problems with some tracts, which raise errors of type
  #  "Loop x edge y has duplicate near loop z edge w", even with the
  #  st_make_valid() call at the end of the individual_tracts target. another
  #  st_make_valid() solves this issue. example: n_pop_unit=223 year=2010
  #  (piracicaba)

  grid_tracts_intersection_or_error <- tryCatch(
    suppressWarnings(sf::st_intersection(stat_grid, tracts_with_data)),
    error = function(cnd) cnd
  )

  if (inherits(grid_tracts_intersection_or_error, "error")) {
    tracts_with_data <- sf::st_make_valid(tracts_with_data)
    grid_tracts_intersection <- suppressWarnings(
      sf::st_intersection(stat_grid, tracts_with_data)
    )
  } else {
    grid_tracts_intersection <- grid_tracts_intersection_or_error
  }

  grid_with_data <- interpolate_tracts_to_grid(
    grid_tracts_intersection,
    tracts_with_data,
    stat_grid,
    year
  )

  return(grid_with_data)
}

# n_possiveis <- c(51L, 78L, 98L, 108L, 170L, 192L, 212L, 239L, 240L, 256L, 276L,
#               283L, 323L, 370L, 376L, 379L, 383L, 427L, 454L, 474L, 484L, 532L,
#               546L, 568L, 588L, 615L, 616L, 620L, 632L, 652L, 657L, 659L, 683L,
#               699L, 734L, 746L, 752L)
# n <- 746 # 10092 celulas
# n_pop_unit <- ifelse(n <= 376, n, n - 376)
# year <- ifelse(n <= 376, 2010, 2022)
# pop_unit <- tar_read(pop_units)[n_pop_unit, ]
# stat_grid <- tar_read(individual_stat_grids, branches = n_pop_unit + ifelse(year == 2010, 0, 376))[[1]]
# tracts_with_data <- tar_read(individual_tracts_with_data, branches = n_pop_unit + ifelse(year == 2010, 0, 376))[[1]]
aggregate_data_to_large_stat_grid <- function(
  year,
  pop_unit,
  stat_grid,
  tracts_with_data
) {
  if (nrow(stat_grid) <= 10000) {
    return(NULL)
  }

  cli::cli_inform(
    paste(
      "Bringing data to statistical grid for",
      "unit {.val {pop_unit$treated_name}} and year {.val {year}}"
    )
  )

  tracts_with_data <- prepare_tracts_for_intersection(tracts_with_data)

  grid_tracts_intersection <- parallel_intersection(stat_grid, tracts_with_data)

  grid_with_data <- interpolate_tracts_to_grid(
    grid_tracts_intersection,
    tracts_with_data,
    stat_grid,
    year
  )

  return(grid_with_data)
}

prepare_tracts_for_intersection <- function(tracts_with_data) {
  cols_to_drop <- c("cod_uf", "cod_muni")

  tracts_with_data <- tracts_with_data[,
    setdiff(names(tracts_with_data), cols_to_drop)
  ]

  # signaling which tracts are empty (i.e. no population), which will be
  # important when calculation the group population in each intersection
  # "slice"

  tracts_with_data <- dplyr::mutate(
    tracts_with_data,
    is_empty = (cor_branca_prop == 0 &
      cor_parda_prop == 0 &
      cor_preta_prop == 0 &
      cor_indigena_prop == 0 &
      cor_amarela_prop == 0 &
      cor_nao_ident_prop == 0)
  )

  return(tracts_with_data)
}

interpolate_tracts_to_grid <- function(
  grid_tracts_intersection,
  tracts_with_data,
  stat_grid,
  year
) {
  grid_tracts_intersection <- sf::st_make_valid(grid_tracts_intersection)

  # previously used st_make_valid() changes geometries, so we have to calculate
  # grid cell and census tracts areas after making the geometries valid. if we
  # don't, our final population (calculated from the group proportions and by
  # the intersect areas) and the sum of the population in the grid cells will
  # be different

  grid_tracts_intersection$intersect_area <- as.numeric(
    sf::st_area(grid_tracts_intersection)
  )

  # to calculate the population count by race and age group in each
  # intersection, we just need to multiply the population in each intersection
  # by the proportions of each group, previously calculated. we have a few cases
  # we have to pay attention, though.

  # - first: if a grid cell only intercepts with one tract and this tract
  # doesn't contain any population, we cannot "throw away" the cell population.
  # instead, we assume the population is non identified in terms of group
  # classification, and set cor/idade_nao_ident_prop to 1, while also setting
  # the intersection to not empty

  data.table::setDT(grid_tracts_intersection)

  grid_tracts_intersection[
    grid_tracts_intersection[, .I[all(is_empty)], by = ID_UNICO]$V1,
    `:=`(
      cor_nao_ident_prop = 1,
      idade_nao_ident_prop = 1,
      is_empty = FALSE
    )
  ]

  # - second: if, on the other hand, a cell intercepts with several tracts,
  # and at least one of them contain any population, we remove the empty
  # interceptions and use only the non empty to calculate the group sizes. since
  # we have set the previous cases to not empty, we can simply remove the empty
  # slices now, as they are guaranteed not to be the only intersection with the
  # grid cells

  grid_tracts_intersection <- grid_tracts_intersection[is_empty == FALSE]

  # grid_tracts_intersection[,
  #   tract_total_area := sum(intersect_area),
  #   by = code_tract
  # ]
  grid_tracts_intersection[,
    grid_total_area := sum(intersect_area),
    by = ID_UNICO
  ]

  grid_tracts_intersection[, prop_grid_area := intersect_area / grid_total_area]
  # grid_tracts_intersection[,
  #   prop_tract_area := intersect_area / tract_total_area
  # ]

  # to calculate the total income in each intersection, we have to calculate the
  # population in each intersection (from the statistical grid population
  # count), calculate the total population count in each tract and then
  # calculate the share of the total tract population count in each
  # intersection. income is distributed according to this share - i.e. we assume
  # that income is evenly distributed among individuals living in the same tract

  grid_tracts_intersection[, intersect_population := prop_grid_area * pop_count]

  if (year == 2010) {
    grid_tracts_intersection[,
      tract_population := sum(intersect_population),
      by = code_tract
    ]
    grid_tracts_intersection[,
      intersect_renda_total := renda_total *
        intersect_population /
        tract_population
    ]
  }

  group_prop_cols <- setdiff(
    names(tracts_with_data),
    c("code_tract", "renda_total", "geom", "tract_total_area")
  )
  new_colnames <- paste0("intersect_", sub("_prop", "", group_prop_cols))

  grid_tracts_intersection[,
    (new_colnames) := lapply(.SD, function(x) x * intersect_population),
    .SDcols = group_prop_cols
  ]

  # the total population count per group and total income in each grid cell is
  # the sum of each of the variables calculated above by grid cell

  cols_to_sum <- if (year == 2010) {
    c("intersect_renda_total", new_colnames)
  } else {
    new_colnames
  }

  grid_with_data <- grid_tracts_intersection[,
    append(
      list(pop_total = pop_count[1]),
      lapply(.SD, sum)
    ),
    by = ID_UNICO,
    .SDcols = cols_to_sum
  ]

  final_colnames <- sub("intersect_", "", cols_to_sum)
  data.table::setnames(grid_with_data, old = cols_to_sum, new = final_colnames)

  # using data.table native left join would later result in an issue with some
  # of the stat_grids (particularly São Roque's) in which the spatial objects
  # would not be valid. related to
  # https://github.com/Rdatatable/data.table/issues/4217

  grid_with_data <- dplyr::left_join(
    grid_with_data,
    stat_grid[, c("ID_UNICO", "geometry")],
    by = "ID_UNICO"
  )
  grid_with_data <- sf::st_sf(grid_with_data)

  # # some sanity checks

  # if (nrow(grid_with_data) < nrow(stat_grid)) {
  #   stop("aggregated grid has less cells than raw grid")
  # }

  # total_pop <- sum(grid_with_data$pop_total)

  # total_pop_age <- 0
  # age_cols <- names(grid_with_data)[grepl("idade_", names(grid_with_data))]
  # for (col in age_cols) {
  #   total_pop_age <- total_pop_age + sum(grid_with_data[[col]])
  # }

  # total_pop_color <- sum(grid_with_data$cor_branca) +
  #   sum(grid_with_data$cor_preta) +
  #   sum(grid_with_data$cor_amarela) +
  #   sum(grid_with_data$cor_parda) +
  #   sum(grid_with_data$cor_indigena) +
  #   sum(grid_with_data$cor_nao_ident)

  # if (!dplyr::near(total_pop, total_pop_age)) {
  #   format(total_pop_age, nsmall = 10)
  #   format(total_pop, nsmall = 10)
  #   stop("total pop by age is different than total pop")
  # }

  # if (!dplyr::near(total_pop, total_pop_color)) {
  #   format(total_pop_color, nsmall = 10)
  #   format(total_pop, nsmall = 10)
  #   stop("total pop by age is different than total pop")
  # }

  return(grid_with_data)
}

# stat grid -> hexs ------------------------------------------------------

# res <- 7
# n_pop_unit <- 1
# year <- 2010
# pop_unit <- tar_read(pop_units)[n_pop_unit, ]
# small_stat_grid <- tar_read(small_stat_grids_with_data, branches = n_pop_unit + ifelse(year == 2010, 0, 376))[[1]]
# large_stat_grid <- tar_read(large_stat_grids_with_data, branches = n_pop_unit + ifelse(year == 2010, 0, 376))[[1]]
# hex_grid_file <- tar_read_raw(paste0("file_hex_grids_res", res), branches = n_pop_unit)[[1]]
aggregate_data_to_small_hexagons <- function(
  year,
  pop_unit,
  small_stat_grid,
  large_stat_grid,
  hex_grid_file
) {
  res <- as.integer(stringr::str_extract(hex_grid_file, "(?<=\\/res_)\\d{1}"))

  stat_grid <- if (is.null(large_stat_grid)) {
    small_stat_grid
  } else {
    large_stat_grid
  }

  if (
    (res == 7 && nrow(stat_grid) > 28000) ||
      (res == 8 && nrow(stat_grid) > 4500) ||
      (res == 9 && nrow(stat_grid) > 2000)
  ) {
    return(NULL)
  }

  cli::cli_inform(
    paste(
      "Bringing data to hex grid res {.val {res}} for",
      "unit {.val {pop_unit$treated_name}} and year {.val {year}}"
    )
  )

  hex_grid <- sf::st_as_sf(arrow::open_dataset(hex_grid_file))

  # filter out hexagons that don't intersect with the grid cells, otherwise they
  # would significantly slow down the intersection process

  filtered_hex_grid <- filter_hex_grid(hex_grid, stat_grid)

  # similarly, filter out stat grid cells that don't intersect with the
  # hexagons. this happens because the individual statistical grids are
  # generated from the census tracts, which were filtered using a 3 km buffer
  # around the pop units (so we could make sure that we would not lose any
  # data). as a result, we can have some grid cells that are very far from the
  # actual urban concentrations, especially in more rural areas, which would
  # make them not intersect with the hexagons.

  filtered_stat_grid <- filter_stat_grid(stat_grid, filtered_hex_grid)

  # st_intersection can be quite slow with the large datasets we're using. so
  # an optimization we can use is to find which grid cells are fully contained
  # by the hexagons, create a dataset with these cells and their data and
  # filter them out from the grid, reducing the workload of the intersection
  # operation. then, we use these further filtered grid to calculate the
  # intersections.
  #
  # we transform the crs because st_contains_properly() assumes coordinates are
  # planar

  filtered_hex_grid <- sf::st_transform(filtered_hex_grid, 5880)
  filtered_stat_grid <- sf::st_transform(filtered_stat_grid, 5880)

  contained_grid_cells <- find_contained_grid_cells(
    filtered_hex_grid,
    filtered_stat_grid
  )

  further_filtered_stat_grid <- filtered_stat_grid[
    !(filtered_stat_grid$ID_UNICO %in% contained_grid_cells$ID_UNICO),
  ]

  contained_grid_cells[, ID_UNICO := NULL]

  # before intersecting the hexs with the stat grid, we calculate the original
  # size of the stat grid cells, which we're going to use later to calculate
  # the number of people in each intersection slice. we assume the population
  # is evenly distributed among the grid, so the final pop size in each slice
  # is slice_area * people_in_stat_cell

  further_filtered_stat_grid$grid_cell_area <- as.numeric(
    sf::st_area(further_filtered_stat_grid)
  )

  # suppressed warning:
  #  - attribute variables are assumed to be spatially constant throughout all
  #    geometries

  hex_grid_intersection <- suppressWarnings(
    sf::st_intersection(filtered_hex_grid, further_filtered_stat_grid)
  )

  hex_grid_intersection <- interpolate_stat_grid_to_hexs(
    hex_grid_intersection,
    year
  )

  full_hexs_with_data <- calculate_hexs_totals(
    contained_grid_cells,
    hex_grid_intersection,
    hex_grid,
    year
  )

  filepath <- save_hexs_with_data(full_hexs_with_data, res, year, pop_unit)

  return(filepath)
}

# res <- 7
# n_pop_unit <- 170 # alguns pra res7: 170, 240, 376
# year <- 2010
# pop_unit <- tar_read(pop_units)[n_pop_unit, ]
# small_stat_grid <- tar_read(small_stat_grids_with_data, branches = n_pop_unit + ifelse(year == 2010, 0, 376))[[1]]
# large_stat_grid <- tar_read(large_stat_grids_with_data, branches = n_pop_unit + ifelse(year == 2010, 0, 376))[[1]]
# hex_grid_file <- tar_read_raw(paste0("hex_grids_res_", res))[n_pop_unit]
aggregate_data_to_large_hexagons <- function(
  year,
  pop_unit,
  small_stat_grid,
  large_stat_grid,
  hex_grid_file
) {
  res <- as.integer(stringr::str_extract(hex_grid_file, "(?<=\\/res_)\\d{1}"))

  stat_grid <- if (is.null(large_stat_grid)) {
    small_stat_grid
  } else {
    large_stat_grid
  }

  if (
    (res == 7 && nrow(stat_grid) <= 28000) ||
      (res == 8 && nrow(stat_grid) <= 4500) ||
      (res == 9 && nrow(stat_grid) <= 2000)
  ) {
    return(NULL)
  }

  cli::cli_inform(
    paste(
      "Bringing data to hex grid res {.val {res}} for",
      "unit {.val {pop_unit$treated_name}} and year {.val {year}}"
    )
  )

  hex_grid <- sf::st_as_sf(arrow::open_dataset(hex_grid_file))

  # filter out hexagons that don't intersect with the grid cells, otherwise they
  # would significantly slow down the intersection process

  unified_grid <- sf::st_union(stat_grid)
  unified_grid <- sf::st_make_valid(unified_grid)

  intersecting_hexs <- sf::st_intersects(hex_grid, unified_grid)
  do_intersect <- lengths(intersecting_hexs) > 0

  filtered_hex_grid <- hex_grid[do_intersect, ]

  # similarly, filter out stat grid cells that don't intersect with the
  # hexagons. this happens because the individual statistical grids are
  # generated from the census tracts, which were filtered using a 3 km buffer
  # around the pop units (so we could make sure that we would not lose any
  # data). as a result, we can have some grid cells that are very far from the
  # actual urban concentrations, especially in more rural areas, which would
  # make them not intersect with the hexagons.

  unified_filtered_hexs <- sf::st_union(filtered_hex_grid)

  intersecting_grid_cells <- sf::st_intersects(stat_grid, unified_filtered_hexs)
  do_intersect <- lengths(intersecting_grid_cells) > 0

  filtered_stat_grid <- stat_grid[do_intersect, ]

  # st_intersection can be quite slow with the large datasets we're using. so
  # an optimization we can use is to find which grid cells are fully contained
  # by the hexagons, create a dataset with these cells and their data and
  # filter them out from the grid, reducing the workload of the intersection
  # operation. then, we use these further filtered grid to calculate the
  # intersections.
  #
  # we transform the crs because st_contains_properly() assumes coordinates are
  # planar

  filtered_hex_grid <- sf::st_transform(filtered_hex_grid, 5880)
  filtered_stat_grid <- sf::st_transform(filtered_stat_grid, 5880)

  contains <- sf::st_contains_properly(
    filtered_hex_grid,
    filtered_stat_grid
  )

  containing_hexs <- data.table::data.table(
    h3_address = filtered_hex_grid$h3_address,
    contains = unclass(contains)
  )
  containing_hexs <- containing_hexs[,
    .(cell_index = contains[[1]]),
    by = h3_address
  ]
  containing_hexs[, ID_UNICO := filtered_stat_grid[cell_index, ]$ID_UNICO]
  containing_hexs[, cell_index := NULL]

  stat_grid_cols <- names(filtered_stat_grid)

  desired_cols <- setdiff(stat_grid_cols, c("ID_UNICO", "is_empty", "geometry"))

  join_expr <- glue::glue("{desired_cols} = i.{desired_cols}")
  join_expr <- paste(join_expr, collapse = ", ")
  join_expr <- glue::glue("`:=`({join_expr})")
  join_expr <- parse(text = join_expr)

  containing_hexs[filtered_stat_grid, on = "ID_UNICO", eval(join_expr)]

  containing_hexs[, ID_UNICO := NULL]

  # # TODO: ACHO QUE NÃO PRECISA DA GEOMETRIA AQUI
  # containing_hexs <- dplyr::left_join(
  #   containing_hexs,
  #   filtered_stat_grid,
  #   by = "ID_UNICO"
  # )
  # containing_hexs <- sf::st_sf(containing_hexs)

  contained_cells_indices <- unlist(contains)
  further_filtered_stat_grid <- filtered_stat_grid[-contained_cells_indices, ]

  # before intersecting the hexs with the stat grid, we calculate the original
  # size of the stat grid cells, which we're going to use later to calculate
  # the number of people in each intersection slice. we assume the population
  # is evenly distributed among the grid, so the final pop size in each slice
  # is slice_area * people_in_stat_cell

  further_filtered_stat_grid$grid_cell_area <- as.numeric(
    sf::st_area(further_filtered_stat_grid)
  )

  # suppressed warning:
  #  - attribute variables are assumed to be spatially constant throughout all
  #    geometries

  hex_grid_intersection <- suppressWarnings(
    sf::st_intersection(filtered_hex_grid, further_filtered_stat_grid)
  )
  hex_grid_intersection <- sf::st_make_valid(hex_grid_intersection)

  hex_grid_intersection$intersection_area <- as.numeric(
    sf::st_area(hex_grid_intersection)
  )

  data.table::setDT(hex_grid_intersection)

  hex_grid_intersection[, prop_grid_area := intersection_area / grid_cell_area]

  # we first calculate the number of people per group in each intersection, then
  # we bind the "contained grid cells" dataset and sum the total count per group
  # in each hexagon

  colnames <- names(hex_grid_intersection)

  cols_to_sum <- c(
    "pop_total",
    colnames[grepl("cor", colnames)],
    colnames[grepl("idade", colnames)]
  )

  if (year == 2010) {
    cols_to_sum <- c(cols_to_sum, "renda_total")
  }

  hex_grid_intersection[,
    (cols_to_sum) := lapply(.SD, function(x) x * prop_grid_area),
    .SDcols = cols_to_sum
  ]

  desired_cols <- c("h3_address", cols_to_sum)
  hex_grid_intersection[,
    setdiff(names(hex_grid_intersection), desired_cols) := NULL
  ]

  intersections <- rbind(containing_hexs, hex_grid_intersection)

  hexs_with_data <- intersections[,
    lapply(.SD, sum),
    .SDcols = cols_to_sum,
    by = h3_address
  ]

  # since we used filtered_hex_grid, and not hex_grid, to calculate the
  # intersections, we need to merge hexs_with_data with the complete hex grid.
  # we fill the columns of hexagons "without data" with the appropriate values.

  data_cols <- setdiff(names(hexs_with_data), "h3_address")

  full_hexs_with_data <- dplyr::left_join(
    hex_grid,
    hexs_with_data,
    by = "h3_address"
  )

  data.table::setDT(full_hexs_with_data)
  full_hexs_with_data[is.na(pop_total), (data_cols) := 0]

  if (year == 2010) {
    full_hexs_with_data[, renda_per_capita := renda_total / pop_total]
  }

  full_hexs_with_data <- sf::st_sf(full_hexs_with_data)

  filepath <- save_hexs_with_data(full_hexs_with_data, res, year)

  return(filepath)

  # if (manual_parallelization) {
  #   if (res == 9 && nrow(stat_grid) > 10000) {
  #     parallel_intersection_in_batches(
  #       filtered_hex_grid,
  #       further_filtered_stat_grid,
  #       n_batches = 2,
  #       n_cores_union = 4,
  #       n_cores_inter = getOption("TARGETS_N_CORES")
  #     )
  #   } else {
  #     parallel_intersection(
  #       filtered_hex_grid,
  #       further_filtered_stat_grid,
  #       n_cores = getOption("TARGETS_N_CORES")
  #     )
  #   }
  # } else {
  #   sf::st_intersection(filtered_hex_grid, further_filtered_stat_grid)
  # }

  # TODO: APAGAR DAQUI PRA BAIXO

  # # we bind the contained cells dataset with the intersection dataset before
  # # proceeding with the calculations.

  # intersections <- rbind(containing_hexs, hex_grid_intersection)

  # intersections <- sf::st_make_valid(intersections)
  # intersections$intersect_area <- as.numeric(sf::st_area(intersections))

  # data.table::setDT(intersections)

  # # we also change the name of some columns and calculate the proportion of each
  # # age, race and income group in each intersection.

  # data.table::setnames(
  #   intersections,
  #   old = c("pop_total", "renda"),
  #   new = c("grid_cell_total_pop", "grid_cell_income"),
  #   skip_absent = TRUE
  # )

  # colnames <- names(intersections)

  # age_cols <- colnames[grepl("idade", colnames)]
  # age_prop_cols <- paste0(age_cols, "_prop")

  # intersections[,
  #   (age_prop_cols) := lapply(.SD, function(x) x / grid_cell_total_pop),
  #   .SDcols = age_cols
  # ]

  # race_cols <- colnames[grepl("cor", colnames)]
  # race_prop_cols <- paste0(race_cols, "_prop")

  # intersections[,
  #   (race_prop_cols) := lapply(.SD, function(x) x / grid_cell_total_pop),
  #   .SDcols = race_cols
  # ]

  # intersections[, c(age_cols, race_cols) := NULL]

  # # the rest of the process (the calculations themselves) are the same conducted
  # # in aggregate_data_to_stat_grid()

  # intersections[, stat_cell_total_area := sum(intersect_area), by = ID_UNICO]
  # intersections[, prop_stat_cell_area := intersect_area / stat_cell_total_area]

  # # calculating total income in each intersection. we assume that income is
  # # evenly distributed among individuals living in the same statistical grid
  # # cell, so it's proportional to the share of the cell in the intersection.

  # intersections[,
  #   `:=`(
  #     intersect_population = prop_stat_cell_area * grid_cell_total_pop,
  #     intersect_men = prop_stat_cell_area * grid_cell_men,
  #     intersect_women = prop_stat_cell_area * grid_cell_women
  #   )
  # ]
  # intersections[, intersect_income := prop_stat_cell_area * grid_cell_income]

  # # to calculate the population count by race and age group in each
  # # intersection, we just need to multiply the population in each intersection
  # # by the proportions of each group, previously calculated

  # group_prop_cols <- c(age_prop_cols, race_prop_cols)
  # new_colnames <- paste0("intersect_", sub("_prop", "", group_prop_cols))

  # intersections[,
  #   (new_colnames) := lapply(.SD, function(x) x * intersect_population),
  #   .SDcols = group_prop_cols
  # ]

  # # the total population count, count per group and total income in each grid
  # # cell are the sum of each of the variables calculated above by hexagon

  # pop_cols <- paste0("intersect_", c("population", "men", "women"))

  # cols_to_sum <- c(pop_cols, "intersect_income", new_colnames)
  # final_colnames <- sub("intersect_", "", cols_to_sum)
  # final_colnames[1:4] <- c("pop_total", "homens", "mulheres", "renda_total")

  # hexs_with_data <- intersections[,
  #   lapply(.SD, sum),
  #   by = h3_address,
  #   .SDcols = cols_to_sum
  # ]

  # data.table::setnames(hexs_with_data, old = cols_to_sum, new = final_colnames)

  # # finally, since we used filtered_hex_grid, and not hex_grid, to calculate the
  # # intersections, we need to merge our dataset "with data" with the complete
  # # hex grid. we fill the columns of hexagons "without data" with the
  # # appropriate values.

  # data_cols <- setdiff(names(hexs_with_data), "h3_address")

  # full_hexs_with_data <- dplyr::left_join(
  #   hex_grid,
  #   hexs_with_data,
  #   by = "h3_address"
  # )

  # data.table::setDT(full_hexs_with_data)
  # full_hexs_with_data[is.na(pop_total), (data_cols) := 0]
  # full_hexs_with_data[, renda_per_capita := renda_total / pop_total]

  # full_hexs_with_data <- sf::st_sf(full_hexs_with_data)

  # # save object as RDS and return the path

  # hex_dir <- file.path(
  #   "../../data/acesso_oport_v2/hex_grids_with_data",
  #   paste0("res_", res),
  #   "2010"
  # )
  # if (!dir.exists(hex_dir)) {
  #   dir.create(hex_dir, recursive = TRUE)
  # }

  # basename <- paste0(pop_unit$code_pop_unit, "_", pop_unit$treated_name, ".rds")
  # filepath <- file.path(hex_dir, basename)

  # saveRDS(full_hexs_with_data, filepath)

  # return(filepath)
}

filter_hex_grid <- function(hex_grid, stat_grid) {
  unified_grid <- sf::st_union(stat_grid)
  unified_grid <- sf::st_make_valid(unified_grid)

  intersecting_hexs <- sf::st_intersects(hex_grid, unified_grid)
  do_intersect <- lengths(intersecting_hexs) > 0

  filtered_hex_grid <- hex_grid[do_intersect, ]

  return(filtered_hex_grid)
}

filter_stat_grid <- function(stat_grid, filtered_hex_grid) {
  unified_filtered_hexs <- sf::st_union(filtered_hex_grid)

  intersecting_grid_cells <- sf::st_intersects(stat_grid, unified_filtered_hexs)
  do_intersect <- lengths(intersecting_grid_cells) > 0

  filtered_stat_grid <- stat_grid[do_intersect, ]

  return(filtered_stat_grid)
}

find_contained_grid_cells <- function(filtered_hex_grid, filtered_stat_grid) {
  contains <- sf::st_contains_properly(
    filtered_hex_grid,
    filtered_stat_grid
  )

  contained_grid_cells <- data.table::data.table(
    h3_address = filtered_hex_grid$h3_address,
    contains = unclass(contains)
  )
  contained_grid_cells <- contained_grid_cells[,
    .(cell_index = contains[[1]]),
    by = h3_address
  ]
  contained_grid_cells[, ID_UNICO := filtered_stat_grid[cell_index, ]$ID_UNICO]
  contained_grid_cells[, cell_index := NULL]

  stat_grid_cols <- names(filtered_stat_grid)

  desired_cols <- setdiff(stat_grid_cols, c("ID_UNICO", "is_empty", "geometry"))

  join_expr <- glue::glue("{desired_cols} = i.{desired_cols}")
  join_expr <- paste(join_expr, collapse = ", ")
  join_expr <- glue::glue("`:=`({join_expr})")
  join_expr <- parse(text = join_expr)

  contained_grid_cells[filtered_stat_grid, on = "ID_UNICO", eval(join_expr)]

  return(contained_grid_cells[])
}

interpolate_stat_grid_to_hexs <- function(hex_grid_intersection, year) {
  hex_grid_intersection <- sf::st_make_valid(hex_grid_intersection)

  hex_grid_intersection$intersection_area <- as.numeric(
    sf::st_area(hex_grid_intersection)
  )

  data.table::setDT(hex_grid_intersection)

  hex_grid_intersection[, prop_grid_area := intersection_area / grid_cell_area]

  # we first calculate the number of people per group in each intersection, then
  # we bind the "contained grid cells" dataset and sum the total count per group
  # in each hexagon

  colnames <- names(hex_grid_intersection)

  cols_to_sum <- c(
    "pop_total",
    colnames[grepl("cor", colnames)],
    colnames[grepl("idade", colnames)]
  )

  if (year == 2010) {
    cols_to_sum <- c(cols_to_sum, "renda_total")
  }

  hex_grid_intersection[,
    (cols_to_sum) := lapply(.SD, function(x) x * prop_grid_area),
    .SDcols = cols_to_sum
  ]

  desired_cols <- c("h3_address", cols_to_sum)
  hex_grid_intersection[,
    setdiff(names(hex_grid_intersection), desired_cols) := NULL
  ]

  return(hex_grid_intersection[])
}

calculate_hexs_totals <- function(
  contained_grid_cells,
  hex_grid_intersection,
  hex_grid,
  year
) {
  intersections <- rbind(contained_grid_cells, hex_grid_intersection)

  cols_to_sum <- setdiff(names(intersections), "h3_address")

  hexs_with_data <- intersections[,
    lapply(.SD, sum),
    .SDcols = cols_to_sum,
    by = h3_address
  ]

  # since we used filtered_hex_grid, and not hex_grid, to calculate the
  # intersections, we need to merge hexs_with_data with the complete hex grid.
  # we fill the columns of hexagons "without data" with the appropriate values.

  full_hexs_with_data <- dplyr::left_join(
    hex_grid,
    hexs_with_data,
    by = "h3_address"
  )

  data.table::setDT(full_hexs_with_data)
  full_hexs_with_data[is.na(pop_total), (cols_to_sum) := 0]

  if (year == 2010) {
    full_hexs_with_data[, renda_per_capita := renda_total / pop_total]
  }

  full_hexs_with_data <- sf::st_sf(full_hexs_with_data)

  return(full_hexs_with_data)
}

save_hexs_with_data <- function(full_hexs_with_data, res, year, pop_unit) {
  hex_dir <- file.path(
    Sys.getenv("USERS_DATA_PATH"),
    "Proj_acess_oport/data/acesso_oport_v2/hex_grids_with_data",
    glue::glue("res_{res}"),
    year
  )

  if (!dir.exists(hex_dir)) {
    dir.create(hex_dir, recursive = TRUE)
  }

  basename <- glue::glue(
    "{pop_unit$code_pop_unit}_{pop_unit$treated_name}.parquet"
  )
  filepath <- file.path(hex_dir, basename)

  # writing to local tempfile then moving to the server is faster than writing
  # directly to the server

  local_tmpfile <- tempfile(fileext = ".parquet")

  arrow::write_parquet(full_hexs_with_data, local_tmpfile)

  file.copy(local_tmpfile, filepath, overwrite = TRUE)

  unlink(local_tmpfile)

  return(filepath)
}

# pop_unit <- subset(tar_read(pop_units), tar_group == 1)
# stat_grid <- tar_read(stat_grids_with_data)[[1]]
# hex_grid_path <- tar_read(hex_grids, branches = 1)[[1]]
# res <- 7
# manual_parallelization <- FALSE
aggregate_data_to_hexagons <- function(
  pop_unit,
  stat_grid,
  hex_grid_path,
  res,
  manual_parallelization
) {
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
  containing_hexs <- containing_hexs[,
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

  intersections[,
    (age_prop_cols) := lapply(.SD, function(x) x / grid_cell_total_pop),
    .SDcols = age_cols
  ]

  race_cols <- colnames[grepl("cor", colnames)]
  race_prop_cols <- paste0(race_cols, "_prop")

  intersections[,
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

  intersections[,
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

  intersections[,
    (new_colnames) := lapply(.SD, function(x) x * intersect_population),
    .SDcols = group_prop_cols
  ]

  # the total population count, count per group and total income in each grid
  # cell are the sum of each of the variables calculated above by hexagon

  pop_cols <- paste0("intersect_", c("population", "men", "women"))

  cols_to_sum <- c(pop_cols, "intersect_income", new_colnames)
  final_colnames <- sub("intersect_", "", cols_to_sum)
  final_colnames[1:4] <- c("pop_total", "homens", "mulheres", "renda_total")

  hexs_with_data <- intersections[,
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
  if (!dir.exists(hex_dir)) {
    dir.create(hex_dir, recursive = TRUE)
  }

  basename <- paste0(pop_unit$code_pop_unit, "_", pop_unit$treated_name, ".rds")
  filepath <- file.path(hex_dir, basename)

  saveRDS(full_hexs_with_data, filepath)

  return(filepath)
}

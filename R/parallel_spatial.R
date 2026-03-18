parallel_intersection <- function(
  x,
  y,
  n_cores = getOption("TARGETS_N_CORES")
) {
  if (n_cores == 1) {
    batch_indices = list(seq_len(nrow(x)))
  } else {
    indices_cuts <- cut(seq_len(nrow(x)), breaks = n_cores)
    batch_indices <- split(seq_len(nrow(x)), indices_cuts)
  }

  mirai::daemons(n_cores)

  intersections_list <- purrr::pmap(
    list(x, y, batch_indices),
    purrr::in_parallel(function(x, y, is) {
      unified_x <- sf::st_union(x[is, ])

      intersecting_y <- sf::st_intersects(y, unified_x)
      filtered_y <- y[lengths(intersecting_y) > 0, ]

      sf::st_intersection(x[is, ], filtered_y)
    }),
    .progress = getOption("TARGETS_SHOW_PROGRESS")
  )

  mirai::daemons(0)

  # the class of the geom column of each list element may be different,
  # depending whether the intersection result in polygon or multipolygon
  # objects. so we check if they are different, and, if so, we cast it to the
  # same class

  intersected_obj <- do.call(rbind, intersections_list)

  return(intersected_obj)
}

parallel_intersects <- function(x, y, n_cores = getOption("TARGETS_N_CORES")) {
  if (n_cores == 1) {
    batch_indices = list(seq_len(nrow(x)))
  } else {
    indices_cuts <- cut(seq_len(nrow(x)), breaks = n_cores)
    batch_indices <- split(seq_len(nrow(x)), indices_cuts)
  }

  mirai::daemons(n_cores)

  intersect_list <- purrr::pmap(
    list(list(x), list(y), batch_indices),
    purrr::in_parallel(function(x, y, is) {
      intersecting <- sf::st_intersects(x[is, ], y)
    }),
    .progress = getOption("TARGETS_SHOW_PROGRESS")
  )

  mirai::daemons(0)

  intersect_list <- Reduce(append, intersect_list)

  return(intersect_list)
}

parallel_union <- function(object, n_cores = getOption("TARGETS_N_CORES")) {
  if (n_cores == 1) {
    batch_indices = seq_len(nrow(object))
  } else {
    indices_cuts <- cut(seq_len(nrow(object)), breaks = n_cores)
    batch_indices <- split(seq_len(nrow(object)), indices_cuts)
  }

  mirai::daemons(n_cores)

  unified_object_list <- purrr::map2(
    list(object),
    batch_indices,
    purrr::in_parallel(function(obj, is) {
      x <- sf::st_union(obj[is, ])
      x <- sf::st_make_valid(x)
      x
    })
  )

  mirai::daemons(0)

  unified_object <- Reduce(
    function(obj1, obj2) {
      x <- sf::st_union(obj1, obj2)
      x <- sf::st_make_valid(x)
    },
    unified_object_list
  )

  return(unified_object)
}

parallel_intersection_in_batches <- function(
  x,
  y,
  n_batches,
  n_cores_union,
  n_cores_inter
) {
  if (n_batches == 1) {
    batch_indices = seq_len(nrow(x))
  } else {
    indices_cuts <- cut(seq_len(nrow(x)), breaks = n_batches)
    batch_indices <- split(seq_len(nrow(x)), indices_cuts)
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

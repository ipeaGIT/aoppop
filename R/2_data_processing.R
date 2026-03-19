# year <- tar_read(years)[1]
prepare_census_data <- function(year) {
  if (year == 2010) {
    # census variables:
    #   - Dom2_V002 -> total count of residents in permanent private households
    #   - DomRend_V003 -> total income
    #   - Pess3_V002:V006 -> population count by color/race
    #   - Pess13_V023:Pess13_V134 -> population count by age group

    dom <- censobr::read_tracts(year, dataset = "Domicilio")
    dom <- dplyr::select(
      dom,
      code_tract,
      code_muni,
      code_state,
      domicilio02_V002
    )

    dom_renda <- censobr::read_tracts(year, dataset = "DomicilioRenda")
    dom_renda <- dplyr::select(dom_renda, c(code_tract, V003))

    # FIXME: censobr Pessoa table is missing the tract 250010605000004.
    pess <- censobr::read_tracts(year, dataset = "Pessoa")
    pess <- dplyr::select(
      pess,
      code_tract,
      pessoa03_V002:pessoa03_V006,
      pessoa13_V023:pessoa13_V134
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
  } else if (year == 2022) {
    # census variables:
    #   # - V0001 -> total population count         # removing for now
    #   - domicilio01_V00005 -> total count of population in permanent private households
    #   - raca_V01317:V01321 -> population count by color/race
    #   - demografia_V01031:V01041 -> population count by age group

    # bas <- censobr::read_tracts(year, dataset = "Basico")
    # bas <- dplyr::select(
    #   bas,
    #   code_tract,
    #   code_muni,
    #   code_state,
    #   V0001
    # )

    dom <- censobr::read_tracts(year, dataset = "Domicilio")
    dom <- dplyr::select(
      dom,
      code_tract,
      code_muni,
      code_state,
      domicilio01_V00005
    )

    # census_data <- dplyr::left_join(bas, dom, by = "code_tract")
    # census_data <- dplyr::mutate(
    #   census_data,
    #   moradores_dom_part = domicilio01_V00005 + domicilio01_V00006
    # )

    pess <- censobr::read_tracts(year, dataset = "Pessoas")
    pess <- dplyr::select(
      pess,
      code_tract,
      raca_V01317:raca_V01321,
      demografia_V01031:demografia_V01041
    )

    census_data <- dplyr::left_join(dom, pess, by = "code_tract")

    census_data <- dplyr::select(
      census_data,
      cod_setor = code_tract,
      cod_muni = code_muni,
      cod_uf = code_state,
      moradores_total = domicilio01_V00005,
      cor_branca = raca_V01317,
      cor_preta = raca_V01318,
      cor_amarela = raca_V01319,
      cor_parda = raca_V01320,
      cor_indigena = raca_V01321,
      idade_0a4 = demografia_V01031,
      idade_5a9 = demografia_V01032,
      idade_10a14 = demografia_V01033,
      idade_15a19 = demografia_V01034,
      idade_20a24 = demografia_V01035,
      idade_25a29 = demografia_V01036,
      idade_30a39 = demografia_V01037,
      idade_40a49 = demografia_V01038,
      idade_50a59 = demografia_V01039,
      idade_60a69 = demografia_V01040,
      idade_70mais = demografia_V01041
    )
  }

  cols <- names(census_data)
  age_cols <- cols[grepl("^idade_", cols)]
  race_cols <- cols[grepl("^cor_", cols)]

  census_data <- dplyr::mutate(
    census_data,
    dplyr::across(
      c(
        moradores_total,
        dplyr::starts_with("idade"),
        dplyr::starts_with("cor"),
        dplyr::matches("moradores_dom_part"), # só seleciona para o ano de 2022
      ),
      ~ ifelse(is.na(.), 0, .)
    )
  )
  census_data <- dplyr::compute(census_data)

  expr <- parse(text = paste(age_cols, collapse = "+"))
  census_data <- dplyr::mutate(census_data, age_total = eval(expr))

  expr <- parse(text = paste(race_cols, collapse = "+"))
  census_data <- dplyr::mutate(census_data, race_total = eval(expr))

  # in several tracts, the population count by age/color group is lower than the
  # total count. not knowing the exact reasons for that, we have decided to
  # create an extra "not identified" category for each group, which we fill with
  # the difference between the total and the group count

  census_data <- dplyr::mutate(
    census_data,
    idade_nao_ident = moradores_total - age_total,
    cor_nao_ident = moradores_total - race_total
  )

  # in 2010, we have tracts with more age_total/race_total than moradores_total.
  # in these cases, idade_nao_ident/cor_nao_ident will be negative, so we set it
  # to zero.
  #
  # it's important to note that we use the group counts just to calculate the
  # proportions of each category inside the group, which we'll ultimately carry
  # to the statistical grid and then to the hexagons. therefore, it's not an
  # absurd if the group counts diverge from the total count - although it's
  # useful to note that something is off.
  #
  # we then recalculate age_total and race_total to calculate the proportions

  census_data <- dplyr::mutate(
    census_data,
    idade_nao_ident = ifelse(idade_nao_ident < 0, 0, idade_nao_ident),
    cor_nao_ident = ifelse(cor_nao_ident < 0, 0, cor_nao_ident)
  )

  census_data <- dplyr::mutate(
    census_data,
    age_total = age_total + idade_nao_ident,
    race_total = race_total + cor_nao_ident
  )

  census_data <- dplyr::compute(census_data)

  # if (year == 2022) {
  #   # some tracts have discrepancies between moradores_total and
  #   # race/age_total. in some cases, this is for, allegedly, privacy concerns.
  #   # counts per group are never shown if they are lower than 3. so, when there
  #   # is a difference between general and grouped totals lower than 3, and only
  #   # one of the groups is zeroed, we assume the missing people come from these
  #   # groups. we cannot "fix" tracts in which more than one group is zeroed,
  #   # because we don't know how the distribution between the zeroed groups look
  #   # like

  #   census_data <- data.table::setDT(dplyr::collect(census_data))

  #   census_data <- fill_missing_counts(census_data, "age", age_cols)
  #   census_data <- fill_missing_counts(census_data, "race", race_cols)

  #   age_sum_expr <- parse(text = paste(age_cols, collapse = "+"))
  #   census_data[, age_total := eval(age_sum_expr)]

  #   race_sum_expr <- parse(text = paste(race_cols, collapse = "+"))
  #   census_data[, race_total := eval(race_sum_expr)]

  #   census_data <- remove_collective_hh_tracts(census_data, "age", age_cols)
  #   census_data <- remove_collective_hh_tracts(census_data, "race", race_cols)
  # }

  # total population count will be derived from statistical grid data, not
  # from census data. from the census data, we want the proportions of each
  # group

  census_data <- dplyr::mutate(
    census_data,
    dplyr::across(dplyr::starts_with("idade"), list(prop = ~ . / age_total))
  )

  census_data <- dplyr::mutate(
    census_data,
    dplyr::across(
      dplyr::matches("idade_.*_prop"),
      list(prop = ~ ifelse(age_total == 0, 0, .)),
      .names = "{.col}"
    )
  )

  census_data <- dplyr::mutate(
    census_data,
    dplyr::across(dplyr::starts_with("cor"), list(prop = ~ . / race_total))
  )

  census_data <- dplyr::mutate(
    census_data,
    dplyr::across(
      dplyr::matches("cor_.*_prop"),
      list(prop = ~ ifelse(race_total == 0, 0, .)),
      .names = "{.col}"
    )
  )

  census_data <- dplyr::select(
    census_data,
    dplyr::starts_with("cod"),
    dplyr::matches("renda_total"), # só seleciona para o ano de 2010
    moradores_total,
    age_total,
    race_total,
    dplyr::matches("^cor_.*_prop$"),
    dplyr::matches("^idade_.*_prop$")
  )

  census_data <- data.table::setDT(dplyr::collect(census_data))

  # when moradores_total = 0, renda_total may be NA, so we set it to zero

  if (year == 2010) {
    census_data[moradores_total == 0, renda_total := 0]
  }

  # TODO: em 2022 existem vários setores (8789 de 458772) que tem
  # moradores_total > 0 e age_total ou race_total == 0. por quê? isso não
  # acontece em 2010.
  #
  # ps: diminui ainda mais agora que estamos usando apenas os moradores de
  # domicílios particulares permanentes
  #
  # ainda, em 2022: (468099 registros)
  # nrow(census_data[moradores_total == race_total])     - 303040 65%
  # nrow(census_data[moradores_total == age_total])      - 414770 89%
  #
  # em 2010: (310120 registros)
  # nrow(census_data[moradores_total == race_total])     - 244056 79%
  # nrow(census_data[moradores_total == age_total])      - 244432 79%
  #
  #
  #
  # depois de "preencher" contagens zeradas de grupos com a diferença <=2, por
  # supostas questões de privacidade, ficamos assim em 2022:
  #   (468099 registros)
  # nrow(census_data[moradores_total == race_total])     - 329378 70%
  # nrow(census_data[moradores_total == age_total])      - 430912 92%
  #
  # depois de remover setores sem residentes de domicílios particulares e com
  # totais por grupo zerados:
  #   (464434 registros)
  # nrow(census_data[moradores_total == race_total])     - 329378 71%
  # nrow(census_data[moradores_total == age_total])      - 430912 93%

  census_data[, c("moradores_total", "age_total", "race_total") := NULL]

  return(census_data[])
}

# group <- "race"
# cols <- race_cols
fill_missing_counts <- function(census_data, group, cols) {
  group_total <- ifelse(group == "race", "race_total", "age_total")

  census_data[, group_diff := moradores_total - get(group_total)]

  n_zeroed_vars_expr <- paste(
    glue::glue("({cols} == 0)"),
    collapse = "+"
  )
  n_zeroed_vars_expr <- parse(text = n_zeroed_vars_expr)

  census_data[, n_zeroed := eval(n_zeroed_vars_expr)]

  for (col in cols) {
    census_data[
      group_diff <= 2 & group_diff > 0 & n_zeroed == 1 & get(col) == 0,
      (col) := group_diff
    ]
  }

  census_data[, c("group_diff", "n_zeroed") := NULL]

  return(census_data[])
}

# group <- "age"
# cols <- age_cols
remove_collective_hh_tracts <- function(census_data, group, cols) {
  group_total <- ifelse(group == "race", "race_total", "age_total")

  census_data[, group_diff := moradores_total - get(group_total)]

  n_zeroed_vars_expr <- paste(
    glue::glue("({cols} == 0)"),
    collapse = "+"
  )
  n_zeroed_vars_expr <- parse(text = n_zeroed_vars_expr)

  census_data[, n_zeroed := eval(n_zeroed_vars_expr)]

  # we remove tracts that don't have any residents of private households and
  # whose group total is also zero. however, we also impose the condition of
  # the difference between the group total and the total population count being
  # higher than twice the number of zeroed group classes. group counts are only
  # shown if they are higher than 2, so if the difference is smaller than twice
  # the number of zeroed classes, we cannot know if the group counts are
  # actually zero or if they are masked for privacy concerns

  census_data <- census_data[
    !(group_diff > n_zeroed * 2 &
      moradores_dom_part == 0 &
      get(group_total) == 0)
  ]

  census_data[, c("group_diff", "n_zeroed") := NULL]

  return(census_data[])
}

# census_tracts <- tar_read(pop_units_tracts, branches = 1)[[1]]
# census_data <- tar_read(census_data, branches = 1)[[1]]
merge_census_tracts_data <- function(census_tracts, census_data) {
  tracts_with_data <- dplyr::left_join(
    census_tracts,
    census_data,
    by = dplyr::join_by(code_tract == cod_setor)
  )
  tracts_with_data <- dplyr::filter(tracts_with_data, !is.na(cod_uf))

  return(tracts_with_data)
}

# year <- tar_read(years)[1]
# tracts_with_data <- tar_read(tracts_with_data, branches = 1)[[1]]
# pop_unit <- tar_read(pop_units)[1, ]
filter_tracts_with_data <- function(year, tracts_with_data, pop_unit) {
  cli::cli_inform(
    paste(
      "Preparing census tracts for",
      "unit {.val {pop_unit$treated_name}} and year {.val {year}}"
    )
  )

  intersections <- sf::st_intersects(
    tracts_with_data,
    sf::st_buffer(pop_unit, 3000)
  )
  do_intersect <- lengths(intersections) > 0

  individual_tracts <- tracts_with_data[do_intersect, ]

  # some tracts overlap, which create problems when later reaggregating census
  # data to hex grid (population count would be overcounted). we remove these
  # overlapping bits with st_difference().

  individual_tracts <- sf::st_transform(individual_tracts, 5880)
  individual_tracts <- sf::st_difference(individual_tracts)
  individual_tracts <- sf::st_collection_extract(individual_tracts, "POLYGON")
  individual_tracts <- sf::st_make_valid(individual_tracts)

  individual_tracts <- sf::st_transform(individual_tracts, 4674)
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

  for (i in seq_len(nrow(x))) {
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

# n_pop_unit <- 170
# year <- 2022
# stat_grid <- tar_read(pop_units_stat_grids, branches = ifelse(year == 2010, 1, 2))[[1]]
# pop_unit <- tar_read(pop_units)[n_pop_unit, ]
# tracts_with_data <- tar_read(individual_tracts_with_data, branches = n_pop_unit + ifelse(year == 2010, 0, 376))[[1]]
filter_individual_stat_grids <- function(
  year,
  stat_grid,
  pop_unit,
  tracts_with_data
) {
  cli::cli_inform(
    paste(
      "Preparing statistical grid subset for",
      "unit {.val {pop_unit$treated_name}} and year {.val {year}}"
    )
  )

  tracts_with_data <- sf::st_make_valid(tracts_with_data)

  # we first subset the statistical grid using st_intersects() because it is way
  # faster than st_intersection() (just using the latter would yield the same
  # result, but would take much longer)
  #
  # ps: some tracts may raise errors when using st_union() (of the type "Loop x
  # edge y has duplicate near loop z edge 2"). a second st_make_valid() solves
  # this issue. example: n_pop_unit=223 year=2010 (piracicaba)

  unified_tracts_or_error <- tryCatch(
    sf::st_union(tracts_with_data),
    error = function(cnd) cnd
  )

  if (inherits(unified_tracts_or_error, "error")) {
    tracts_with_data <- sf::st_make_valid(tracts_with_data)
    unified_tracts <- sf::st_union(tracts_with_data)
  } else {
    unified_tracts <- unified_tracts_or_error
  }

  unified_tracts <- sf::st_make_valid(unified_tracts)

  intersections <- sf::st_intersects(stat_grid, unified_tracts)
  do_intersect <- lengths(intersections) > 0

  filtered_grid <- stat_grid[do_intersect, ]

  # finally, we have to intersect the statistical grid with the census tracts
  # that cover the urban concentrations. that's because we consider that the
  # population is distributed only in the areas covered by the tracts (i.e. it's
  # not distributed on top of a body of water or a forest, for example).
  #
  # suppressed warning:
  #  - attribute variables are assumed to be spatially constant throughout all
  #    geometries

  # filtered_grid$area_before <- as.numeric(sf::st_area(filtered_grid))

  suppressWarnings(
    filtered_grid <- sf::st_intersection(filtered_grid, unified_tracts)
  )

  # filtered_grid$area_after <- as.numeric(sf::st_area(filtered_grid))
  # filtered_grid <- dplyr::mutate(
  #   filtered_grid,
  #   share_of_orig_area = area_after / area_before
  # )

  # the intersection operation may result in polygons so small that they become
  # empty geometrycollections after st_make_valid(). we remove them, otherwise
  # they generate NaN values when aggregating data to the statistical grid

  filtered_grid <- sf::st_make_valid(filtered_grid)
  filtered_grid <- filtered_grid[!sf::st_is_empty(filtered_grid), ]

  filtered_grid <- dplyr::select(filtered_grid, c(ID_UNICO, pop_count = POP))

  if (year == 2010) {
    filtered_grid <- dplyr::rename(filtered_grid, geometry = geom)
  }

  return(filtered_grid)
}

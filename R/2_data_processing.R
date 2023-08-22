# census_data_path <- tar_read(census_data_path)
filter_census_data <- function(census_data_path) {
  census_data <- data.table::fread(
    census_data_path,
    select = list(
      character = c("Cod_UF", "Cod_municipio", "Cod_setor"),
      integer = c(
        "DomRend_V003",
        "Dom2_V002",
        "Pess3_V002", "Pess3_V003", "Pess3_V004", "Pess3_V005", "Pess3_V006",
        paste0("Pess13_V", formatC(23:134, width = 3, flag = "0"))
      )
    )
  )
  
  int_cols <- setdiff(
    names(census_data),
    c("Cod_UF", "Cod_municipio", "Cod_setor")
  )
  
  for (col in int_cols) {
    census_data[is.na(get(col)), eval(col) := 0L]
  }
  
  return(census_data[])
}

# census_income_data_paths <- tar_read(census_income_data_paths)
bind_income_data <- function(census_income_data_paths) {
  census_data_list <- lapply(census_income_data_paths, readxl::read_xls)
  
  census_data <- data.table::rbindlist(census_data_list, fill = TRUE)
  
  cols_to_keep <- c("Cod_setor", paste0("V", 683:694))
  census_data <- census_data[, ..cols_to_keep]
  
  census_data[, Cod_setor := as.character(Cod_setor)]
  
  cols_with_x <- cols_to_keep[-1]
  for (col in cols_with_x) {
    census_data[get(col) == "X", eval(col) := 0]
    census_data[, eval(col) := as.integer(get(col))]
  }
  
  data.table::setnames(
    census_data,
    old = cols_with_x,
    new = paste0("Ent4_", cols_with_x)
  )
  
  return(census_data)
}

# subset_census_data <- tar_read(subset_census_data)
# census_income_data <- tar_read(census_income_data)
process_census_data <- function(subset_census_data, census_income_data) {
  census_data <- subset_census_data[census_income_data, on = "Cod_setor"]
  
  expr <- parse(text = paste(paste0("Pess13_V0", 23:39), collapse = "+"))
  census_data[, idade_0a5 := eval(expr)]
  
  expr <- parse(text = paste(paste0("Pess13_V0", 40:48), collapse = "+"))
  census_data[, idade_6a14 := eval(expr)]
  
  expr <- parse(text = paste(paste0("Pess13_V0", 49:52), collapse = "+"))
  census_data[, idade_15a18 := eval(expr)]
  
  expr <- parse(text = paste(paste0("Pess13_V0", 53:58), collapse = "+"))
  census_data[, idade_19a24 := eval(expr)]
  
  expr <- parse(text = paste(paste0("Pess13_V0", 59:73), collapse = "+"))
  census_data[, idade_25a39 := eval(expr)]
  
  expr <- parse(
    text = paste(
      paste0("Pess13_V", formatC(74:103, width = 3, flag = "0")),
      collapse = "+"
    )
  )
  census_data[, idade_40a69 := eval(expr)]
  
  expr <- parse(text = paste(paste0("Pess13_V", 104:134), collapse = "+"))
  census_data[, idade_70mais := eval(expr)]

  census_data[
    ,
    `:=`(
      moradores_SM_0_1Q = Ent4_V683 + Ent4_V684 + Ent4_V693 + Ent4_V694,
      moradores_SM_1Q_1M = Ent4_V685 + Ent4_V686,
      moradores_SM_1M_1 = Ent4_V687 + Ent4_V688,
      moradores_SM_1_2 = Ent4_V689 + Ent4_V690,
      moradores_SM_2 = Ent4_V691 + Ent4_V692
    )
  ]
  
  # select and rename columns
  
  colnames <- names(census_data)
  
  income_bracket_cols <- colnames[grepl("moradores_SM", colnames)]
  age_cols <- colnames[grepl("idade", colnames)]
  
  cols_to_keep <- c(
    "Cod_UF", "Cod_municipio", "Cod_setor", 
    "DomRend_V003", "Dom2_V002", 
    income_bracket_cols,
    "Pess3_V002", "Pess3_V003", "Pess3_V004", "Pess3_V005", "Pess3_V006",
    age_cols
  )
  census_data[, setdiff(colnames, cols_to_keep) := NULL]

  new_colnames <- c(
    "cod_uf", "cod_muni", "cod_setor", 
    "renda_total", "moradores_total", 
    income_bracket_cols,
    "cor_branca", "cor_preta", "cor_amarela", "cor_parda", "cor_indigena",
    age_cols
  )
  data.table::setnames(census_data, old = cols_to_keep, new = new_colnames)
  
  # total population count will be derived from statistical grid data, not from
  # census data. from the census data, we want the proportions of each
  # income/age/racial groups as a share of the total population.
  #
  # however, the total population count living in permanent households
  # (moradores_total) diverges from the sum of population per age and race
  # group. that's not a problem for age and racial groups, because it doesn't
  # affect the proportions, but it is for the income groups - which population
  # count to use? in this case, we use moradores_total to calculate the
  # proportion of each income group.
  #
  # FIXME: there are tracts with 0 moredores_total and non negligibles
  # age_total and race_total. investigate the difference between these variables
  # again.

  age_prop_cols <- paste0(age_cols, "_prop")
  
  census_data[
    ,
    age_total := eval(parse(text = paste(age_cols, collapse = "+")))
  ]
  census_data[
    ,
    (age_prop_cols) := lapply(.SD, function(x) x / age_total),
    .SDcols = age_cols
  ]
  census_data[
    ,
    (age_prop_cols) := lapply(.SD, function(x) ifelse(is.na(x), 0, x)),
    .SDcols = age_prop_cols
  ]

  race_cols <- new_colnames[grepl("cor", new_colnames)]
  race_prop_cols <- paste0(race_cols, "_prop")

  census_data[
    ,
    race_total := eval(parse(text = paste(race_cols, collapse = "+")))
  ]
  census_data[
    ,
    (race_prop_cols) := lapply(.SD, function(x) x / race_total),
    .SDcols = race_cols
  ]
  census_data[
    ,
    (race_prop_cols) := lapply(.SD, function(x) ifelse(is.na(x), 0, x)),
    .SDcols = race_prop_cols
  ]
  
  inc_bracket_pop_cols <- paste0(income_bracket_cols, "_prop")
  
  census_data[
    ,
    (inc_bracket_pop_cols) := lapply(.SD, function(x) x / moradores_total),
    .SDcols = income_bracket_cols
  ]
  census_data[
    ,
    (inc_bracket_pop_cols) := lapply(.SD, function(x) ifelse(is.na(x), 0, x)),
    .SDcols = inc_bracket_pop_cols
  ]
  
  # we can remove tracts that don't have any population on them, because they
  # will not affect the aggregation processes from the census tracts to the
  # statistical grid and, therefore, from the grid to the hexagons
  
  census_data <- census_data[
    moradores_total > 0 | age_total > 0 | race_total > 0
  ]

  census_data[, c("age_total", age_cols) := NULL]
  census_data[, c("race_total", race_cols) := NULL]
  census_data[, c("moradores_total", income_bracket_cols) := NULL]
  
  return(census_data[])
}
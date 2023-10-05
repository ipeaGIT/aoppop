plot_rio_deciles <- function(res) {
  path <- paste0(
    "../../data/acesso_oport/hex_conc_urbanas/res_",
    res,
    "/rio_de_janeiro_rj.rds"
  )
  
  grid <- readRDS(path)
  
  data.table::setDT(grid)
  
  deciles <- Hmisc::wtd.quantile(
    grid$income_per_capita,
    grid$population,
    probs = seq(0.0, 1, by = 0.1)
  )
  
  grid[
    ,
    decile := cut(
      income_per_capita,
      breaks = deciles,
      labels = paste0("D", 1:10),
      include.lowest = TRUE)
  ]
  grid[is.na(decile) & population > 0, decile := "D10"]
  
  concs <- tar_read(urban_concentrations)
  city_border <- concs[concs$name_urban_concentration == "Rio de Janeiro/RJ", ]
  
  p <- ggplot(sf::st_sf(grid[!is.na(decile)])) +
    geom_sf(aes(fill = decile), color = NA) +
    geom_sf(data = city_border, color = "black", fill = NA) +
    scale_fill_brewer(
      name = "Decil de\nrenda",
      labels = c("D1\nmais\npobres", paste0("D", 2:9), "D10\nmais\nricos"),
      palette = "RdBu"
    ) +
    labs(title = paste0("Resolução ", res)) +
    guides(
      fill = guide_legend(byrow = TRUE, nrow = 1, label.position = "bottom")
    ) +
    theme_minimal() +
    theme(
      legend.position = "bottom",
      axis.text = element_blank(),
      plot.title = element_text(hjust = 0.5),
      panel.grid = element_blank()
    )
  
  ggsave(
    p,
    filename = paste0("figures/rio_inc_deciles_res_", res, ".png"),
    width = 15,
    height = 10,
    units = "cm"
  )
}

plot_rio_deciles(7)
plot_rio_deciles(8)
plot_rio_deciles(9)

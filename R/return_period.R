box::use(
  moments,
  stats[sd,qnorm]
         )

#' lp3_rv
#'
#' @param x `numeric` vector containing block (year) maxima values
#' @param return_period  `numeric` vector containing return periods to calculate return values for
#'
#' @return
#' @export
#'
#' @examples
rv_lp3 <- function(x,return_period){
  params <- lp3_params(x=x)
  g <-  params$g
  mu <- params$mu
  stdev <- params$sd

  rp_exceedance <- 1 / return_period
  q_rp_norm <- qnorm(1 - rp_exceedance, mean = 0, sd = 1)  # Normal quantiles
  k_rp <- (2/g)*(((q_rp_norm-(g/6))*(g/6)+1)^3-1)  # Skewness adjustment
  y_fit_rp <- mu + k_rp * stdev  # Fitted values for return periods
  quantiles_rp <- 10^y_fit_rp
}



#' lp3_params
#'
#' @param x `numeric` vector
#' @param imputation_method `character` method to impute zeros when/if present in the maxima dataset
#'     These are pretty rare, because by taking the `max()` per year we usually end up w/ non-zero values when
#'     aggregated. Nonetheless, it will happen. Current (and only) option is set to `lowest` which will replace a true 0
#'     value with the next lowest number. **Note** this produces a warning message when creating the empirical linear
#'     linear interpolation functions downstream as it will trim the pairs to unique values.
#'
#' @return list containing parameters needed for log pearson type III distribution calculations
#' @details:
#' Video link with methodology details shared by John (AER)
#' log-pearson III
#' fits extreme values/flooding/discharge very well
#' recommendation from USGS: https://www.youtube.com/watch?v=HD2tEZw18EE

lp3_params <- function(x, imputation_method = "lowest"){
  if(imputation_method == "lowest"){
    x[x==0]<- min(x[x!=0])
  }
  x_log = log10(x)
  list(
    mu = mean(x_log),
    sd = sd(x_log),
    g = moments$skewness(x_log)
  )
}

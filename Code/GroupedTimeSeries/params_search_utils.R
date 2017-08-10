library(foreach)
library(dplyr)
library(tidyr)
library(hts)
library(lubridate)

# A function that generates a power set of the given set
# 
# Input: set        - a set for which to generate the power set
#
# Output: powerset - generated power set of the input set

get_powerset <- function(set){
  
  f <- function(set) { 
    n <- length(set)
    masks <- 2^(1:n-1)
    lapply( 1:2^n-1, function(u) set[ bitwAnd(u, masks) != 0 ] )
  }
  
  powerset <- f(set)
  
  return(powerset)
}

# Function that iterates through the space of possible algorithm parameters, and
# returns the parameters that generate the best evaluation metric (e.g., smallest RMSE)
#
# Input:  dataset     - dataset 
#         base_params - a list of initial global parameters
#        
# Output: best_score  - minimum evaluation score obtained for optimal parameters
#         opt_params  - a list of parameters found to be optimal

search_algorithm_parameters <- function(dataset, base_params){
  
  # Initialize the list of parameters
  param_vals <- list()
  
  param_vals$GTSMETHOD <- c("bu", "comb")
  param_vals$COMBHTS_WEIGHTS <- c("ols", "wls", "nseries")
  param_vals$TSMETHOD <-  c("ets", "arima")
  
  # Generate all possible combinations
  combs <- expand.grid(param_vals, stringsAsFactors = FALSE)
  
  
  # Remove ilegal combinations - COMBWEIGHTS only applies to GTSMETHOD = "comb"
  if(!is.null(param_vals$GTSMETHOD) & !is.null(param_vals$COMBHTS_WEIGHTS)){
    rm_inds <- combs$GTSMETHOD != "comb"
    combs$COMBHTS_WEIGHTS[rm_inds] <- param_vals$COMBHTS_WEIGHTS[1]
    keep_inds <- !duplicated.data.frame(combs)
    combs <- combs[keep_inds,]
  }
  
  # Convert combs into parameter space
  nms <- names(combs)
  params_space <- list()
  for (i in 1:dim(combs)[1]){
    
    this_comb <- combs[i,]
    
    params_space[[i]] <- base_params
    for (j in 1:dim(combs)[2]){
      params_space[[i]][nms[j]] <- this_comb[[j]]
      
    }
  } 
  
  
  ## ---------------- Distributing jobs on the Azure pool -------------------------##
  
  result <- foreach(p=params_space, .export = EXPORT2POOL) %dopar% {
    
    # load necessary libraries on compute nodes
    load_libraries()

    # run cross-validation
    crossval_result <- get_crossval_accuracy(dataset, params = p)
    # collect scores across folds
    crossval_scores <- lapply(crossval_result, function(x) x %>% summarise(meanScore = mean(RMSE))) %>% bind_rows()
    # average all scores
    best_score <- mean(crossval_scores$meanScore)
    
  }
  
  
  ## -------------------------- Process the results ----------------------------##
  
  # Gather scores from all runs and find the run that produced the best RMSE
  all_scores <- unlist(result)
  best_score_indx <- which.min(all_scores)
  
  # return best score and best parameters
  return(list(best_score = all_scores[best_score_indx], 
              opt_params = params_space[[best_score_indx]]))
  
}


# Entry point function to parameter optimization
#
# Input:  dataset      - dataset 
#
# Output: opt_params  - a list of parameters found to be optimal
#         best_score    - minimum RMSE obtained for optimal parameters

run_parameter_optimization <- function(dataset){
  
  
  # Initialize values in repeat
  curr_score <- Inf
  best_score <- curr_score
  base_params <- make_params()
  
  # Get initial Score
  cat("Getting initial score ... \n")
  crossval_result <- get_crossval_accuracy(dataset, base_params)
  
  crossval_scores <- lapply(crossval_result, function(x) x %>% summarise(meanScore = mean(RMSE))) %>% bind_rows()
  best_score <- mean(crossval_scores$meanScore)
  opt_params <- base_params
  
  print(paste("Initial best score: ", best_score))
  
  cat("Searching algorithm parameters ... \n")
  search_results <- search_algorithm_parameters(dataset, opt_params)
    
  print(paste("Current best score: ", best_score))
  print(paste("Found best score: ", search_results$best_score))
    
  curr_score <- search_results$best_score
    
  if(curr_score < best_score) {
    best_score <- curr_score
    opt_params <- search_results$opt_params
  } 
  
  # Return optimal parameters and best score (e.g., minimum RMSE)
  return(list(opt_params = opt_params,
              best_score   = best_score))
  
}

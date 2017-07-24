#####################################################################
##############   User defined functions and global vars     #########
#####################################################################


# Function defines forecast parameters
# 
# Output: params - a list containing the full set of parameters

make_params <- function() {
  
  # Initialize parameter list
  params <- list()

  # Number of months to forecast, forecasting horizon
  params$HORIZON <- 3

  # Number of months history to use for computing evaluation metrics
  params$EVALUATION_WINDOW <- 3

  # Method to use for Hierarchical clustering:
  # bu - Bottom up
  # comb - Optimal combination 
  # tdgsa, tdgsf, tdfp - Top down approaches
  params$GTSMETHOD <- "comb"
  
  # Method to use for univariate time series forecasting for individual nodes in the tree: 
  # arima - Arima
  # ets - Exponential smoothing
  params$TSMETHOD <- 'ets'
  
  # Weights used when GTSMETHOD = "comb"
  # c("ols", "wls", "nseries")
  params$COMBHTS_WEIGHTS <- 'ols'
  
  # Time series frequency, currently only monthly
  params$FREQUENCY <- 12
  
  # Variables to do forecasting across
  params$FORECASTING_VARS <- c('CustomerName', 'ProductCategory', 'Destination')
  
  return(params)
}



# Function that processes the data 
# 
# Input: dataset  - dataset 
#        params   - a list of global parameters used throughout the project
#
# Output: dataset - processed dataset

process_dataset <- function(dataset){
  
  dataset <- dataset %>% 
    mutate(Date = as.Date(Date)) %>%
    na.omit()
  
  return(dataset)
}


# Function that computes root mean squared error (RMSE)
#
# Input: forecast - a vector containing forecasted values
#        actual   - a vector containing actual values
#
# Output: e       - root mean squared scalar

rmse <- function(forecast, actual){
  if (length(forecast) != length(actual)) {
    return (NA);
  } else if (length(forecast) == 0 || length(actual) == 0) {
    return (NA);
  }
  else{
    e <- sqrt(mean((actual - forecast)^2))
  }
  
  return(e)
}

# Function that returns forecast for horizon h, given training data
#
# Input: data_train - training data
#        params     - a list of global parameters used throughout the project
#        univariate - logical indicator whether the time series si univariate or not
#
# Output: fcasts  - forecasts for horizon h


make_forecast <- function(data_train, params, univariate=FALSE){
  
  if (univariate){
    fit <- auto.arima(data_train)
    fcasts <- forecast(fit, h = params$HORIZON)
    fcasts <- fcasts$mean
    
    fcasts[fcasts < 0] <- 0
    
  } else {
    
    # Forecast the next HORIZON months based on the training data
    fcasts <- forecast.gts(data_train,  
                           h=params$HORIZON, 
                           method=params$GTSMETHOD, 
                           fmethod = params$TSMETHOD, 
                           weights = params$COMBHTS_WEIGHTS)
    # remove negative values from forecast
    fcasts$bts[fcasts$bts < 0] <- 0
  }
  
  return(fcasts)
  
}

# Function that formats the output of make_forecast into a data frame
#
# Input: fcasts     - output of make_forecast()
#        ts_names   - the names of time series (returned by create_gts_data)
#        univariate - logical indicator whether the time series si univariate or not
#
# Output: fcast_output  - forecast data frame

format_fcast <- function(fcasts, ts_names, univariate=FALSE){
  
  if (univariate){
    
    fts <- fcasts
    
  } else {
    
    fts <- fcasts$bts
  }
  
  fcast_labels <-  as.yearmon(time(fts))
  bfcasts <- as.data.frame(t(fts))
  colnames(bfcasts) <- fcast_labels
  fcast_output <- cbind(ts_names, bfcasts)
  rownames(fcast_output) <- NULL
  
  return(fcast_output)
  
}


# A helper function that fills in missing values in an inclomplete time series with zeros
# 
# Input: ts_data  - a time series data frame generated in create_ts_data() function 
#        ts_seq   - a sequence of dates for which to complete the time series
#
# Output: complete_ts - a complete ts data frame with no missing values

complete_ts <- function(ts_data, ts_seq){
  
  # merge data with full time sequence
  ts_seq <- data.frame(Date = ts_seq)
  complete_ts <- ts_data %>% right_join(ts_seq, by = "Date")
  
  # find NAs introduced by the merge
  nas <- !complete.cases(complete_ts)
  q <- which(names(complete_ts) == "Quantity")
  grp <- unique(complete_ts[!nas, -q])
  
  # fill in the NAs
  complete_ts$Quantity[nas] <- 0
  grp <- unique(complete_ts[!nas, -q])
  complete_ts[nas, -q] <- sapply(grp, rep.int, times=sum(nas))
  
  return(complete_ts)
  
}



# Function that creates grouped time series object from a data set
# 
# Input: dataset  - data set
#        params   - a list of global parameters used throughout the project
#
# Output: all_ts  - grouped time series (gts) or ordinary time series (ts) object

create_gts_data <- function(dataset, params){
  
  keep_vars <- c('Date', params$FORECASTING_VARS)
  dots <- lapply(keep_vars, as.symbol)
  
  # Aggregate to monthly data
  bts <- dataset %>% 
    dplyr::select(Date, one_of(params$FORECASTING_VARS), Quantity) %>% 
    dplyr::group_by_(.dots=dots) %>% 
    dplyr::summarise(Quantity = sum(Quantity)) %>%
    tidyr::unite(unique_group, -Date, -Quantity) %>% 
    tidyr::spread(unique_group, Quantity, fill=0) %>%
    dplyr::arrange(Date) %>%
    dplyr::ungroup()
  
  ts_start <- bts$Date[1]
  
  bts <- bts %>% select(-Date)
  
  # Create multivariate time series object
  all_ts <- ts(bts, start = c(year(ts_start), month(ts_start)), frequency = params$FREQUENCY)
  
  
  # Create grouped time series object 
  if(length(params$FORECASTING_VARS) == 1){ # Handling 1-level deep hierarchy
    
    unique_groups <- data.frame(colnames(bts))
    colnames(unique_groups) <- c(params$FORECASTING_VARS)
    
    if(dim(all_ts)[2] == 1){ # univariate ts (special case)
      all_bts <- all_ts
    }
    else {
      all_bts <- gts(all_ts)
      all_bts$labels[[params$FORECASTING_VARS]] <- colnames(all_bts$groups)
      
    }
    
    
  } else{
    
    unique_groups <- data.frame(ug = colnames(bts)) %>%
      tidyr::separate(col=ug, params$FORECASTING_VARS, sep="_")
    
    novar <- unique_groups %>% summarise_each(funs(n_distinct))
    rmcol <- which(novar == 1)
    
    if(length(rmcol)>0) {
      bts_groups <- unique_groups[, -rmcol]
    } else {
      bts_groups <- unique_groups
    }
    
    
    bts_groups <- t(as.matrix(bts_groups))
    all_bts <- gts(all_ts, groups = bts_groups)
    
  }
  
  # Return time series and the time series names
  return(list(ts = all_bts, ts_names = unique_groups))
  
}


# Function that computes cross validation on the data using RMSE
#
# Input: dataset  - dataset 
#        params   - a list of global parameters used throughout the project
#        nfolds   - number of folds for cross-validation
#
# Output: dataframe containing computed accuracy measures and the original dataset

get_crossval_accuracy <- function(dataset, params, nfolds = 3){
  
  # Always forecasting 1 month ahead for cross-validation
  horizon = 1
  
  # Assume it's multivariate ts
  univariate = FALSE
  
  ############# SPLIT TRAINING AND TESTING ############
  
  # Create time series data set
  ts_data <- create_gts_data(dataset, params)
  all_ts <- ts_data$ts
  
  # Check if the ts is univariate
  if(dim(ts_data$ts_names)[1] == 1) univariate = TRUE
  
  if(univariate){
    tseries <- all_ts
  } else{
    tseries <- all_ts$bts
  }
  
  time_points <- time(tseries)
  
  test_indices <- as.list((length(time_points) - (horizon - 1) - (nfolds - 1)) : (length(time_points) - (horizon - 1)))
  
  # Modify the default horizon
  tmp_params <- params
  tmp_params$HORIZON <- horizon
  
  eval_list <- lapply(test_indices, 
                      
                      function(tstart) {
                        
                        # --- Forecast on training set
                        # Get training data
                        data_train <- window(all_ts, start = time_points[1], end=time_points[tstart-1])
                        
                        # Compute the MAE for a naive forecast on each training series
                        # These will be used in the MASE calculation
                        first_train_date <- as.Date.yearmon(time_points[1])
                        last_train_date <- as.Date.yearmon(time_points[tstart-1])
                        
                        
                        # Forecast the next HORIZON months based on the training data
                        fcasts <- make_forecast(data_train, tmp_params, univariate)
                        
                        # Format the forecasts for the input to further functions
                        final_fcast <- format_fcast(fcasts, ts_data$ts_names, univariate)
                        
                        month_cols <- (length(params$FORECASTING_VARS)+1):dim(final_fcast)[2]
                        colnames(final_fcast)[month_cols] <- paste0('fcast', 1:length(month_cols)) -> fcast_labs
                        
                        # --- Extract actuals (this needs to be done from dataset, since the ts object does not contain the bottom level ts)
                        
                        first_test_date <- as.Date.yearmon(time_points[tstart])
                        last_test_date <- as.Date.yearmon(time_points[tstart + horizon - 1])
                        
                        id_vars <- c(params$FORECASTING_VARS, 'Date')
                        id_vars <- lapply(id_vars, as.symbol)
                        
                        data_test <- dataset %>% 
                          filter(Date >= first_test_date & Date <= last_test_date) %>%
                          group_by_(.dots=id_vars) %>% 
                          summarise(Quantity = sum(Quantity)) %>%
                          arrange(Date) %>%
                          spread(Date, Quantity, fill=0)
                        
                        colnames(data_test)[month_cols] <- paste0('actual', 1:length(month_cols)) -> actual_labs
                        
                        # --- Compute evaluation metrics
                        
                        # Merge testing and forecasted data
                        merge_data <- data_test %>%
                          full_join(final_fcast, by=params$FORECASTING_VARS) %>%
                          as.data.frame()
                        
                        merge_data[is.na(merge_data)] <- 0 
                        
                        # Compute evaluation metrics across the horizon for each time series 
                        merge_data$RMSE <- apply(merge_data, 1, function(x) rmse(as.numeric(x[fcast_labs]), as.numeric(x[actual_labs])))
                        
                        return(merge_data)
                      }
  )
  
  return(eval_list)
  
}


# Helper function to load necessary libraries on the compute nodes on Azure batch
load_libraries <- function(){
  library(dplyr)
  library(lubridate)
  library(hts)
  library(tidyr)
}


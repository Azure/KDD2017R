# ================================================ #
# Main script to run parameter search optimization 
# To run: source("main.R)
# ================================================ #

# Load necessary functions
source('forecast_utils.R')
source('params_search_utils.R')
# Set global variables
RUN_LOCAL <- TRUE
EXPORT2POOL <- c(as.vector(lsf.str()), 'RUN_LOCAL')

# Reading the dataset
dataset <- read.csv(file = 'ExampleDemandData.csv', stringsAsFactors = FALSE)
dataset <- process_dataset(dataset)

if(RUN_LOCAL){

    paramopt_result <- run_parameter_optimization(dataset)

} else {
  
  # 1. Generate your credential and cluster configuration files.  
  # generateClusterConfig("cluster.json")
  # generateCredentialsConfig("credentials.json")
  
  # 2. Fill out your credential config and cluster config files.
  # Enter your Azure Batch Account & Azure Storage keys/account-info into your credential config ("credentials.json") 
  # and configure your cluster in your cluster config ("cluster.json")
  
  # 3. Set your credentials - you need to give the R session your credentials to interact with Azure
  setCredentials("credentials.json")
  
  # 4. Register the pool. This will create a new pool if your pool hasn't already been provisioned.
  cluster <- makeCluster("cluster.json")
  
  # 5. Register the pool as your parallel backend
  registerDoAzureParallel(cluster)
  # Turn on or off verbose mode
  setVerbose(FALSE)
  
  # 6. Check that your parallel backend has been registered
  getDoParWorkers()
  
  # 7. Run your code
  paramopt_result <- run_parameter_optimization(dataset)
  
  # 8. Delete the pool
  # stopCluster(cluster)
}


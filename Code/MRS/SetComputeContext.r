if(file.exists("/dsvm"))
{
  # Set environment variables for the Data Science VM
  Sys.setenv(SPARK_HOME = "/dsvm/tools/spark/current",
             HADOOP_HOME = "/opt/hadoop/current",
             YARN_CONF_DIR = "/opt/hadoop/current/etc/hadoop", 
             PATH = paste0(Sys.getenv("PATH"), ":/opt/hadoop/current/bin"),
             JAVA_HOME = "/usr/lib/jvm/java-1.8.0-openjdk-amd64"
  )
} else {
  Sys.setenv(SPARK_HOME="/usr/hdp/current/spark2-client")
}




################################################
# Use Hadoop-compatible Distributed File System
# N.B. Can be used with local or RxSpark compute contexts
################################################

rxOptions(fileSystem = RxHdfsFileSystem())

dataDir <- "/share"
  




rxRoc <- function(...){
  previousContext <- rxSetComputeContext(RxLocalSeq())
  
  # rxRoc requires local compute context
  roc <- RevoScaleR::rxRoc(...)
  
  rxSetComputeContext(previousContext)
  
  return(roc)
}

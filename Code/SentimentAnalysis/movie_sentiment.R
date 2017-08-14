# set the working directory
setwd("/data/movie")

library(readr)

set.seed(1)

# -----------------------------------------
# Download IMDB moview reviews data
# -----------------------------------------
destfile <- 'aclImdb_v1.tar.gz'
if(!file.exists(destfile)){
  download.file('http://ai.stanford.edu/~amaas/data/sentiment/aclImdb_v1.tar.gz', destfile = destfile)
  untar('aclImdb_v1.tar.gz') # this can take a few minutes
}

# -----------------------------------------
# Data Processing
# -----------------------------------------
fnames <- list.files("aclImdb", pattern = "[[:digit:]_]+.txt", recursive = TRUE, full.names = TRUE)
fnames <- grep(pattern = 'unsup', fnames, invert = TRUE, value = TRUE)
fnames <- fnames[sample(length(fnames))]

getIntSentiment <- function(fname) {
    tname=unlist(strsplit(fname, "[.]"))[1]
    as.integer(unlist(strsplit(tname, "_"))[2])
}

df <- data.frame(fname=fnames, review = sapply(fnames, read_file),
              sentiment= sapply(fnames, getIntSentiment)>5, stringsAsFactors = FALSE)

# -----------------------------------------
# Split data into training and testing
# -----------------------------------------
trainxdf <- RxXdfData('train.xdf')
testxdf <- RxXdfData('test.xdf')
trainInd <- grep(pattern="train", fnames, value=FALSE)
testInd <- c(1:50000)[-trainInd]

# -----------------------------------------
# Get sentiment from the pre-trained model as a feature
# -----------------------------------------
rxFeaturize(data=df[trainInd,], outData = trainxdf,
            mlTransforms = list(getSentiment(vars = c(preSentiment="review"))),
            overwrite = TRUE, randomSeed = 1)

rxFeaturize(data=df[testInd,], outData = testxdf,
            mlTransforms = list(getSentiment(vars = c(preSentiment="review"))),
            overwrite = TRUE, randomSeed = 1)


# -----------------------------------------
# Train models
# -----------------------------------------
form1 <- sentiment~reviewTran
form2 <- sentiment~reviewTran+preSentiment

ft <- list(featurizeText(vars=c(reviewTran="review"), language = "English",
                      stopwordsRemover = stopwordsDefault(),
                      case = "lower", keepDiacritics = FALSE, keepPunctuations = FALSE,
                      keepNumbers = TRUE, dictionary = NULL,
                      wordFeatureExtractor = ngramCount(), charFeatureExtractor = NULL,
                      vectorNormalizer = "l2"))

rm(list=ls(pattern = 'model\\.')) # removing any previously built models
model.rxlr1=rxLogisticRegression(form1, trainxdf, type='binary', mlTransforms = ft)
model.rxlr2=rxLogisticRegression(form2, trainxdf, type='binary', mlTransforms = ft)


# -----------------------------------------
# Score test data
# -----------------------------------------
scores <- rxImport(testxdf, overwrite = TRUE)
for (modelname in ls(pattern = 'model\\.')) {
    model <- get(modelname)
    scores <- rxPredict(model, scores, extraVarsToWrite =  names(scores), overwrite = TRUE,
                       suffix = paste(' ', model$Description, model$params$Formula, sep='.'))
}

rxRocCurve("sentiment", grep("Probability", names(scores), value=TRUE), scores)


# -----------------------------------------
# Parallelized Scoring
# -----------------------------------------

# Store files in HDFS
fs <- RxHdfsFileSystem()

# Start in local compute context
rxSetComputeContext("local")

# Save the model
saveRDS(model.rxlr2, "/tmp/sentimentModel.rds")

# Create 4 XDF data sets in HDFS totaling 25k rows
numXdfs <- 4
testData <- df[testInd,]
totalRows <- nrow(testData)
rowsPerSet <- totalRows / numXdfs

for(setNum in 1:numXdfs) {
  firstRow <- (setNum - 1) * rowsPerSet + 1
  lastRow <- setNum * rowsPerSet
  testSubset <- testData[firstRow:lastRow,]
  testSubsetXdf <- RxXdfData(paste0("/tmp/testSubsetXdf-s-", setNum), fileSystem = fs)
  # Write XDF to HDFS
  rxDataStep(testSubset, testSubsetXdf, overwrite = T)
}

# Define the scoring function
scoreFn <- function(setNum) {
  fs <- RxHdfsFileSystem()

  # Load the scoring model
  modelFileName <-"/tmp/sentimentModel.rds"
  model <- readRDS(modelFileName)

  # Input data
  testSubsetXdf <- RxXdfData(paste0("/tmp/testSubsetXdf-s-", setNum), fileSystem = fs)

  # Featurized data
  featurizedXdf <- RxXdfData(paste0("/tmp/featurizedXdf-s-", setNum), fileSystem = fs)

  # Featurize
  rxFeaturize(data=testSubsetXdf, outData = featurizedXdf,
              mlTransforms = list(getSentiment(vars = c(preSentiment="review"))),
              overwrite = TRUE, randomSeed = 1)

  # Scores
  scoresXdf <- RxXdfData(paste0("/tmp/testSubsetScoresXdf-s-", setNum), fileSystem = fs)

  # Score
  rxPredict(model, data = featurizedXdf, outData = scoresXdf,
            extraVarsToWrite = c("fname"), overwrite = T)
}

# Set environment variables for Spark
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

# Launch Spark on YARN
cc <- rxSparkConnect(reset = TRUE,
                     consoleOutput = TRUE,
                     # numExecutors = 1,
                     executorCores = 2,
                     driverMem = "1g",
                     executorMem = "1g",
                     executorOverheadMem = "6g"
)

# Perform scoring in parallel using rxExec
scoreFiles <- rxExec(scoreFn, setNum = rxElemArg(1:numXdfs))

# Shut down the Spark application
rxSparkDisconnect(cc)

# Check the contents of the first scores file
scoresXdf <- scoreFiles[[1]]
rxGetInfo(scoresXdf, getVarInfo = T, numRows = 3)

# Sample output from rxGetInfo:

# File name: /tmp/testSubsetScoresXdf-s-1
# Number of composite data files: 1
# Number of observations: 6250
# Number of variables: 4
# Number of blocks: 1
# Compression type: none
# Variable information:
#   Var 1: fname, Type: character
# Var 2: PredictedLabel, Type: logical, Low/High: (0, 1)
# Var 3: Score, Type: numeric, Storage: float32, Low/High: (-11.3273, 9.0390)
# Var 4: Probability, Type: numeric, Storage: float32, Low/High: (0.0000, 0.9999)
# Data (3 rows starting with row 1):
#   fname PredictedLabel      Score Probability
# 1 aclImdb/test/pos/10695_10.txt           TRUE  0.9811620 0.727338731
# 2   aclImdb/test/pos/4242_9.txt           TRUE  0.7777346 0.685191691
# 3   aclImdb/test/neg/7823_1.txt          FALSE -4.8393278 0.007850257

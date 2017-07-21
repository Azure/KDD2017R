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

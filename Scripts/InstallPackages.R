# options(repos = "https://mran.microsoft.com/snapshot/2017-05-01")

# install.packages("sparklyr")
# install.packages("ggplot2")
# install.packages("gridExtra")
# install.packages("rmarkdown")
# install.packages("knitr")
# install.packages("formatR")
# install.packages("tidyr")

install.packages("bigmemory")
install.packages("biganalytics")
install.packages("ff")
install.packages("ffbase")
install.packages("biglm")

install.packages("hts", repos='https://mran.revolutionanalytics.com/snapshot/2016-11-01')
install.packages("fpp", repos='https://mran.revolutionanalytics.com/snapshot/2016-11-01')

library(devtools)
install_github(c("Azure/rAzureBatch", "Azure/doAzureParallel"))

# install.packages("ggmap", dependencies=TRUE)

#######################################################################################################################################
#######################################################################################################################################
## THIS SCRIPT CUSTOMIZES THE DSVM BY ENABLING HADOOP AND YARN, INSTALLING R PACKAGES, AND DOWNLOADING DATA SETS 
## FOR THE KDD 2017 HANDS-ON TUTORIAL.
#######################################################################################################################################
#######################################################################################################################################

## DOWNLOAD CODE FILES
cd /home/remoteuser
git clone https://github.com/Azure/KDD2017R.git

#######################################################################################################################################
## Setup autossh for hadoop service account
#######################################################################################################################################
echo -e 'y\n' | ssh-keygen -t rsa -P '' -f ~hadoop/.ssh/id_rsa
cat ~hadoop/.ssh/id_rsa.pub >> ~hadoop/.ssh/authorized_keys
chmod 0600 ~hadoop/.ssh/authorized_keys
chown hadoop:hadoop ~hadoop/.ssh/id_rsa
chown hadoop:hadoop ~hadoop/.ssh/id_rsa.pub
chown hadoop:hadoop ~hadoop/.ssh/authorized_keys

# Increase vmem ratio limit
cp /home/remoteuser/KDD2017R/Scripts/yarn-site.xml /opt/hadoop/current/etc/hadoop

# move HDFS directories to /data
cp /home/remoteuser/KDD2017R/Scripts/hdfs-site.xml /opt/hadoop/current/etc/hadoop
mkdir -p /data/hadoop
chown hadoop:hadoop /data/hadoop
mv /opt/hadoop/data /data/hadoop

#######################################################################################################################################
## Start up several services, yarn, hadoop, rstudio server
#######################################################################################################################################
systemctl start hadoop-namenode hadoop-datanode hadoop-yarn rstudio-server

#######################################################################################################################################
## MRS Deploy Setup
#######################################################################################################################################
cd /usr/lib64/microsoft-r/rserver/o16n/9.1.0
dotnet Microsoft.RServer.Utils.AdminUtil/Microsoft.RServer.Utils.AdminUtil.dll -silentoneboxinstall KDD2017+halifax

#######################################################################################################################################
# Copy data to VM
#######################################################################################################################################

cd /data
mkdir airline
cd airline

# Airline data
wget http://cdspsparksamples.blob.core.windows.net/data/Airline/WeatherSubsetCsv.tar.gz
wget http://cdspsparksamples.blob.core.windows.net/data/Airline/AirlineSubsetCsv.tar.gz

tar -xzf WeatherSubsetCsv.tar.gz
tar -xzf AirlineSubsetCsv.tar.gz

rm WeatherSubsetCsv.tar.gz AirlineSubsetCsv.tar.gz

# Make hdfs directories
/opt/hadoop/current/bin/hdfs dfs -mkdir -p /user/RevoShare/remoteuser/Data

# Copy data to HDFS
/opt/hadoop/current/bin/hdfs dfs -copyFromLocal WeatherSubsetCsv AirlineSubsetCsv /user/RevoShare/remoteuser/Data

rm -rf WeatherSubsetCsv AirlineSubsetCsv

wget http://strata2017r.blob.core.windows.net/airline/airline_20MM.csv

# Data directory for movie sentiment analysis
mkdir /data/movie

# Data directory for learning curves
mkdir /data/learning_curves

# Make directory used by Spark compute context
mkdir -p /var/RevoShare/remoteuser

# Create /tmp directory in HDFS
/opt/hadoop/current/bin/hdfs dfs -mkdir /tmp
/opt/hadoop/current/bin/hdfs dfs -chmod 777 /tmp

#######################################################################################################################################
#######################################################################################################################################
## Change ownership of some of directories
chown -R remoteuser:remoteuser /home/remoteuser/KDD2017R
chown -R remoteuser:remoteuser /data/airline
chown -R remoteuser:remoteuser /data/movie
chown -R remoteuser:remoteuser /data/learning_curves
chown remoteuser:remoteuser /var/RevoShare/remoteuser

sudo -u hadoop /opt/hadoop/current/bin/hdfs dfs -chown -R remoteuser /user/RevoShare/remoteuser

#######################################################################################################################################
#######################################################################################################################################
# Install R packages
Rscript /home/remoteuser/KDD2017R/Scripts/InstallPackages.R

#######################################################################################################################################
#######################################################################################################################################
## END
#######################################################################################################################################
#######################################################################################################################################

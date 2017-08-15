\# This repository has content for the KDD 2017 tutorial "Using R for Scalable Data Science"

## Tutorial Prerequisites
* Please bring a wireless enabled laptop.
* Make sure your machine has an ssh client with port-forwarding capability. On Mac or Linux, simply run the ssh command in a terminal window.
On Windows, download [plink.exe](https://the.earth.li/~sgtatham/putty/latest/x86/plink.exe)
from http://www.chiark.greenend.org.uk/~sgtatham/putty/download.html.

## Steps to run the tutorial of In-Database Advanced Analytics

The first section of this half-day tutorial is the in-database advanced analytics in SQL Server 2016 with Microsoft R. You will be using a Jupyter notebook running on an Azure virtual machine with R kernel. The jupyter notebook will connect to a SQL Server hosted on another Azure virtual machine. Both Jupyter Notebook server and SQL Server virtual machines have been created for you. You will need the information on the paper clip handed out to you when you enter the tutorial room. Since multiple users will be using the same Jupyter Notebook server (10 servers created), and the same SQL Server (5 servers created), please follow the following steps as much as you can, to minimize the interference with other users on the same machine. 

Step 1. Open https://\<ip address xxx.xxx.xxx.xxx\>:9999 from a browser, Ignore security warnings.

Step 2. Input password: **Kdd2017@Halifax** when prompted

Step 3. Click on ***Prepare Jupyter Notebooks.ipynb*** to open. Click "Continue" to ignore any security warning.

Step 4.	In the first cell, input a directory name that should be unique to you, and has high odds to be unique among the all audience.

Step 5.	Run the cells one by one, all run all of them in a batch. Press SHIFT-ENTER to run each cell, or got to the Cell menu and click “Run All”. After running all cells, go to the File menu and click “Close and Halt”.

Step 6.	There should be a directory created with the name you provided in Step 4. Open the SQL_R_Services_End….ipynb **UNDER THAT DIRECTORY**. Ignore any security warning.

Step 7.	In the first cell, input the following information about a SQL server that is provided on the paper clip under the line of **SQL Server Information**:
	
	IP: xx.xxx.xxx.xxx
	UID: sqluserxxxx
	PWD: Kdd17@Halifax_xxxxx

## Connecting to the Data Science Virtual Machine on Microsoft Azure
We will provide Azure Data Science Virtual Machines (running Spark 2.1.1) for attendees to use during the tutorial. You will use your laptop to connect to your allocated virtual machine.

* **On Windows:** command line to connect with plink.exe - run the following commands in a Windows command prompt window - replace XXX with the IP address of your Data Science Virtual Machine [e.g. 40.80.111.222]
```bash
cd directory-containing-plink.exe
.\plink.exe -L localhost:8787:localhost:8787 -L localhost:8088:localhost:8088 remoteuser@XXX
```
* **On Linux or Mac:** command line to connect with ssh - replace XXX with the IP address of your Data Science Virtual Machine [e.g. 40.80.111.222]
```bash
ssh -L localhost:8787:localhost:8787 -L localhost:8088:localhost:8088 remoteuser@XXX
```
* After connecting via the above command lines, open [http://localhost:8787/](http://localhost:8787/) in your web browser to connect to **RStudio Server** on your Data Science Virtual Machine<br>
* You can also open [http://localhost:8088/](http://localhost:8088/) in your web browser to connect to the **YARN User Interface** on the Data Science Virtual Machine to monitor YARN applications and node health<br>
* Note that the terminal window with ssh or plink is only needed to provide a secure tunnel to the Data Science Virtual Machine

## Locating and Running the Tutorials on the Data Science Virtual Machine
In the RStudio *Files* pane, click **KDD2017R** and then **Code**
1. **MRS** directory: Run **1-Clean-Join.r, 2-Train-Test.r, 3-Deploy-Score.r**. In 3-Deploy-Score.r on line 43, please **replace "INSERT PASSWORD HERE" with "KDD2017+halifax"**
2. **learning_curves** directory: Run **gibberish_hdinsight_rxFastLinear.Rmd**
3. **GroupedTimeSeries** directory: Run **main.R**
4. **SentimentAnalysis** directory: Run **movie_sentiment.R**

<hr>



**TITLE: Using R for Scalable Data Science: Single Machines to Hadoop Spark Clusters**

PRESENTERS: Robert Horton, Mario Inchiosa, Vanja Paunic, and Hang Zhang

HANDS-ON TUTORIAL DURATION: 3 hours

TARGET AUDIENCE:  Intermediate level in knowledge and practice of machine learning and R

**ABSTRACT**

In this tutorial, we will demonstrate how to create scalable, end-to-end data analysis processes in R on single machines as well as in-database in SQL Server and on Hadoop clusters running Spark. We will provide hands-on exercises as well as code in a public GitHub repository for attendees to adopt in their data science practice. In particular, the attendees will see how to build, persist, and consume machine learning models using distributed machine learning functions in R. 

R is one of the most used languages in the data science, statistical and machine learning (ML) community. Although open-source R (CRAN library) now has in excess of 10,000 packages and functions for statics and ML, when it comes to scalable analysis using R, or deployment of trained models into production, many data scientists are blocked or hindered by (a) its limitations of available functions to handle large datasets efficiently, and (b) knowledge about the appropriate computing environments to scale R scripts from desktop analysis to elastic and distributed cloud services. In this tutorial, we will discuss how to create end-to-end data science solutions that utilize distributed compute resources. During the tutorial, we will provide presentations, worked-out examples, and hands-on exercises with sample code. In addition, we will provide a public GitHub code repository that attendees will be able to access and adapt to their own practice. We believe this tutorial will be of strong interest to a large and growing community of data scientists and developers who are using R for creating and deploying analytical solutions.   

**TUTORIAL OUTLINE**
1.	Introduction: Scaling your R scripts - issues and solutions
    
    a.	What limits the scalability of R scripts?
    
    b.	What functions and techniques can be used to overcome those limits?
    
    c.	How do the base and scalable approaches compare? 

2.	Hands-on exercises and demonstrations: End to end scalable data process in R
Data exploration, wrangling, visualization, modeling and deployment using R on single node virtual machines and Hadoop clusters running Spark

    a.	Scalable analysis on single nodes: Analysis with data on disk, in-database, and in Spark

    b.	Analytics in distributed Hadoop/Spark environment: Data exploration, wrangling, distributed training, model evaluation

    c.	Deployment of ML models as web-services APIs

3.	Practical examples and case studies

    a.	Distributed model training and parameter optimization

    b.	Grouped time series forecasting: distributed parameter optimization and model deployment 

    c.	Semantic analysis using deep learning




# Contributing

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

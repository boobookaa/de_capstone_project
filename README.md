# Overview
Amazon sells tens and hundreds thousands of goods every day. After selling a customer can leave a review about the item purchased. All these reviews help other people to make the right choice. Also they can be used as an excellent source of data for marketing experts and other specialists from Amazon. Everyday customers generate huge amounts of data. So their analysis can be a bit complicated. Usually before the analysis data are cleared and transformed to the proper form. In many cases it means performing a lot of different operations and transformations. 
The goal of this capstone project is building an analytical Data Warehouse. Customer reviews have been chosen as a subject of analysis. 
The Data Warehouse can answer to the next questions:
How do ratings vary with different options, for example verified purchases, marketplaces or product categories
How have number of ratings changed over time
Are ratings helpful
Also it is possible to take a look at the reviewer behavior or provide sentiment analysis.

# Data sources
## Amazon Customer Reviews Dataset
In a period of over two decades since the first review in 1995, millions of Amazon customers have contributed over a hundred million reviews to express opinions and describe their experiences regarding products on the Amazon.com website. Over 130+ million customer reviews are available to researchers as part of this dataset.
More: https://registry.opendata.aws/amazon-reviews/ and full documentation: https://s3.amazonaws.com/amazon-reviews-pds/readme.html
## Country
Country dimension is provided in the JSON file: s3://brutway-capstone-project/country/country-and-continent-codes-list.json

# Prerequisites and Environment Setup
The project has been developed using Apache Airflow, AWS S3, Amazon Redshift Database and AWS EMR.
Amazon Customer Reviews Dataset resides in the AWS S3 bucket in us-east-1 region. Copying between S3 buckets can be possible if all the buckets are in the same region. So Storage Area must be organized in us-east-1 region too. For performance improvement I resided the EMR cluster and Redshift Database in us-east-1 region too.

## Apache Airflow
Apache Airflow - is a platform to programmatically author, schedule and monitor workflows. One of the well known installations of Airflow is puckel/docker-airflow. I have used this docker image for my workflows orchestration. But there are other possibilities for Apache Airflow deployment. For example AWS Cloud Formation or your own installation.

The next preliminary steps must be performed before Airflow using

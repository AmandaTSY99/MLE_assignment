# CS611 Assignment 1: Data Processing Pipelines

## How to run
1. Clone the repository
```git clone https://github.com/AmandaTSY99/MLE_assignment.git```
2. Open the file directory in vscode
3. In the vscode terminal, build the docker image
```docker-compose build```
3. In the same terminal, run an instance of the image
```docker-compose up```
4. Go to the link to JupyterLab provided
5. From the Launcher in JupyterLab, open Terminal
6. Run the main.py script in the Terminal
```python main.py```

Wait for the script to finish running and you should see a new datamart file with bronze, silver and gold files. 

## About this assignment
In this assignment, my role is a data scientist who works as a financial institute, and is tasked to build a machine learning model to predict whether a user will default on their loan at the point of application. The data on customers has been provided and the code to process it using the medallion structure has been stored in this repository. Docker image has also been set up to allow for the deployment of container-based application consistently. 

For more information on the data and how to run the script, please find them below. 

## Files in the repo
1. data file contains raw dataset.
2. utils file contains codes for processing from bronze to gold. 
3. docker-compose.yaml, Dockerfile, requirements.txt files are used for setting up docker container. 
4. main.py is the mains script to run to create a datamart containing the data in the different medallion architecture layer. 


## Raw dataset
1. `feature_clickstream.csv` - daily clickstream activity of each customer for 20 anonymised features.
2. ` features_attributes.csv` - general information about each customer, e.g. age, occupation. 
3. `features_financials.csv` - financial-related information about each customer, e.g. annual income, monthly investment amount. 
4. `lms_loan_daily.csv` - monthly information about each loan, e.g. balance, amount paid, etc.



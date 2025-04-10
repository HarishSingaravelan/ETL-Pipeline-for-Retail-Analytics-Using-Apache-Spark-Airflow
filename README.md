# Project-2


## Name
Batch processing of Candy store data.

## Description
This project implements a data pipeline using Apache Airflow. It is designed to automate workflows, including data extraction, transformation, and loading (ETL), as well as running model training tasks and evaluation. This project serves as an example for setting up an Airflow DAG and orchestrating data science tasks.


<!-- ## Visuals
Depending on what you are making, it can be a good idea to include screenshots or even a video (you'll frequently see GIFs rather than actual videos). Tools like ttygif can help, but check out Asciinema for a more sophisticated method. -->
## Required Packages

The following packages are required to run this project:

1. **Apache Airflow** - For orchestrating and managing the workflow.
2. **PySpark** - For processing and analyzing data at scale.
3. **Python-dotenv** - For loading environment variables from a `.env` file.
4. **os** - For handling file system operations and environment variables.
5. **sklearn** - For machine learning tasks, including data preprocessing and modeling.
6. **prophet** -  For time series forecasting and predictive analytics.


## Installation


1. Install Airflow Python package (activate your virtual environment if you have one):
    
    ```
    pip install apache-airflow
    ```
    There might not be a compatible version of apache-airflow with latest python version (3.13.x). If you encounter this issue, you can install python 3.10.x and use that version to install airflow.

2. Set the Airflow home directory (optional, but recommended):
    
    ```
    export AIRFLOW_HOME=~/airflow
    ```
    
3. Initialize the Airflow database:
    
    ```
    airflow db init
    ```
    
4. Create an Airflow user and set up password:
    
    ```
    airflow users create \\
        --username <Your-username> \\
        --firstname <Your-name> \\
        --lastname <Your-name> \\
        --role Admin \\
        --email <Your-email>
    ```
    
    You'll be prompted to set a password for this user.
    
5. Start the Airflow webserver:
    
    ```
    airflow webserver --port 8080
    ```
    
6. Start the Airflow scheduler (in new terminal window):
    
    ```
    airflow scheduler
    ```
    
7. Copy the file named `Airflow_DAG.py`, `main.py`, ` data_processor.py` and `time_series.py` and paste it in the `~/airflow/dags/` directory.
   
8. You can access the Airflow UI by going to `http://localhost:8080` in your web browser.

  - Open a web browser and go to `http://localhost:8080`
  - Log in with the user you created in step 4
 

9.  View DAG jobs:
    - In the Web UI, click on 'example_dag'
    - You'll see the Graph and Tree views showing the status of your tasks
  
10. View results
    1.  See the terminal outputs from Web UI:
        - Navigate to the DAG:
          - From the DAGs view, find and click on the name of your DAG (e.g., hello_world_dag).
          - This will take you to the DAG details page, showing you a graphical view of the DAG with tasks.

        
    2.  Alternatively, you can view logs from the airflow directory:
    
        ```
        cat ~/airflow/logs/dag_id=example_dag/run_id=manual__2024-09-24T02:59:28+00:00/task_id=hello_task/attempt=1.log
        ```
    

## Usage
Example: To retrieve from a .csv file

```
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example").getOrCreate()
df = spark.read.csv("../dataset_9/products.csv", header=True)
df.show()
```
Expected output:

| product_id | product_name                  | product_category | product_subcategory | product_shape | sales_price | cost_to_make | stock |
|------------|--------------------------------|------------------|---------------------|---------------|-------------|--------------|-------|
| 1          | Salted Laces Cubes             | Licorice         | Laces               | Cubes         | 7.45        | 4.21         | 4576  |
| 2          | Black/Red/White Laces Cubes    | Licorice         | Laces               | Cubes         | 3.75        | 2.24         | 3584  |
| 3          | Salted Laces Braids            | Licorice         | Laces               | Braids        | 9.97        | 5.2          | 2031  |
| 4          | Black/Red/White Laces Braids   | Licorice         | Laces               | Braids        | 8.6         | 3.51         | 3057  |
| 5          | Salted Twists Cubes            | Licorice         | Twists              | Cubes         | 0.81        | 0.49         | 2727  |
| 6          | Black/Red/White Twists Cubes   | Licorice         | Twists              | Cubes         | 3.8         | 1.86         | 3839  |
| 7          | Salted Twists Braids           | Licorice         | Twists              | Braids        | 7.96        | 5.4          | 4657  |
| 8          | Black/Red/White Twists Braids  | Licorice         | Twists              | Braids        | 2.34        | 1.47         | 3794  |
| 9          | Salted Bites Cubes             | Licorice         | Bites               | Cubes         | 9.59        | 4.32         | 2052  |
| 10         | Black/Red/White Bites Cubes    | Licorice         | Bites               | Cubes         | 9.11        | 6.34         | 4425  |


## Support
If you need help with this project, feel free to reach out through the following channels:

Issue Tracker: [GitLab Issues](/issues)
Discussion Forum: [GitLab Discussions](/discussions)
Email: hs7569@g.rit.com

For bug reports, feature requests, or general questions, please create an issue with a detailed description.

## Roadmap
Below is the planned roadmap for the project:

Current Features:
- Workflow automation using Apache Airflow
- Large-scale data processing with PySpark
- Time-series forecasting using Prophet

Upcoming Enhancements:
- Adding advanced machine learning model integrations
- Enhancing error handling and logging

If you have suggestions or would like to contribute, feel free to open an issue!


## Contributing
We welcome contributions! Follow these steps to get started:

1. Fork the Repository
Click on the Fork button at the top of the repository page to create a copy of this project in your GitLab account.

2. Clone Your Forked Repository
```
git clone https://github.com/HarishSingaravelan/ETL-Pipeline-for-Retail-Analytics-Using-Apache-Spark-Airflow
```
```
cd project-2
```
3. Create a New Branch
```
git checkout -b feature-branch
```
4. Make Your Changes and Commit
Make your modifications, then commit your changes:
```
git add .
```
```
git commit -m "Added a new feature"
```
5. Push Your Changes
```
git push origin feature-branch
```
6. Create a Merge Request
Go to your GitLab repository and submit a Merge Request to the main branch.



📩 For inquiries regarding usage permissions, please contact Zimeng Lyu at zimenglyu@mail.rit.e

## Project status
This project is no longer actively maintained. If you're interested in contributing or taking over maintenance, please reach out via hs7569@g.rit.edu

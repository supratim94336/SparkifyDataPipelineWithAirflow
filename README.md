## Data Pipelines with Airflow
Getting started with airflow

### Installing and starting

#### Installing Python Dependencies
You need to install this python dependencies
In Terminal/CommandPrompt:  

without anaconda you can do this:
```
$ python3 -m venv virtual-env-name
$ source virtual-env-name/bin/activate
$ pip install -r requirements.txt
```
with anaconda you can do this (in Windows):
```
$ conda env create -f env.yml
$ source activate <conda-env-name>
```
or (in Others)
```
conda create -y -n <conda-env-name> python==3.6
conda install -f -y -q -n <conda-env-name> -c conda-forge --file requirements.txt
[source activate/ conda activate] <conda-env-name>
```
#### Fixing/Configuring Airflow
```
$ pip install --upgrade Flask
$ pip install zappa
$ mkdir airflow_home
$ export AIRFLOW_HOME=./airflow_home
$ cd airflow_home
$ airflow initdb
$ airflow webserver
$ airflow scheduler
```

#### More Airflow commands
To list existing dags registered with airflow
```
$ airflow list_dags
```

#### Secure/Encrypt your connections and hooks
**Run**
```bash
$ python cryptosetup.py
```
copy this key to *airflow.cfg* to paste after   
fernet_key = ************

#### Setting up connections and variables in Airflow UI
TODO: There is no code to modify in this exercise. We're going to 
create a connection and a variable.  

**S3**
1. Open your browser to localhost:8080 and open Admin->Variables
2. Click "Create"
3. Set "Key" equal to "s3_bucket" and set "Val" equal to "udacity-dend"
4. Set "Key" equal to "s3_prefix" and set "Val" equal to "data-pipelines"
5. Click save  

**AWS**
1. Open Admin->Connections
2. Click "Create"
3. Set "Conn Id" to "aws_credentials", "Conn Type" to "Amazon Web Services"
4. Set "Login" to your aws_access_key_id and "Password" to your aws_secret_key
5. Click save

**Redshift**
1. Open Admin->Connections
2. Click "Create"
3. Set "Conn Id" to "redshift", "Conn Type" to "postgres"
4. Set "Login" to your master_username for your cluster and "Password" 
to your master_password for your cluster
5. Click save

#### Links for Airflow
**Macro**  
https://airflow.apache.org/macros.html

**Hacks**  
https://medium.com/datareply/airflow-lesser-known-tips-tricks-and-best-practises-cf4d4a90f8f
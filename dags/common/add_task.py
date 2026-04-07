from airflow.decorators import task
from datetime import datetime, timedelta

@task
def sum_task(x,y):
    print(f"task argument: X={x}")
    return x+y

@task.virtualenv(
    task_id="virtual_task",
    requirements=["pandas","numpy","scikit-learn"]
)
def task_virtualenv():
    import numpy as np
    import sys
    print(sys.version)

@task.virtualenv(
    task_id="training_model",
    requirements=["pandas","numpy","scikit-learn","joblib"]
)
def task_training_model():
    from sklearn.linear_model import Lasso
    import sys
    import pandas as pd
    import joblib

    PATH_COMMON="../"
    sys.path.append(PATH_COMMON)
    xtrain=pd.read_csv("/opt/airflow/dags/data/inputs/xtrain.csv")
    ytrain=pd.read_csv("/opt/airflow/dags/data/inputs/ytrain.csv")
    features=pd.read_csv("/opt/airflow/dags/data/inputs/selected_features.csv")

    features=features['0'].to_list()
    xtrain=xtrain[features]

    model=Lasso(alpha=0.001, random_state=0)
    model.fit(xtrain,ytrain)

    joblib.dump(model,"/opt/airflow/dags/data/model/LinearRegression-Lasso.joblib")

    print("Modelo Guardado. ")
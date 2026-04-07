from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import joblib

# ---------------------------------------------------------
# 1. Funciones de Python para cada Task
# ---------------------------------------------------------

def procesar_datos():
    """
    Task 1: Consume y procesa los datos de entrada.
    """
    ruta_entrada = '/opt/airflow/data/datos_entrenamiento.csv'
    ruta_salida = '/opt/airflow/data/datos_procesados.csv'
    
    print(f"Leyendo datos crudos desde {ruta_entrada}")
    df = pd.read_csv(ruta_entrada)
    
    # Simular el procesamiento: Quitar la columna 'target' para que el modelo la adivine
    if 'target' in df.columns:
        df_procesado = df.drop('target', axis=1)
        print("Columna 'target' eliminada correctamente.")
    else:
        df_procesado = df
        
    # Guardar los datos limpios/procesados para que el Task 2 los pueda consumir
    df_procesado.to_csv(ruta_salida, index=False)
    print(f"Datos procesados listos en {ruta_salida}")

def ejecutar_prediccion():
    """
    Task 2: Consume datos procesados y ejecuta la predicción.
    """
    ruta_datos_procesados = '/opt/airflow/data/datos_procesados.csv'
    ruta_modelo = '/opt/airflow/data/modelo_entrenado.pkl'
    ruta_predicciones = '/opt/airflow/data/predicciones_finales.csv'
    
    print("Cargando modelo y datos procesados...")
    modelo = joblib.load(ruta_modelo)
    df_procesado = pd.read_csv(ruta_datos_procesados)
    
    print("Ejecutando predicciones...")
    predicciones = modelo.predict(df_procesado)
    
    # Agregar las predicciones al DataFrame y guardarlo como el entregable final
    df_procesado['prediccion_rf'] = predicciones
    df_procesado.to_csv(ruta_predicciones, index=False)
    print(f"¡Éxito! Predicciones guardadas en {ruta_predicciones}")

# ---------------------------------------------------------
# 2. Definición del DAG
# ---------------------------------------------------------
default_args = {
    'owner': 'alan',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    '2_dag_prediccion_ml',
    default_args=default_args,
    description='DAG para predecir con el modelo',
    schedule_interval=None, # Ejecución manual
    start_date=days_ago(1),
    catchup=False,
    tags=['mlops', 'prediccion'],
) as dag:

    # Definir la Tarea 1
    task_procesar = PythonOperator(
        task_id='procesar_datos_entrada',
        python_callable=procesar_datos,
    )

    # Definir la Tarea 2
    task_predecir = PythonOperator(
        task_id='ejecutar_prediccion_rf',
        python_callable=ejecutar_prediccion,
    )

    # ---------------------------------------------------------
    # 3. Definición de Dependencias (El flujo)
    # ---------------------------------------------------------
    # Ejecutar procesamiento y DESPUÉS la predicción
    task_procesar >> task_predecir
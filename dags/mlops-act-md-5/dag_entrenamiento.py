from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import joblib

# ---------------------------------------------------------
# 1. Función de Entrenamiento (La lógica de Machine Learning)
# ---------------------------------------------------------
def entrenar_modelo():
    # ¡OJO AQUÍ! Usamos la ruta interna del contenedor de Docker, no la de WSL.
    ruta_datos = '/opt/airflow/data/datos_entrenamiento.csv'
    ruta_modelo = '/opt/airflow/data/modelo_entrenado.pkl'

    print(f"Leyendo datos desde {ruta_datos}")
    df = pd.read_csv(ruta_datos)

    X = df.drop('target', axis=1)
    y = df['target']

    print("Entrenando RandomForestClassifier...")
    modelo = RandomForestClassifier(random_state=42)
    modelo.fit(X, y)

    # Guardamos el modelo entrenado en la misma carpeta compartida
    # para que el DAG de predicción pueda usarlo después.
    joblib.dump(modelo, ruta_modelo)
    print(f"Modelo guardado exitosamente en {ruta_modelo}")

# ---------------------------------------------------------
# 2. Definición del DAG
# ---------------------------------------------------------
default_args = {
    'owner': 'alan',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    '1_dag_entrenamiento_ml',
    default_args=default_args,
    description='DAG para entrenar modelo, actividad 5 MLOps',
    schedule_interval=None, # Lo ejecutaremos manualmente por ahora
    start_date=days_ago(1),
    catchup=False,
    tags=['mlops', 'entrenamiento'],
) as dag:

    # Task 1: Ejecutar la función de Python
    task_entrenar = PythonOperator(
        task_id='entrenar_modelo_rf',
        python_callable=entrenar_modelo,
    )

    # Task 2: Notificación en caso de ÉXITO
    # Utilizamos Jinja Templating ({{ ds }}) para inyectar la fecha de ejecución dinámicamente.
    notificacion_exito = EmailOperator(
        task_id='enviar_correo_exito',
        to='alanperfect2003@gmail.com',
        subject='Airflow DAG: Entrenamiento Exitoso ✅',
        html_content="""
        <h3 style="color:green;">El entrenamiento del modelo ha finalizado correctamente.</h3>
        <ul>
            <li><b>Fecha de ejecución:</b> {{ ds }}</li>
            <li><b>Estado de la tarea:</b> EXITOSO</li>
        </ul>
        <p>El modelo .pkl está listo para hacer predicciones.</p>
        """,
        trigger_rule='all_success' # Solo se ejecuta si task_entrenar termina bien
    )

    # Task 3: Notificación en caso de FALLO
    notificacion_fallo = EmailOperator(
        task_id='enviar_correo_fallo',
        to='alanperfect2003@gmail.com',
        subject='Airflow DAG: ALERTA - Fallo en Entrenamiento ❌',
        html_content="""
        <h3 style="color:red;">El entrenamiento del modelo ha fallado.</h3>
        <ul>
            <li><b>Fecha de ejecución:</b> {{ ds }}</li>
            <li><b>Estado de la tarea:</b> FALLIDO</li>
        </ul>
        <p>Revisa los logs en la interfaz web de Airflow para más detalles.</p>
        """,
        trigger_rule='one_failed' # Solo se ejecuta si task_entrenar falla
    )

    # ---------------------------------------------------------
    # 3. Definición de Dependencias (El flujo)
    # ---------------------------------------------------------
    # Si task_entrenar es exitoso, pasa a notificacion_exito.
    # Si task_entrenar falla, salta a notificacion_fallo.
    task_entrenar >> [notificacion_exito, notificacion_fallo]
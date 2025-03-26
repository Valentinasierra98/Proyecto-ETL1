from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

# Función de extracción de datos (Extraer)
def extract():
    # Configuración de la conexión con SQL Server usando SQLAlchemy
    server = '127.0.0.1'
    database = 'Actividad_ETL'
    username = 'sa'
    password = '12345678'

    # Crear la URL de conexión
    conn_str = f"mssql+pymssql://{username}:{password}@{server}/{database}"

    # Crear el motor de conexión
    engine = create_engine(conn_str)

    # Ejecutar la consulta SQL y cargar los datos en un DataFrame
    query = "SELECT * FROM dbo.tabla_etl_nueva1"
    df = pd.read_sql(query, engine)

    # Renombrar columnas: eliminar espacios y caracteres especiales
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()

    return df

# Función de transformación de datos (Transformar)
def transform(df):
    # Porcentaje de valores nulos por columna
    print(df.isnull().sum().to_frame(name="Valores Nulos").assign(Porcentaje=lambda x: (x["Valores Nulos"] / len(df)) * 100))

    numerical_columns = ["puntaje_global_icfes", "a_global", "a_lectura_critica", 
                         "a_matematicas", "a_sociales_y_ciudadanas", 
                         "a_ciencias_naturales", "a_ingles", "edad"]

    # Identificadores (se mantienen como texto)
    identifier_columns = ["codigo_estudiante", "registro_snp", "snp"]

    # Aplicar conversiones
    df[numerical_columns] = df[numerical_columns].astype("float64")
    df[identifier_columns] = df[identifier_columns].astype("object")
    df[df.select_dtypes(include=['object']).columns] = df.select_dtypes(include=['object']).astype("category")

    # Reemplazar valores 0 en 'edad' con la mediana de la población
    mediana_edad = df["edad"].median()
    df.loc[df["edad"] == 0, "edad"] = mediana_edad

    # Convertir las columnas a string para evitar problemas con categorías
    df["estado_civil"] = df["estado_civil"].astype(str).replace(
        {"No registra": "No Registra", "Sin Registro": "No Registra", "Unión libre": "Unión Libre"}
    ).astype("category")

    df["colegio_sector"] = df["colegio_sector"].astype(str).replace(
        {"NO REGISTRA": "No Registra", "SIN CLASIFICACION": "Sin Clasificación"}
    ).astype("category")

    df["colegio_clasificacion"] = df["colegio_clasificacion"].astype(str).replace(
        {"NO REGISTRA": "No Registra", 
         "SIN CLASIFICACION": "Sin Clasificación",
         "SIN CLASIFICACIÓN": "Sin Clasificación"}  # Normaliza acentos
    ).astype("category")

    # Convertir las columnas de texto (object) a mayúsculas
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.upper()

    # Convertir valores de las columnas categóricas a mayúsculas sin cambiar las categorías
    columnas_categoricas = df.select_dtypes(include=['category']).columns
    for col in columnas_categoricas:
        # Convertir los valores a mayúsculas sin cambiar las categorías
        df[col] = df[col].cat.codes.map(lambda x: str(df[col].cat.categories[x]).upper() if isinstance(df[col].cat.categories[x], str) else df[col].cat.categories[x])

    return df

# Función de carga de datos (Cargar)
def load(df):
    try:
        # Configuración de la conexión con SQL Server usando SQLAlchemy
        server = '127.0.0.1'
        database = 'Actividad_ETL'
        username = 'sa'
        password = '12345678'

        # Crear la URL de conexión
        conn_str = f"mssql+pymssql://{username}:{password}@{server}/{database}"

        # Crear el motor de conexión
        engine = create_engine(conn_str)

        # Cargar datos transformados a SQL
        df.to_sql("TRANSFORMACION", engine, if_exists="replace", index=False)

        print("Datos cargados correctamente.")
    except Exception as e:
        print(f"Error al cargar los datos: {e}")

# Definir el DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2021, 3, 25),  # Ajustar a la fecha que desees
    'catchup': False,  # Para evitar que ejecute tareas pasadas
}

dag = DAG(
    'ETL_Actividad_FINAL',  # Nombre del DAG
    default_args=default_args,
    description='Un DAG para el proceso ETL',
    schedule_interval=None,  # Ajustar al horario que desees
)

# Definir las tareas
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    op_args=[extract_task.output],  # Pasar el resultado de extract
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    op_args=[transform_task.output],  # Pasar el resultado de transform
    dag=dag,
)

# Definir la secuencia de tareas
extract_task >> transform_task >> load_task

# Proyecto ETL - Actividades_Fase1

Este proyecto se desarrolla utilizando Python para la extracción, transformación y carga (ETL) de datos en SQL Server. La información procesada proviene de dos archivos CSV que contienen datos sobre los estudiantes admitidos en el primer semestre de una universidad X, así como sus puntajes en la prueba Saber 11, obtenidos de las fuentes oficiales de datos del ICFES. El objetivo es construir una base de datos relacional en SQL Server que permita analizar y gestionar esta información de manera eficiente.

## Requisitos

- Python 3.x
- pandas
- openpyxl
- pymssql
- unidecode
- numpy
- re

## Instalación

Para instalar los paquetes necesarios, ejecuta:

```bash
pip install pandas openpyxl pymssql unidecode numpy
```

## Descripción del Proyecto

Este proyecto realiza los siguientes pasos:

1. **Extracción de Datos:** Se leen dos archivos Excel con datos de estudiantes.
2. **Transformación Inicial:**
   - Se normalizan los nombres de las columnas eliminando tildes y caracteres especiales.
   - Se unifican las claves para poder fusionar los datos.
   - Se realiza una unión entre ambos DataFrames para completar la información.
3. **Carga de Datos:**
   - Se conecta con SQL Server.
   - Se crea la base de datos `Actividad_ETL` si no existe.
   - Se crea la tabla `tabla_etl_nueva1` con los datos transformados.
   - Se insertan los registros en la tabla.
   - Se verifican los datos almacenados.

## Uso

1. Clona el repositorio:

```bash
git clone https://github.com/tuusuario/proyecto_etl.git
cd proyecto_etl
```

2. Instala las dependencias:

```bash
pip install -r requirements.txt
```

3. Ejecuta el script principal:

```bash
python etl_script.py
```

## Contribuciones

¡Las contribuciones son bienvenidas! Abre un issue o envía un pull request.

## Licencia

Este proyecto está bajo la licencia MIT. Consulta el archivo [LICENSE](LICENSE) para más detalles.


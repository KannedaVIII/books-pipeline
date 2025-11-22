Book Data Integration Pipeline: Requisitos y Guía de Uso
Este repositorio contiene un pipeline de Ingeniería de Datos en Python diseñado para extraer, enriquecer, estandarizar y deduplicar metadatos de libros procedentes de Goodreads (mediante Web Scraping) y Google Books (mediante su API), generando una fuente de datos canónica y limpia.
1. Requisitos de Ejecución 
Para ejecutar el pipeline completo, se requieren las siguientes dependencias a nivel de sistema y Python:

1.1. Dependencias del Sistema
Dependencia	Propósito	Requisito
Python	Entorno de ejecución	Versión 3.8 o superior.
PIP	Gestor de paquetes	Incluido con Python.

Exportar a Hojas de cálculo

1.2. Dependencias de Python
Todas las dependencias de Python deben instalarse desde el archivo requirements.txt (asumiendo que existe):

Bash

pip install -r requirements.txt
Las librerías clave utilizadas son:

requests: Para realizar peticiones HTTP (Web Scraping y API).

beautifulsoup4: Para parsear HTML (Goodreads Scraping).

pandas: Para la manipulación, limpieza y estandarización de datos (Núcleo del Ejercicio 3).

openpyxl / pyarrow: Necesario para leer/escribir formatos Parquet (para dim_book.parquet).

python-dotenv: Para cargar la clave de API desde el archivo .env.

2. Configuración Esencial 
El proyecto requiere una clave de API para la fase de enriquecimiento de Google Books.

2.1. Obtener Clave de Google Books API
Obtén una clave de API de la Consola de Desarrolladores de Google.

Asegúrate de que la API de Google Books esté habilitada en tu proyecto.

2.2. Archivo .env
Crea un archivo llamado .env en la raíz del proyecto para almacenar tu clave:

Ini, TOML

# .env file
GOOGLE_BOOKS_API_KEY="TU_CLAVE_DE_API_AQUÍ"
Advertencia: Si el archivo .env o la clave no están configurados, el Ejercicio 2 utilizará datos simulados (mocking), pero el resto del pipeline se ejecutará con datos de baja calidad.

3. Guía de Uso (Flujo de Trabajo) 
El pipeline está diseñado para ejecutarse en orden: Ejercicio 1 (Extracción), Ejercicio 2 (Enriquecimiento) y Ejercicio 3 (Integración).

Paso 1: Extracción de Goodreads (Ejercicio 1)
Ejecuta el script de web scraping para obtener los IDs y datos básicos de Goodreads:

Bash

# Ejecutar el script que contiene la lógica del Ejercicio 1
python src/ejercicio_1_goodreads_scraper.py 
# (Asumiendo que el archivo de scraping se llama así, ajusta el nombre si es diferente)
Resultado Esperado: Archivo landing/goodreads_books.json.

Paso 2: Enriquecimiento con Google Books (Ejercicio 2)
Ejecuta el script que consulta la API de Google Books para complementar los datos:

Bash

# Ejecutar el script que contiene la lógica del Ejercicio 2
python src/ejercicio_2_googlebooks_enricher.py 
# (Asumiendo que el archivo de enriquecimiento se llama así, ajusta el nombre si es diferente)
Resultado Esperado: Archivo landing/googlebooks_books.csv.

Paso 3: Integración y Estandarización (Ejercicio 3)
Ejecuta el script principal del pipeline para unificar, limpiar, deduplicar y generar los artefactos finales:

Bash

# Ejecutar el script del pipeline principal
python src/integrate_pipeline.py 
# (Asumiendo que el archivo principal se llama así, ajusta el nombre si es diferente)
Resultado Esperado:

standard/dim_book.parquet (Tabla de datos limpia y canónica).

standard/book_source_detail.parquet (Detalle de todos los registros de origen).

standard/docs/quality_metrics.json (Reporte de calidad).

standard/docs/schema.md (Documentación del esquema).

4. Estructura de Directorios
El pipeline asumirá la siguiente estructura para entradas y salidas:

project-root/
├── .env                  <-- Clave de API aquí
├── src/
│   ├── ejercicio_1_*.py  <-- Extracción GR
│   ├── ejercicio_2_*.py  <-- Enriquecimiento GB
│   └── integrate_pipeline.py <-- Integración (Ej. 3)
│
├── landing/              <-- Datos crudos o enriquecidos
│   ├── goodreads_books.json
│   └── googlebooks_books.csv
│
└── standard/             <-- Artefactos finales y limpios
    ├── dim_book.parquet
    ├── book_source_detail.parquet
    └── docs/             <-- Documentación de la capa Standard
        ├── quality_metrics.json
        └── schema.md

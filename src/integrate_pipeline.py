import pandas as pd
import json
import os
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

# Importar funciones de utilidades (ya corregido para usar utils_goods)
from utils_isbn import clean_isbn, is_isbn13
from utils_goods import normalize_date, normalize_language, check_currency, calculate_quality_metrics 

# ---------------------------------------------------------------------
# Configuración y Rutas
# ---------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LANDING_DIR = PROJECT_ROOT / "landing"
STANDARD_DIR = PROJECT_ROOT / "standard"
DOCS_DIR = STANDARD_DIR / "docs"

# Archivos de entrada
GOODREADS_JSON = LANDING_DIR / "goodreads_books.json"
GOOGLEBOOKS_CSV = LANDING_DIR / "googlebooks_books.csv"

# Archivos de salida
DIM_BOOK_PARQUET = STANDARD_DIR / "dim_book.parquet"
BOOK_SOURCE_DETAIL_PARQUET = STANDARD_DIR / "book_source_detail.parquet"
QUALITY_METRICS_JSON = DOCS_DIR / "quality_metrics.json"
SCHEMA_MD = DOCS_DIR / "schema.md"

# ---------------------------------------------------------------------
# Modelo Canónico (Campos de dim_book.parquet)
# ---------------------------------------------------------------------
CANONICAL_COLUMNS = [
    'book_id', 'title', 'title_normalized', 'author_principal', 'authors', 
    'editorial', 'anio_publicacion', 'fecha_publicacion', 'idioma', 
    'isbn10', 'isbn13', 'paginas', 'formato', 'categoria', 
    'precio', 'moneda', 'fuente_ganadora', 'ts_ultima_actualizacion'
]

# ---------------------------------------------------------------------
# Lógica Principal del Pipeline
# ---------------------------------------------------------------------

def load_data() -> Dict[str, pd.DataFrame]:
    """Step 1: Land (Read) files without modification."""
    print("[INFO] 1. Aterrizando (leyendo) los archivos de landing/.")
    data = {}
    
    # Load GoodReads (JSON)
    try:
        # Read the JSON file
        with open(GOODREADS_JSON, 'r', encoding='utf-8') as f:
            gr_data = json.load(f)
        # Normalize the 'books' list
        data['goodreads'] = pd.json_normalize(gr_data['books'])
        print(f"   -> GoodReads cargado: {len(data['goodreads'])} filas.")
    except Exception as e:
        print(f"[FATAL] Error al cargar goodreads_books.json: {e}")
        data['goodreads'] = pd.DataFrame()

    # Load GoogleBooks (CSV)
    try:
        # Read the CSV file
        data['googlebooks'] = pd.read_csv(GOOGLEBOOKS_CSV, encoding='utf-8')
        print(f"   -> GoogleBooks cargado: {len(data['googlebooks'])} filas.")
    except Exception as e:
        print(f"[FATAL] Error al cargar googlebooks_books.csv: {e}")
        data['googlebooks'] = pd.DataFrame()
        
    return data

def map_and_normalize(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Applies mapping, cleaning, and normalization to a single source."""
    
    df_std = df.copy()
    
    # ----------------------------------------
    # Naming and Source Column Definitions
    # ----------------------------------------
    if source == 'goodreads':
        # Rename original fields to raw/standard
        df_std.rename(columns={'title': 'title_raw', 'author': 'authors_raw', 'book_url': 'url_source'}, inplace=True)
        # Normalize title for merging/hashing
        df_std['title_normalized'] = df_std['title_raw'].astype(str).str.strip().str.lower()
        df_std['authors_raw'] = df_std['authors_raw'].astype(str).fillna('').str.strip()
        
        # Add placeholder columns if missing from simple scrape
        df_std['language_raw'] = None
        df_std['publishedDate_raw'] = None 
        df_std['price_amount'] = None
        df_std['price_currency'] = None
        df_std['publisher'] = None
        df_std['categories_raw'] = None
        df_std['pageCount'] = None

    elif source == 'googlebooks':
        # Rename enriched fields from GoogleBooks
        df_std.rename(columns={
            'title': 'title_raw_gb', # Enriched title (used for description)
            'authors': 'authors_raw', 
            'pub_date': 'publishedDate_raw', 
            'language': 'language_raw', 
            'categories': 'categories_raw',
            'goodreads_title': 'title_raw', # Original title from GoodReads (used for normalization)
            'goodreads_url': 'url_source',
            'publisher': 'publisher',
            'price_amount': 'price_amount',
            'price_currency': 'price_currency'
        }, inplace=True)
        
        # Use the original GoodReads title for normalization/hashing
        df_std['title_normalized'] = df_std['title_raw'].astype(str).str.strip().str.lower()
        df_std['authors_raw'] = df_std['authors_raw'].astype(str).fillna('').str.strip()
        df_std['pageCount'] = None # Placeholder if pages were not scraped in Ej. 2

    df_std['source_name'] = source
    df_std['timestamp_ingesta'] = datetime.now().isoformat()
    
    # ----------------------------------------
    # Normalization of ISBNs (Applies to both sources)
    # ----------------------------------------
    df_std['isbn13_clean'] = df_std.apply(
        lambda row: clean_isbn(row.get('isbn13')) or clean_isbn(row.get('goodreads_isbn13')), axis=1
    )
    df_std['isbn10_clean'] = df_std.apply(
        lambda row: clean_isbn(row.get('isbn10')) or clean_isbn(row.get('goodreads_isbn10')), axis=1
    )
    
    # ----------------------------------------
    # Normalization of Dates (YYYY-MM-DD or YYYY)
    # ----------------------------------------
    df_std['fecha_publicacion'] = df_std['publishedDate_raw'].apply(normalize_date)
    
    # ----------------------------------------
    # Normalization of Language (BCP-47)
    # ----------------------------------------
    df_std['idioma_bcp47'] = df_std['language_raw'].apply(normalize_language)
    
    # ----------------------------------------
    # Normalization of Currency (ISO-4217) and Price
    # ----------------------------------------
    df_std['moneda_iso'] = df_std['price_currency'].apply(
        lambda x: x if check_currency(x) else pd.NA
    )
    df_std['precio'] = df_std['price_amount']

    # ----------------------------------------
    # Definition of Canonical ID (isbn13 preferred)
    # ----------------------------------------
    
    # 1. Primary Canonical ID: Clean ISBN-13
    df_std['book_id'] = df_std['isbn13_clean'].apply(lambda x: x if is_isbn13(x) else pd.NA)
    
    # 2. Secondary Canonical ID: Generate hash if ISBN-13 is missing
    # We create a fallback key based on cleaned title and first author
    df_std['fallback_key'] = (
        df_std['title_normalized'].fillna('__MISSING_TITLE__') + 
        '|' + 
        df_std['authors_raw'].str.split(',').str[0].fillna('__MISSING_AUTHOR__')
    )
    df_std['book_id'] = df_std.apply(
        lambda row: row['book_id'] if pd.notna(row['book_id']) else hash(row['fallback_key']),
        axis=1
    )
    
    # 3. Year of Publication (derived)
    df_std['anio_publicacion'] = df_std['fecha_publicacion'].astype(str).str[:4]
    
    return df_std

def standardize_and_merge(raw_data: Dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Coordinates standardization, quality checks, and merging of the two sources.
    """
    print("[INFO] 2. Estandarizando y Normalizando las fuentes.")
    
    # 1. Separate Mapping and Normalization
    df_gr_std = map_and_normalize(raw_data['goodreads'], 'goodreads')
    df_gb_std = map_and_normalize(raw_data['googlebooks'], 'googlebooks')
    
    # 2. Unification (Concatenate both DataFrames)
    df_unified = pd.concat([df_gr_std, df_gb_std], ignore_index=True)

    # 3. Quality Checks
    quality_metrics = {}
    
    # Key columns for completeness checks
    key_cols = ['book_id', 'title_normalized', 'isbn13_clean', 'authors_raw']
    quality_metrics['goodreads'] = calculate_quality_metrics(df_gr_std, 'goodreads', key_cols)
    quality_metrics['googlebooks'] = calculate_quality_metrics(df_gb_std, 'googlebooks', key_cols)

    return df_unified, quality_metrics

def deduplicate_and_create_dims(df_unified: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Steps 7, 8: Deduplication with survival rules and creation of Parquet files.
    """
    print("[INFO] 3. Deduplicando y aplicando reglas de supervivencia.")
    
    # ----------------------------------------
    # Survival Rules (Priority)
    # ----------------------------------------
    
    # Rule: 1. Source (GoogleBooks > GoodReads), 2. Title (longest length), 3. Date (most recent)
    df_unified['source_rank'] = df_unified['source_name'].apply(lambda x: 1 if x == 'googlebooks' else 2)
    df_unified['title_length'] = df_unified['title_raw'].astype(str).str.len().fillna(0)
    
    # Sort by duplication key (book_id) and then by survival criteria
    # We use a placeholder for date sorting to handle missing values gracefully
    df_unified['fecha_sort'] = df_unified['fecha_publicacion'].fillna('0000-00-00')
    
    df_sorted = df_unified.sort_values(
        by=['book_id', 'source_rank', 'title_length', 'fecha_sort'], 
        ascending=[True, True, False, False] # book_id asc, source_rank asc (GB=1), length desc, date desc
    )
    
    # Identify the winner record (first in each group)
    df_winners = df_sorted.drop_duplicates(subset=['book_id'], keep='first')
    df_winners['fuente_ganadora'] = df_winners['source_name']
    df_winners['ts_ultima_actualizacion'] = datetime.now().isoformat()
    
    # ----------------------------------------
    # Emission of Parquet Artifacts
    # ----------------------------------------
    
    # a) book_source_detail.parquet
    print("[INFO] Generando book_source_detail.parquet...")
    df_source_detail = df_unified.copy()
    # Add a flag indicating if the record is the winner for that key
    winner_ids = df_winners['book_id'].tolist()
    df_source_detail['is_winner'] = df_source_detail['book_id'].isin(winner_ids)
    
    df_source_detail.to_parquet(BOOK_SOURCE_DETAIL_PARQUET, index=False)

    
    # b) dim_book.parquet (Canonical Table)
    print("[INFO] Generando dim_book.parquet (Tabla Canónica)...")
    
    # Final mapping to the canonical columns
    df_dim_book = df_winners.rename(columns={
        'title_raw': 'title',
        'authors_raw': 'authors',
        'idioma_bcp47': 'idioma',
        'isbn13_clean': 'isbn13',
        'isbn10_clean': 'isbn10',
        'precio': 'precio',
        'moneda_iso': 'moneda',
        'publisher': 'editorial', 
        'categories_raw': 'categoria',
        'pageCount': 'paginas',
    })
    
    # Generate missing or simplified fields
    df_dim_book['author_principal'] = df_dim_book['authors'].astype(str).str.split(',').str[0].str.strip()
    df_dim_book['formato'] = None 
    
    # Ensure all canonical columns are present
    for col in CANONICAL_COLUMNS:
        if col not in df_dim_book.columns:
            df_dim_book[col] = pd.NA
    
    df_dim_book = df_dim_book[CANONICAL_COLUMNS].reset_index(drop=True)
    
    df_dim_book.to_parquet(DIM_BOOK_PARQUET, index=False)
    
    print(f"   -> dim_book.parquet generado con {len(df_dim_book)} registros canónicos.")
    
    return {'dim_book': df_dim_book, 'book_source_detail': df_source_detail}

def generate_docs(df_dim_book: pd.DataFrame, quality_metrics: Dict[str, Any]):
    """
    Step 4: Generate quality_metrics.json and schema.md.
    """
    print("[INFO] 4. Generando archivos de documentación (standard/docs/).")
    
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    
    # a) quality_metrics.json
    metrics_list = [quality_metrics[k] for k in quality_metrics]
    with open(QUALITY_METRICS_JSON, 'w', encoding='utf-8') as f:
        json.dump(metrics_list, f, ensure_ascii=False, indent=4)
    print(f"   -> {QUALITY_METRICS_JSON.name} generado.")

    # b) schema.md
    schema_content = generate_schema_markdown(df_dim_book)
    with open(SCHEMA_MD, 'w', encoding='utf-8') as f:
        f.write(schema_content)
    print(f"   -> {SCHEMA_MD.name} generado.")

def generate_schema_markdown(df: pd.DataFrame) -> str:
    """Generates the documentation for the canonical model dim_book.parquet in Markdown."""
    markdown = f"""# Esquema del Modelo Canónico: `dim_book.parquet`

**Fecha de Generación:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Clave de Deduplicación:** `book_id` (ISBN-13 limpio o Hash si falta ISBN)
**Regla de Supervivencia:** Google Books > Título más largo > Fecha Publicación más reciente

## Estructura de Columnas ({len(CANONICAL_COLUMNS)} Campos)

| Nombre de Columna | Tipo de Dato (Pandas/Parquet) | Descripción | Nullable | Ejemplo |
| :--- | :--- | :--- | :--- | :--- |
"""
    
    # Generate metadata table
    for col in CANONICAL_COLUMNS:
        col_type = str(df[col].dtype)
        
        # Get first non-null value as example
        example = str(df[col].dropna().iloc[0]) if not df[col].dropna().empty else "N/A"
        
        # Estimate Nullable
        nullable = "Sí" if df[col].isnull().any() else "No"
        
        description = {
            'book_id': 'ID canónico (ISBN13 o Hash del título). Clave primaria.',
            'title': 'Título del libro (del registro ganador).',
            'title_normalized': 'Título en minúsculas y sin espacios extra (para deduplicación).',
            'author_principal': 'Autor principal (primer autor de la lista).',
            'authors': 'Cadena de todos los autores (del registro ganador).',
            'editorial': 'Nombre de la editorial (principalmente de Google Books).',
            'anio_publicacion': 'Año de publicación (YYYY).',
            'fecha_publicacion': 'Fecha de publicación normalizada (YYYY-MM-DD, YYYY-MM o YYYY).',
            'idioma': 'Código de idioma normalizado (BCP-47, ej. "en", "es").',
            'isbn10': 'ISBN-10 limpio.',
            'isbn13': 'ISBN-13 limpio. Clave preferente.',
            'paginas': 'Número de páginas.',
            'formato': 'Formato del libro (no implementado).',
            'categoria': 'Categorías/Géneros (cadena separada).',
            'precio': 'Monto del precio (solo si existe).',
            'moneda': 'Código de moneda normalizado (ISO-4217, ej. "USD").',
            'fuente_ganadora': 'Fuente del registro que fue elegido como canónico.',
            'ts_ultima_actualizacion': 'Timestamp de la última actualización del registro.',
        }.get(col, 'Descripción pendiente.')
        
        markdown += f"| {col} | {col_type} | {description} | {nullable} | {example} |\n"

    markdown += "\n## Metodología de Normalización\n"
    markdown += "- **Fechas:** Normalizadas a ISO-8601 (YYYY-MM-DD, YYYY-MM o YYYY).\n"
    markdown += "- **Idioma:** Normalizado a BCP-47 (ej. 'en', 'es').\n"
    markdown += "- **Moneda:** Normalizado a ISO-4217 (ej. 'USD', 'EUR').\n"
    markdown += "- **Nombres:** Título se normaliza a `title_normalized` para la deduplicación.\n"
    
    return markdown

# ---------------------------------------------------------------------
# Punto de entrada principal
# ---------------------------------------------------------------------

def main():
    print("=" * 60)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] EJERCICIO 3: INICIANDO PIPELINE DE INTEGRACIÓN")
    print("=" * 60)

    # 1. Load Data
    raw_data = load_data()
    
    if raw_data['goodreads'].empty or raw_data['googlebooks'].empty:
        print("[FATAL] Faltan archivos de entrada en landing/. Asegúrate de ejecutar Ej. 1 y Ej. 2.")
        return

    # 2. Standardization and Normalization
    df_unified, quality_metrics = standardize_and_merge(raw_data)
    
    # 3. Deduplication and Artifact Creation
    results = deduplicate_and_create_dims(df_unified)
    
    # 4. Documentation and Metrics Generation
    generate_docs(results['dim_book'], quality_metrics)
    
    print("\n" + "=" * 60)
    print("✅ PIPELINE COMPLETO. Todos los artefactos generados en standard/.")
    print("=" * 60)

if __name__ == "__main__":
    # Ensure standard and docs directories exist
    STANDARD_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    
    main()
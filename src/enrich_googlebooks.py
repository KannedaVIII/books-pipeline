import requests
import json
import csv
import os
import time
from dotenv import load_dotenv
from typing import List, Dict, Optional, Any
import random

# Cargar variables de entorno (necesario si la clave existe, ignorado si no)
load_dotenv()

# --- Configuración y Rutas ---
GOOGLE_BOOKS_API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY", "NO_API_KEY_PROVIDED") 
BASE_API_URL = "https://www.googleapis.com/books/v1/volumes"

# Rutas de archivos de entrada/salida
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
INPUT_FILE = os.path.join(PROJECT_ROOT, 'landing', 'goodreads_books.json')
OUTPUT_FILE = os.path.join(PROJECT_ROOT, 'landing', 'googlebooks_books.csv')
DELAY_SECONDS = 0.5 

# Campos que deben ir en el CSV de salida (según el enunciado)
CSV_FIELDNAMES = [
    'gb_id', 'title', 'subtitle', 'authors', 'publisher', 'pub_date', 
    'language', 'categories', 'isbn13', 'isbn10', 'price_amount', 
    'price_currency', 'goodreads_title', 'goodreads_author', 'goodreads_url', 
    'goodreads_isbn10', 'goodreads_isbn13' 
]

# ---------------------------------------------------------------------
# Funciones de MOCKING (para desarrollo sin clave o datos)
# ---------------------------------------------------------------------

def get_mock_result(gr_book: Dict) -> Dict:
    """
    Genera un resultado simulado de Google Books para pruebas.
    Se garantiza que siempre devuelve un resultado para completar el flujo.
    """
    mock_id = f"GBID-{random.randint(1000, 9999)}"
    
    # Simula la estructura básica de la respuesta de Google Books API
    return {
        "id": mock_id,
        "volumeInfo": {
            "title": f"Enhanced: {gr_book.get('title', 'Unknown Title')}",
            "subtitle": "A Deep Dive into Data",
            "authors": [gr_book.get('author', 'J. Doe')],
            "publisher": "Tech Press Inc.",
            "publishedDate": f"{random.randint(2018, 2023)}-01-01",
            "language": random.choice(["en", "es", "fr"]),
            "categories": [random.choice(["Data Science", "Programming", "Machine Learning"])],
            "industryIdentifiers": [
                {"type": "ISBN_13", "identifier": "9781234567890"},
                {"type": "ISBN_10", "identifier": "1234567890"}
            ]
        },
        "saleInfo": {
            "listPrice": {
                "amount": random.uniform(20.00, 50.00),
                "currencyCode": random.choice(["USD", "EUR"])
            }
        }
    }


# ---------------------------------------------------------------------
# Funciones de Lógica de Negocio 
# ---------------------------------------------------------------------

def load_goodreads_data(filepath: str) -> List[Dict]:
    """Carga los datos de libros desde el JSON de Goodreads."""
    if not os.path.exists(filepath):
        print(f"[ERROR] Archivo de entrada no encontrado: {filepath}. NO se puede ejecutar el Ejercicio 2.")
        return []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
        return data.get('books', [])

def search_google_books(query: str) -> Optional[Dict]:
    """Busca en la API de Google Books con la query dada."""
    # Si tenemos la clave, usamos la API real
    if GOOGLE_BOOKS_API_KEY != "NO_API_KEY_PROVIDED":
        params = {
            'q': query,
            'maxResults': 1,
            'key': GOOGLE_BOOKS_API_KEY
        }
        try:
            response = requests.get(BASE_API_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get('items'):
                return data['items'][0]
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Fallo en la llamada a la API con query '{query}': {e}")
        return None
    
    return None

def normalize_and_map_gb_data(gb_item: Dict, gr_book: Dict) -> Dict:
    """Extrae y mapea los campos de Google Books al formato de salida."""
    volume_info = gb_item.get('volumeInfo', {})
    sale_info = gb_item.get('saleInfo', {})
    
    result = {
        'gb_id': gb_item.get('id'),
        'title': volume_info.get('title'),
        'subtitle': volume_info.get('subtitle'),
        'authors': ', '.join(volume_info.get('authors', [])), 
        'publisher': volume_info.get('publisher'),
        'pub_date': volume_info.get('publishedDate'),
        'language': volume_info.get('language'),
        'categories': '; '.join(volume_info.get('categories', [])),
        'price_amount': sale_info.get('listPrice', {}).get('amount'),
        'price_currency': sale_info.get('listPrice', {}).get('currencyCode'),
        'isbn13': None,
        'isbn10': None
    }
    
    identifiers = volume_info.get('industryIdentifiers', [])
    for ident in identifiers:
        if ident.get('type') == 'ISBN_13':
            result['isbn13'] = ident.get('identifier')
        elif ident.get('type') == 'ISBN_10':
            result['isbn10'] = ident.get('identifier')

    # Añadir campos originales de Goodreads para trazabilidad
    result['goodreads_title'] = gr_book.get('title')
    result['goodreads_author'] = gr_book.get('author')
    result['goodreads_url'] = gr_book.get('book_url')
    result['goodreads_isbn10'] = gr_book.get('isbn10')
    result['goodreads_isbn13'] = gr_book.get('isbn13')
    
    return result

def enrich_data(gr_books: List[Dict]) -> List[Dict]:
    """Realiza el bucle de enriquecimiento para cada libro de Goodreads."""
    enriched_data = []
    use_mocking = GOOGLE_BOOKS_API_KEY == "NO_API_KEY_PROVIDED"
    
    if use_mocking:
        print("\n[WARNING] Usando DATOS SIMULADOS (MOCKING) para el Ejercicio 2.")
    
    print(f"\n[INFO] Iniciando enriquecimiento de {len(gr_books)} libros.")
    
    for idx, gr_book in enumerate(gr_books, 1):
        
        gb_result = None
        
        # Estrategia de búsqueda
        if use_mocking:
            gb_result = get_mock_result(gr_book)
        else:
            # Estrategia TÍTULO/AUTOR (Prioridad en API real)
            title = gr_book.get('title')
            author = gr_book.get('author')
            isbn = gr_book.get('isbn13') or gr_book.get('isbn10')
            
            # 1. Título y Autor
            if title and author:
                query = f'intitle:"{title}"+inauthor:"{author}"'
                gb_result = search_google_books(query)
            
            # 2. Sólo Título
            if not gb_result and title:
                query = f'intitle:"{title}"'
                gb_result = search_google_books(query)
                
            # 3. ISBN
            if not gb_result and isbn:
                query = f'isbn:{isbn}'
                gb_result = search_google_books(query)
        
        if gb_result:
            mapped_data = normalize_and_map_gb_data(gb_result, gr_book)
            enriched_data.append(mapped_data)
        else:
            print(f"[WARN] No se encontró match para: {gr_book.get('title', 'Sin título')} (Saltando)")

        time.sleep(DELAY_SECONDS)

    return enriched_data

# ---------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------

def main():
    if GOOGLE_BOOKS_API_KEY == "NO_API_KEY_PROVIDED":
        print("\n" * 2)
        print("!" * 60)
        print("!!! ADVERTENCIA: CLAVE DE API NO CONFIGURADA !!!")
        print("!!! El script usará datos simulados para generar el CSV.")
        print("!" * 60)
        print("\n" * 2)

    gr_books = load_goodreads_data(INPUT_FILE)
    if not gr_books:
        print("[FATAL] No se pudo cargar ningún libro de Goodreads. Asegúrate de ejecutar el Ejercicio 1 primero.")
        return

    enriched_books = enrich_data(gr_books)

    # 3. Guardar el resultado en CSV
    try:
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        # Indicamos el separador (coma) y la codificación (UTF-8)
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDNAMES, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            
            writer.writeheader()
            writer.writerows(enriched_books)
            
        print(f"\n✅ Proceso de enriquecimiento completado. Datos guardados en: {OUTPUT_FILE}")
        print(f"Total de libros enriquecidos: {len(enriched_books)}")

    except Exception as e:
        print(f"[FATAL] Error al escribir el archivo CSV: {e}")

if __name__ == "__main__":
    main()
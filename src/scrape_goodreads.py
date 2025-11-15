import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
import time 
import re 
from typing import List, Dict, Optional

# --- Configuración de Rutas y Parámetros ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
LANDING_DIR = os.path.join(PROJECT_ROOT, "landing")
OUTPUT_FILE = os.path.join(LANDING_DIR, 'goodreads_books.json')

GOODREADS_SEARCH_URL = "https://www.goodreads.com/search" 
BASE_BOOK_URL = "https://www.goodreads.com/book/show/{}"
SEARCH_QUERY = "data science" 
MIN_BOOKS_TO_SCRAPE = 10 
PAGINATION_DELAY_SECONDS = 1.0 # Pausa entre páginas de búsqueda
BOOK_DELAY_SECONDS = 1.5       # Pausa entre la descarga de fichas individuales

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# ---------------------------------------------------------------------
# Funciones de Extracción
# ---------------------------------------------------------------------

def fetch_html(url: str) -> Optional[str]:
    """Descarga el HTML de una URL."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"[ERROR] No se pudo descargar la página {url}: {e}")
        return None

def extract_book_id_from_href(href: str) -> Optional[str]:
    """Extrae el ID del libro de una URL de Goodreads."""
    if not href:
        return None
    # Busca el patrón /book/show/ID o /book/show/ID.Nombre_Libro
    m = re.search(r"/book/show/(\d+)", href)
    if m:
        return m.group(1)
    return None

def search_book_ids(query: str, max_books: int, max_pages: int = 5) -> List[str]:
    """Busca en Goodreads y devuelve una lista de IDs de libro."""
    book_ids: List[str] = []

    for page in range(1, max_pages + 1):
        if len(book_ids) >= max_books:
            break
            
        params = {
            'q': query,
            'search_type': 'books',
            'page': page
        }
        
        url = f"{GOODREADS_SEARCH_URL}?q={query}&search_type=books&page={page}"
        print(f"[INFO] Buscando IDs en página {page}...")
        
        html = fetch_html(url)
        if html is None:
            print("[WARN] No se pudo obtener esta página de búsqueda.")
            break

        soup = BeautifulSoup(html, "html.parser")
        
        # Selector más robusto para las filas de la tabla de resultados
        rows = soup.select("table.tableList tr") 
        
        if not rows or len(rows) <= 1:
            print("[INFO] No hay más resultados o la estructura HTML ha cambiado.")
            break

        for row in rows[1:]: # Ignoramos el encabezado si existe
            link = row.select_one("a.bookTitle")
            if not link:
                continue
            
            href = link.get("href", "")
            book_id = extract_book_id_from_href(href)
            
            if book_id and book_id not in book_ids:
                book_ids.append(book_id)
                
            if len(book_ids) >= max_books:
                break
                
        time.sleep(PAGINATION_DELAY_SECONDS)

    print(f"[INFO] Total de IDs encontrados: {len(book_ids)}")
    return book_ids

def parse_book_page(html: str, book_id: str) -> Dict:
    """Extrae la información requerida de la página de detalles de un libro."""
    soup = BeautifulSoup(html, "html.parser")
    book_data = {
        "book_id_source": book_id,
        "book_url": BASE_BOOK_URL.format(book_id),
        "title": None,
        "author": None,
        "rating": None,
        "ratings_count": None,
        "isbn10": None,
        "isbn13": None,
    }

    # 1. Título
    title_tag = soup.select_one("h1[data-testid='bookTitle']")
    book_data['title'] = title_tag.get_text(strip=True) if title_tag else None

    # 2. Autor (tomamos solo el primero si hay varios)
    author_tag = soup.select_one("span[data-testid='authorName'] a")
    book_data['author'] = author_tag.get_text(strip=True) if author_tag else None

    # 3. Valoración media (rating) y Conteo
    rating_value_tag = soup.select_one("div[data-testid='rating'] span[data-testid='ratingValue']")
    if rating_value_tag:
        try:
            # Goodreads usa la coma como separador de miles en ratingsCount, 
            # pero el rating es un punto (ej. 4.23)
            rating_text = rating_value_tag.get_text(strip=True)
            book_data['rating'] = float(rating_text)
        except (ValueError, TypeError):
            pass

    ratings_count_tag = soup.select_one("div[data-testid='rating'] span[data-testid='ratingsCount']")
    if ratings_count_tag:
        text = ratings_count_tag.get_text(strip=True).replace(",", "")
        parts = re.search(r'(\d+)', text)
        if parts:
             try:
                book_data['ratings_count'] = int(parts.group(1))
             except ValueError:
                pass


    # 4. ISBNs (Suele estar en la sección de 'Details' o en texto plano)
    text_content = soup.get_text(" ", strip=True)

    # ISBN-13
    match_13 = re.search(r"ISBN13:?[\s(]+(\d{13})", text_content)
    book_data['isbn13'] = match_13.group(1) if match_13 else None

    # ISBN-10
    match_10 = re.search(r"ISBN:?[\s(]+(\d{10})", text_content)
    book_data['isbn10'] = match_10.group(1) if match_10 else None

    return book_data

def scrape_goodreads_books(book_ids: List[str]) -> List[Dict]:
    """Descarga la información completa de cada libro por ID."""
    results: List[Dict] = []

    for idx, book_id in enumerate(book_ids, start=1):
        print(f"[INFO] ({idx}/{len(book_ids)}) Extrayendo ficha ID={book_id}...")

        url = BASE_BOOK_URL.format(book_id)
        html = fetch_html(url)
        
        if html is None:
            print(f"[WARN] Saltando libro {book_id} por error de descarga.")
            continue

        book_data = parse_book_page(html, book_id)
        results.append(book_data)

        time.sleep(BOOK_DELAY_SECONDS) # Pausa entre libros

    return results

# ---------------------------------------------------------------------
# Punto de entrada principal
# ---------------------------------------------------------------------

def main():
    print("-" * 40)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Inicio del Ejercicio 1: Scraping Goodreads")
    print("-" * 40)

    # 1. Buscar IDs de libros
    book_ids = search_book_ids(SEARCH_QUERY, MIN_BOOKS_TO_SCRAPE)
    if not book_ids:
        print("[ERROR] No se ha encontrado ningún ID de libro en la búsqueda. Terminando.")
        return

    # 2. Scrapear cada ficha individualmente
    books_data = scrape_goodreads_books(book_ids)
    
    if not books_data:
        print("[ERROR] No se ha podido obtener ningún libro válido. Terminando.")
        return

    # 3. Preparar y guardar el JSON de salida con metadatos
    if not os.path.exists(LANDING_DIR):
        os.makedirs(LANDING_DIR)
        
    metadata = {
        "scraper_metadata": {
            "source_url": f"{GOODREADS_SEARCH_URL}?q={SEARCH_QUERY}&search_type=books",
            "search_query": SEARCH_QUERY,
            "user_agent": HEADERS['User-Agent'],
            "scrape_date": datetime.now().isoformat(),
            "num_records_scraped": len(books_data),
            "extraction_strategy": "Search List -> Visit Individual Book Page"
        },
        "books": books_data
    }

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    print(f"\n Proceso completado correctamente.")
    print(f"Datos de Goodreads guardados en: {OUTPUT_FILE}")
    print(f"Total de registros de libros: {len(books_data)}")


if __name__ == "__main__":
    main()
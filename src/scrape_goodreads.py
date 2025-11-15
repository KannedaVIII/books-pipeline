import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
import time 
import re # Necesario para expresiones regulares en la extracción de rating/count

# --- Configuración de Rutas y Parámetros ---
# Usamos os.path.dirname(os.path.dirname(__file__)) para obtener la raíz del proyecto (books-pipeline/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
# URL base sin parámetros, que se añaden dinámicamente en la función
GOODREADS_SEARCH_URL = "https://www.goodreads.com/search" 
SEARCH_QUERY = "data science" # Consulta de búsqueda requerida
OUTPUT_FILE = os.path.join(PROJECT_ROOT, 'landing', 'goodreads_books.json')
HEADERS = {
    # Simular un navegador para evitar ser bloqueado
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
MIN_BOOKS_TO_SCRAPE = 10 # Mínimo de 10 libros requerido
PAGINATION_DELAY_SECONDS = 1.5 

def get_goodreads_books(query: str, min_books: int = 10) -> list:
    """
    Scrapea Goodreads para obtener la información de los libros de la página de búsqueda.
    Extrae title, author, rating, ratings_count y book_url.
    """
    all_books = []
    page_num = 1
    
    print(f"Iniciando scraping de Goodreads para la consulta: '{query}'")

    while len(all_books) < min_books:
        params = {
            'q': query,
            'search_type': 'books',
            'page': page_num
        }
        
        print(f"Scraping página {page_num}...")
        try:
            response = requests.get(GOODREADS_SEARCH_URL, headers=HEADERS, params=params)
            response.raise_for_status() 
        except requests.exceptions.RequestException as e:
            print(f"Error al conectar con Goodreads en la página {page_num}: {e}")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Selector de la tabla de resultados (el más estable para esta vista)
        results_table = soup.find('table', class_='table_subject_books')
        
        if not results_table:
            # Si no encuentra la tabla, termina el scraping.
            if page_num == 1:
                print("No se encontraron resultados o la estructura HTML ha cambiado. Terminando.")
            break

        # Las filas de la tabla contienen los datos de los libros
        rows = results_table.find_all('tr')
        
        # Iteramos sobre las filas, saltando el encabezado (índice 0)
        for row in rows[1:]: 
            book_data = {
                'isbn10': None, # Inicializar campos opcionales según el requisito
                'isbn13': None
            }
            
            # 1. Título y URL del libro
            title_link = row.find('a', class_='bookTitle')
            if title_link:
                book_data['title'] = title_link.text.strip()
                book_data['book_url'] = "https://www.goodreads.com" + title_link['href']
            else:
                continue # Saltar si no hay título

            # 2. Autor
            author_link = row.find('a', class_='authorName')
            book_data['author'] = author_link.text.strip() if author_link else None
            
            # 3. Rating y ratings_count
            rating_span = row.find('span', class_='minirating')
            if rating_span:
                # El texto suele ser: "X.XX avg rating — NNN ratings"
                text = rating_span.text.strip()
                
                # Extraer rating (ej. X.XX)
                rating_match = re.search(r'([\d.]+)\s+avg rating', text)
                if rating_match:
                    try:
                        book_data['rating'] = float(rating_match.group(1))
                    except ValueError:
                        book_data['rating'] = None

                # Extraer ratings_count (ej. NNN, incluyendo comas)
                count_match = re.search(r'—\s+([\d,]+)\s+ratings', text)
                if count_match:
                    try:
                        # Eliminar comas y convertir a entero
                        book_data['ratings_count'] = int(count_match.group(1).replace(',', ''))
                    except ValueError:
                        book_data['ratings_count'] = None

            all_books.append(book_data)
            
            if len(all_books) >= min_books:
                break 

        page_num += 1
        
        if len(all_books) < min_books: 
            time.sleep(PAGINATION_DELAY_SECONDS)

    print(f"Scraping completado. Total de libros recolectados: {len(all_books)}")
    return all_books[:min_books] 

def main():
    
    # 1. Asegúrate de que el directorio 'landing' exista
    output_dir = os.path.dirname(OUTPUT_FILE)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    books_data = get_goodreads_books(SEARCH_QUERY, MIN_BOOKS_TO_SCRAPE)

    # 2. Preparar el JSON de salida con metadatos de documentación
    metadata = {
        "scraper_metadata": {
            "source_url": f"{GOODREADS_SEARCH_URL}?q={SEARCH_QUERY}&search_type=books",
            "search_query": SEARCH_QUERY,
            "selectors_used": {
                "results_container": "table.table_subject_books",
                "title_link": "a.bookTitle",
                "author_link": "a.authorName",
                "rating_span": "span.minirating"
            },
            "user_agent": HEADERS['User-Agent'],
            "scrape_date": datetime.now().isoformat(),
            "num_records_scraped": len(books_data)
        },
        "books": books_data
    }

    # 3. Guardar el archivo JSON
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)

    print(f"\n✅ Datos de Goodreads guardados en: {OUTPUT_FILE}")
    print(f"Total de registros de libros: {len(books_data)}")

if __name__ == "__main__":
    main()
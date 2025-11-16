# Esquema del Modelo Canónico: `dim_book.parquet`

**Fecha de Generación:** 2025-11-16 18:24:59
**Clave de Deduplicación:** `book_id` (ISBN-13 limpio o Hash si falta ISBN)
**Regla de Supervivencia:** Google Books > Título más largo > Fecha Publicación más reciente

## Estructura de Columnas (18 Campos)

| Nombre de Columna | Tipo de Dato (Pandas/Parquet) | Descripción | Nullable | Ejemplo |
| :--- | :--- | :--- | :--- | :--- |
| book_id | object | ID canónico (ISBN13 o Hash del título). Clave primaria. | No | -286021363280851241 |
| title | object | Título del libro (del registro ganador). | No | Numsense! Data Science for the Layman: No Math Added |
| title_normalized | object | Título en minúsculas y sin espacios extra (para deduplicación). | No | numsense! data science for the layman: no math added |
| author_principal | object | Autor principal (primer autor de la lista). | No | None |
| authors | object | Cadena de todos los autores (del registro ganador). | No | None |
| editorial | object | Nombre de la editorial (principalmente de Google Books). | Sí | O'Reilly Media, Inc. |
| anio_publicacion | object | Año de publicación (YYYY). | No | None |
| fecha_publicacion | object | Fecha de publicación normalizada (YYYY-MM-DD, YYYY-MM o YYYY). | Sí | 2022-12-06 |
| idioma | object | Código de idioma normalizado (BCP-47, ej. "en", "es"). | Sí | en |
| isbn10 | object | ISBN-10 limpio. | Sí | 1119931398 |
| isbn13 | object | ISBN-13 limpio. Clave preferente. | Sí | 9781098121181 |
| paginas | object | Número de páginas. | Sí | N/A |
| formato | object | Formato del libro (no implementado). | Sí | N/A |
| categoria | object | Categorías/Géneros (cadena separada). | Sí | Computers |
| precio | float64 | Monto del precio (solo si existe). | Sí | 46.79 |
| moneda | object | Código de moneda normalizado (ISO-4217, ej. "USD"). | Sí | EUR |
| fuente_ganadora | object | Fuente del registro que fue elegido como canónico. | No | goodreads |
| ts_ultima_actualizacion | object | Timestamp de la última actualización del registro. | No | 2025-11-16T18:24:59.843664 |

## Metodología de Normalización
- **Fechas:** Normalizadas a ISO-8601 (YYYY-MM-DD, YYYY-MM o YYYY).
- **Idioma:** Normalizado a BCP-47 (ej. 'en', 'es').
- **Moneda:** Normalizado a ISO-4217 (ej. 'USD', 'EUR').
- **Nombres:** Título se normaliza a `title_normalized` para la deduplicación.

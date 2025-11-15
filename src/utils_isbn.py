import re
from typing import Optional

def clean_isbn(isbn_raw: Optional[str]) -> Optional[str]:
    """
    Limpia una cadena ISBN (10 o 13) eliminando caracteres no numéricos
    y devolviendo el valor limpio si es de longitud 10 o 13.
    """
    if not isbn_raw:
        return None
    
    # 1. Convertir a string y eliminar espacios
    s = str(isbn_raw).strip()

    # 2. Reemplazar valores nulos (como "None" o "NaN") por None
    if s.lower() in {"none", "null", "nan", "missing"}:
        return None
    
    # 3. Eliminar todo lo que no sea dígito
    digits = re.sub(r'[^0-9]', '', s)

    # 4. Validar longitud
    if len(digits) == 13 or len(digits) == 10:
        return digits
    
    return None

def is_isbn13(value: Optional[str]) -> bool:
    """Comprueba si una cadena limpia es un ISBN-13 válido (13 dígitos)."""
    return bool(value) and len(value) == 13 and value.isdigit()

def is_isbn10(value: Optional[str]) -> bool:
    """Comprueba si una cadena limpia es un ISBN-10 válido (10 dígitos)."""
    return bool(value) and len(value) == 10 and value.isdigit()
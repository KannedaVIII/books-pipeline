import pandas as pd
from typing import Optional, Dict, Any, List
from datetime import datetime
import re

# Listas de valores controlados (para validaciones)
# BCP-47: es, en, fr, pt-BR, etc.
VALID_LANGUAGES = ["en", "es", "fr", "pt-br", "de", "it"] 
# ISO-4217: USD, EUR, GBP, etc.
VALID_CURRENCIES = ["USD", "EUR", "GBP", "CAD", "JPY", "AUD"] 

def normalize_date(date_raw: Optional[str]) -> Optional[str]:
    """
    Normaliza una fecha desde varios formatos a ISO-8601 (YYYY-MM-DD o YYYY).
    """
    if not date_raw:
        return None
        
    date_str = str(date_raw).strip()
    
    try:
        # Intenta parsear la fecha, usando coerce para manejar errores
        date_obj = pd.to_datetime(date_str, errors='coerce')
        
        if pd.isna(date_obj) or date_obj == pd.NaT:
            # Falló, intenta extraer solo el año si es posible
            if re.match(r'^\d{4}$', date_str):
                return date_str
            return None 

        # Si la fecha es completa, devuelve YYYY-MM-DD
        if not pd.isna(date_obj.day):
            return date_obj.strftime('%Y-%m-%d')
        # Si tiene mes, devuelve YYYY-MM
        elif not pd.isna(date_obj.month):
            return date_obj.strftime('%Y-%m')
        # Si solo tiene año, devuelve YYYY
        else:
            return date_obj.strftime('%Y')
            
    except Exception:
        return None

def normalize_language(lang_raw: Optional[str]) -> Optional[str]:
    """
    Normaliza el código de idioma a BCP-47.
    """
    if not lang_raw:
        return None
        
    lang = lang_raw.strip().lower()
    
    # Mapeo simple de códigos de dos o tres letras
    if lang in VALID_LANGUAGES:
        return lang
    
    # Mapeo de códigos de 3 letras comunes
    if lang == 'eng':
        return 'en'
    if lang == 'spa':
        return 'es'
    
    return None # Ignorar si no se puede normalizar

def check_currency(currency_code: Optional[Any]) -> bool:
    """
    Verifica si el código de moneda es un ISO-4217 conocido.
    CORRECCIÓN: Maneja valores no string (como NaN/float) antes de llamar a .upper().
    """
    if currency_code is None or pd.isna(currency_code):
        return False
    
    # Convertir a cadena solo si no es ya una cadena, y luego limpiar
    try:
        code_str = str(currency_code).strip()
        # Si después de convertir a str es un valor de float ('nan', 'none', etc.), lo ignoramos
        if code_str.lower() in {"nan", "none", ""}:
             return False
             
        return code_str.upper() in VALID_CURRENCIES
    except Exception:
        return False


def calculate_quality_metrics(df: pd.DataFrame, source_name: str, key_columns: List[str]) -> Dict[str, Any]:
    """
    Calcula métricas de calidad básicas para un DataFrame de origen.
    """
    total_rows = len(df)
    
    metrics: Dict[str, Any] = {
        'source': source_name,
        'timestamp': datetime.now().isoformat(),
        'total_rows': total_rows,
        'null_counts': {},
        'completeness_pct': {}
    }
    
    # 1. Completitud: Valores nulos en columnas clave
    for col in key_columns:
        null_count = df[col].isna().sum()
        metrics['null_counts'][col] = null_count
        
        valid_count = df[col].notna().sum()
        metrics['completeness_pct'][col] = round((valid_count / total_rows) * 100, 2)
        
    # 2. Formato: Idiomas válidos
    valid_langs_pct = round(
        (df['idioma_bcp47'].notna().sum() / total_rows) * 100, 2
    ) if total_rows else 0
    metrics['pct_valid_languages_bcp47'] = valid_langs_pct
    
    # 3. Formato: Monedas válidas
    if 'moneda_iso' in df.columns:
        # Contar filas donde moneda_iso NO es nulo
        valid_currencies_pct = round(
            (df['moneda_iso'].notna().sum() / total_rows) * 100, 2
        ) if total_rows else 0
        metrics['pct_valid_currencies_iso4217'] = valid_currencies_pct

    return metrics
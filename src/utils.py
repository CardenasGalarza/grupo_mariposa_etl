"""
Utilidades para el Pipeline ETL de Grupo Mariposa
Funciones auxiliares para logging, validación y formateo
"""

import logging
from datetime import datetime
from typing import Optional

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """
    Configura el sistema de logging para el pipeline.
    
    Args:
        log_level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Logger configurado
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger("GrupoMariposa_ETL")


def parse_date(date_value) -> Optional[datetime]:
    """
    Parsea una fecha en formato YYYYMMDD a objeto datetime.
    
    Args:
        date_value: Fecha en formato entero o string YYYYMMDD
    
    Returns:
        Objeto datetime o None si el formato es inválido
    """
    try:
        date_str = str(date_value)
        return datetime.strptime(date_str, "%Y%m%d")
    except (ValueError, TypeError):
        return None


def validate_date_range(start_date: int, end_date: int) -> bool:
    """
    Valida que el rango de fechas sea correcto.
    
    Args:
        start_date: Fecha de inicio en formato YYYYMMDD
        end_date: Fecha de fin en formato YYYYMMDD
    
    Returns:
        True si el rango es válido, False en caso contrario
    """
    start = parse_date(start_date)
    end = parse_date(end_date)
    
    if start is None or end is None:
        return False
    
    return start <= end


def format_date_for_partition(date_value: int) -> str:
    """
    Formatea una fecha para usar como nombre de partición.
    
    Args:
        date_value: Fecha en formato YYYYMMDD
    
    Returns:
        Fecha formateada como string YYYY-MM-DD
    """
    date_str = str(date_value)
    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"


def get_country_name(country_code: str) -> str:
    """
    Obtiene el nombre completo del país a partir de su código.
    
    Args:
        country_code: Código ISO de 2 letras del país
    
    Returns:
        Nombre completo del país
    """
    country_names = {
        "GT": "Guatemala",
        "PE": "Perú",
        "EC": "Ecuador",
        "SV": "El Salvador",
        "HN": "Honduras",
        "JM": "Jamaica"
    }
    return country_names.get(country_code.upper(), country_code)


def get_material_category(material_code: str) -> str:
    """
    Determina la categoría del material basado en su código.
    
    Args:
        material_code: Código del material (e.g., AA004003, BA018426)
    
    Returns:
        Categoría del material
    """
    if not material_code or material_code.strip() == "":
        return "DESCONOCIDO"
    
    prefix = material_code[:2].upper()
    categories = {
        "AA": "BEBIDAS_ALCOHOLICAS",
        "BA": "BEBIDAS_NO_ALCOHOLICAS",
        "CA": "CONCENTRADOS",
        "EN": "ENERGIA",
    }
    return categories.get(prefix, "OTROS")

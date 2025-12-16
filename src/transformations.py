"""
Transformaciones de Datos para el Pipeline ETL de Grupo Mariposa
Contiene todas las funciones de transformación de datos usando PySpark
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType
from typing import List


def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Estandariza los nombres de las columnas siguiendo convenciones de nomenclatura.
    - snake_case
    - Nombres descriptivos
    - Prefijos consistentes
    
    Args:
        df: DataFrame con columnas originales
    
    Returns:
        DataFrame con columnas renombradas
    """
    column_mapping = {
        "pais": "codigo_pais",
        "fecha_proceso": "fecha_proceso",
        "transporte": "id_transporte",
        "ruta": "id_ruta",
        "tipo_entrega": "tipo_entrega",
        "material": "codigo_material",
        "precio": "precio_original",
        "cantidad": "cantidad_original",
        "unidad": "unidad_original"
    }
    
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    
    return df


def convert_units_to_standard(df: DataFrame, cs_multiplier: int = 20) -> DataFrame:
    """
    Convierte todas las cantidades a una unidad estándar (unidades individuales).
    CS (cajas) se multiplica por el factor especificado.
    ST (unidades) se mantiene igual.
    
    Args:
        df: DataFrame con columnas cantidad_original y unidad_original
        cs_multiplier: Factor de conversión para cajas (default: 20)
    
    Returns:
        DataFrame con nueva columna cantidad_unidades_estandar
    """
    df = df.withColumn(
        "cantidad_unidades_estandar",
        F.when(F.col("unidad_original") == "CS", 
               F.col("cantidad_original") * cs_multiplier)
         .otherwise(F.col("cantidad_original"))
    )
    
    # Agregar columna de unidad estándar
    df = df.withColumn("unidad_estandar", F.lit("UNIDADES"))
    
    return df


def classify_delivery_types(
    df: DataFrame, 
    routine_types: List[str], 
    bonus_types: List[str]
) -> DataFrame:
    """
    Clasifica los tipos de entrega y agrega columnas indicadoras.
    
    Args:
        df: DataFrame con columna tipo_entrega
        routine_types: Lista de códigos de entregas de rutina (e.g., ["ZPRE", "ZVE1"])
        bonus_types: Lista de códigos de entregas con bonificación (e.g., ["Z04", "Z05"])
    
    Returns:
        DataFrame con columnas adicionales de clasificación
    """
    # Columna booleana para entregas de rutina
    df = df.withColumn(
        "es_entrega_rutina",
        F.when(F.col("tipo_entrega").isin(routine_types), True)
         .otherwise(False)
    )
    
    # Columna booleana para entregas con bonificación
    df = df.withColumn(
        "es_entrega_bonificacion",
        F.when(F.col("tipo_entrega").isin(bonus_types), True)
         .otherwise(False)
    )
    
    # Columna de categoría de entrega descriptiva
    df = df.withColumn(
        "categoria_entrega",
        F.when(F.col("tipo_entrega").isin(routine_types), "RUTINA")
         .when(F.col("tipo_entrega").isin(bonus_types), "BONIFICACION")
         .otherwise("OTRO")
    )
    
    return df


def filter_valid_delivery_types(
    df: DataFrame, 
    routine_types: List[str], 
    bonus_types: List[str]
) -> DataFrame:
    """
    Filtra solo los tipos de entrega válidos (rutina y bonificación).
    Excluye tipos como COBR y otros no especificados.
    
    Args:
        df: DataFrame con columna tipo_entrega
        routine_types: Lista de códigos de entregas de rutina
        bonus_types: Lista de códigos de entregas con bonificación
    
    Returns:
        DataFrame filtrado
    """
    valid_types = routine_types + bonus_types
    return df.filter(F.col("tipo_entrega").isin(valid_types))


def remove_anomalies(
    df: DataFrame,
    remove_empty_materials: bool = True,
    remove_duplicates: bool = True
) -> DataFrame:
    """
    Elimina anomalías detectadas en los datos.
    
    Args:
        df: DataFrame con datos a limpiar
        remove_empty_materials: Si True, elimina registros con material vacío
        remove_duplicates: Si True, elimina registros duplicados
    
    Returns:
        DataFrame limpio
    """
    if remove_empty_materials:
        # Eliminar registros con material vacío o nulo
        df = df.filter(
            (F.col("codigo_material").isNotNull()) & 
            (F.trim(F.col("codigo_material")) != "")
        )
    
    if remove_duplicates:
        # Eliminar duplicados exactos
        df = df.dropDuplicates()
    
    return df


def add_calculated_columns(df: DataFrame) -> DataFrame:
    """
    Agrega columnas calculadas adicionales para análisis.
    Puntos extra: columnas con fundamento analítico.
    
    Args:
        df: DataFrame transformado
    
    Returns:
        DataFrame con columnas adicionales
    """
    # Precio unitario normalizado (precio por unidad individual)
    df = df.withColumn(
        "precio_unitario_normalizado",
        F.when(F.col("cantidad_unidades_estandar") > 0,
               F.col("precio_original") / F.col("cantidad_unidades_estandar"))
         .otherwise(F.lit(0.0))
    )
    
    # Valor total de la entrega (precio * cantidad normalizada)
    df = df.withColumn(
        "valor_total_entrega",
        F.col("precio_original")  # El precio ya es el total de la línea
    )
    
    # Categoría del material basada en el prefijo
    df = df.withColumn(
        "categoria_material",
        F.when(F.col("codigo_material").startswith("AA"), "BEBIDAS_ALCOHOLICAS")
         .when(F.col("codigo_material").startswith("BA"), "BEBIDAS_NO_ALCOHOLICAS")
         .when(F.col("codigo_material").startswith("CA"), "CONCENTRADOS")
         .when(F.col("codigo_material").startswith("EN"), "ENERGIA")
         .otherwise("OTROS")
    )
    
    # Nombre del país completo
    df = df.withColumn(
        "nombre_pais",
        F.when(F.col("codigo_pais") == "GT", "Guatemala")
         .when(F.col("codigo_pais") == "PE", "Perú")
         .when(F.col("codigo_pais") == "EC", "Ecuador")
         .when(F.col("codigo_pais") == "SV", "El Salvador")
         .when(F.col("codigo_pais") == "HN", "Honduras")
         .when(F.col("codigo_pais") == "JM", "Jamaica")
         .otherwise(F.col("codigo_pais"))
    )
    
    # Fecha formateada para mejor legibilidad
    df = df.withColumn(
        "fecha_proceso_formateada",
        F.to_date(F.col("fecha_proceso").cast(StringType()), "yyyyMMdd")
    )
    
    # Mes y año para análisis temporal
    df = df.withColumn(
        "anio_proceso",
        F.year(F.col("fecha_proceso_formateada"))
    )
    
    df = df.withColumn(
        "mes_proceso",
        F.month(F.col("fecha_proceso_formateada"))
    )
    
    # Indicador de precio cero (para análisis de productos en promoción)
    df = df.withColumn(
        "es_precio_cero",
        F.when(F.col("precio_original") == 0, True).otherwise(False)
    )
    
    return df


def select_final_columns(df: DataFrame) -> DataFrame:
    """
    Selecciona y ordena las columnas finales del dataset de salida.
    
    Args:
        df: DataFrame con todas las columnas
    
    Returns:
        DataFrame con columnas ordenadas para el output
    """
    final_columns = [
        # Identificadores
        "codigo_pais",
        "nombre_pais",
        "fecha_proceso",
        "fecha_proceso_formateada",
        "anio_proceso",
        "mes_proceso",
        "id_transporte",
        "id_ruta",
        
        # Producto
        "codigo_material",
        "categoria_material",
        
        # Tipo de entrega
        "tipo_entrega",
        "categoria_entrega",
        "es_entrega_rutina",
        "es_entrega_bonificacion",
        
        # Cantidades originales
        "cantidad_original",
        "unidad_original",
        
        # Cantidades normalizadas
        "cantidad_unidades_estandar",
        "unidad_estandar",
        
        # Valores monetarios
        "precio_original",
        "precio_unitario_normalizado",
        "valor_total_entrega",
        
        # Indicadores
        "es_precio_cero"
    ]
    
    # Seleccionar solo columnas que existen
    existing_columns = [col for col in final_columns if col in df.columns]
    return df.select(existing_columns)

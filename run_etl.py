"""
Pipeline ETL - Grupo Mariposa
Prueba Técnica: Flujo de datos parametrizable con PySpark y OmegaConf

Autor: Giancarlos Cardenas Galarza
Fecha: Diciembre 2025

Requisitos cumplidos:
1. ✓ Leer archivo CSV
2. ✓ Filtrar por rango de fechas configurable (start_date, end_date)
3. ✓ Generar salidas particionadas por fecha_proceso (data/processed/${fecha_proceso})
4. ✓ Usar OmegaConf para controlar todos los parámetros desde YAML
5. ✓ Parametrizable por país
6. ✓ Conversión de unidades: CS (cajas) = 20 unidades, ST = 1 unidad
7. ✓ Columnas por tipo de entrega: ZPRE/ZVE1 = rutina, Z04/Z05 = bonificación (excluir otros)
8. ✓ Nombres de columnas estandarizados
9. ✓ Detectar/eliminar anomalías
10. ✓ Columnas adicionales con fundamento
11. ✓ Documentación gráfica en docs/flujo_datos.md

Uso:
    python run_etl.py
    python run_etl.py --start_date 20250101 --end_date 20250331
    python run_etl.py --start_date 20250101 --end_date 20250630 --country GT
"""

import os
import sys
import argparse

# Configurar HADOOP_HOME para Windows ANTES de importar PySpark
if sys.platform == 'win32':
    os.environ['HADOOP_HOME'] = os.environ.get('HADOOP_HOME', 'C:\\hadoop')
    hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin')
    if hadoop_bin not in os.environ.get('PATH', ''):
        os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from omegaconf import OmegaConf


def parse_arguments():
    """Parsea argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Pipeline ETL para entregas de productos - Grupo Mariposa"
    )
    parser.add_argument("--config", type=str, default="config/config.yaml",
                        help="Ruta al archivo de configuración YAML")
    parser.add_argument("--start_date", type=int, 
                        help="Fecha de inicio (YYYYMMDD)")
    parser.add_argument("--end_date", type=int,
                        help="Fecha de fin (YYYYMMDD)")
    parser.add_argument("--country", type=str,
                        help="Código de país (GT, PE, EC, SV, HN, JM)")
    return parser.parse_args()


def load_config(args):
    """Carga configuración desde YAML y aplica overrides de CLI."""
    # Cargar configuración base
    config = OmegaConf.load(args.config)
    
    # Aplicar overrides de línea de comandos
    if args.start_date:
        config.dates.start_date = args.start_date
    if args.end_date:
        config.dates.end_date = args.end_date
    if args.country:
        config.filter.country = args.country
    
    return config


def create_spark_session(config):
    """Crea y configura la sesión de Spark."""
    spark = (
        SparkSession.builder
        .appName(config.spark.app_name)
        .master(config.spark.master)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(config.spark.log_level)
    return spark


def read_csv(spark, config):
    """
    REQUISITO 1: Leer archivo CSV.
    """
    input_path = config.paths.input_file
    print(f"[1] Leyendo archivo CSV: {input_path}")
    
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )
    
    print(f"    ✓ Registros leídos: {df.count()}")
    return df


def filter_by_date_range(df, config):
    """
    REQUISITO 2: Filtrar por rango de fechas configurable (start_date, end_date).
    """
    start_date = config.dates.start_date
    end_date = config.dates.end_date
    
    print(f"[2] Filtrando por rango de fechas: {start_date} - {end_date}")
    
    df_filtered = df.filter(
        (F.col("fecha_proceso") >= start_date) & 
        (F.col("fecha_proceso") <= end_date)
    )
    
    print(f"    ✓ Registros en rango: {df_filtered.count()}")
    return df_filtered


def filter_by_country(df, config):
    """
    REQUISITO 5: Parametrizable por país.
    """
    country = config.filter.get("country")
    
    if country is None:
        print("[5] Sin filtro de país - procesando todos los países")
        return df
    
    print(f"[5] Filtrando por país: {country}")
    df_filtered = df.filter(F.col("pais") == country.upper())
    print(f"    ✓ Registros del país {country}: {df_filtered.count()}")
    return df_filtered


def filter_valid_delivery_types(df, config):
    """
    REQUISITO 7 (parte 1): Filtrar solo tipos de entrega válidos.
    ZPRE, ZVE1 = rutina
    Z04, Z05 = bonificación
    Excluir COBR y otros.
    """
    routine_types = list(config.delivery_types.routine)
    bonus_types = list(config.delivery_types.bonus)
    valid_types = routine_types + bonus_types
    
    print(f"[7a] Filtrando tipos de entrega válidos: {valid_types}")
    print(f"     (Excluyendo: COBR y otros)")
    
    df_filtered = df.filter(F.col("tipo_entrega").isin(valid_types))
    print(f"     ✓ Registros con tipos válidos: {df_filtered.count()}")
    return df_filtered


def remove_anomalies(df, config):
    """
    REQUISITO 9: Detectar/eliminar anomalías.
    - Materiales vacíos
    - Registros duplicados
    """
    print("[9] Detectando y eliminando anomalías...")
    
    initial_count = df.count()
    
    # Eliminar registros con material vacío
    if config.data_quality.remove_empty_materials:
        df = df.filter(
            (F.col("material").isNotNull()) & 
            (F.trim(F.col("material")) != "")
        )
        after_empty = df.count()
        print(f"    - Eliminados {initial_count - after_empty} registros con material vacío")
    
    # Eliminar duplicados
    if config.data_quality.remove_duplicates:
        before_dup = df.count()
        df = df.dropDuplicates()
        after_dup = df.count()
        print(f"    - Eliminados {before_dup - after_dup} registros duplicados")
    
    print(f"    ✓ Registros después de limpieza: {df.count()}")
    return df


def standardize_column_names(df):
    """
    REQUISITO 8: Proponer un estándar correcto en el nombre de las columnas.
    Usando snake_case con nombres descriptivos.
    """
    print("[8] Estandarizando nombres de columnas...")
    
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
    
    print("    ✓ Columnas renombradas según estándar snake_case")
    return df


def convert_units(df, config):
    """
    REQUISITO 6: Conversión de unidades.
    CS (cajas) = 20 unidades
    ST (unidades) = 1 unidad
    Normalizar todo a unidades.
    """
    cs_multiplier = config.units.cs_to_units
    
    print(f"[6] Convirtiendo unidades (CS × {cs_multiplier} = unidades)...")
    
    # Nueva columna con cantidad normalizada a unidades
    df = df.withColumn(
        "cantidad_unidades",
        F.when(F.col("unidad_original") == "CS", 
               F.col("cantidad_original") * cs_multiplier)
         .otherwise(F.col("cantidad_original"))
    )
    
    # Unidad estándar
    df = df.withColumn("unidad_estandar", F.lit("UNIDADES"))
    
    print("    ✓ Todas las cantidades convertidas a UNIDADES")
    return df


def add_delivery_type_columns(df, config):
    """
    REQUISITO 7 (parte 2): Agregar una columna por cada tipo de entrega.
    - es_entrega_rutina (ZPRE, ZVE1)
    - es_entrega_bonificacion (Z04, Z05)
    """
    routine_types = list(config.delivery_types.routine)
    bonus_types = list(config.delivery_types.bonus)
    
    print("[7b] Agregando columnas por tipo de entrega...")
    
    # Columna para entregas de rutina (ZPRE, ZVE1)
    df = df.withColumn(
        "es_entrega_rutina",
        F.when(F.col("tipo_entrega").isin(routine_types), True)
         .otherwise(False)
    )
    
    # Columna para entregas con bonificación (Z04, Z05)
    df = df.withColumn(
        "es_entrega_bonificacion",
        F.when(F.col("tipo_entrega").isin(bonus_types), True)
         .otherwise(False)
    )
    
    # Columna descriptiva de categoría
    df = df.withColumn(
        "categoria_entrega",
        F.when(F.col("tipo_entrega").isin(routine_types), "RUTINA")
         .when(F.col("tipo_entrega").isin(bonus_types), "BONIFICACION")
         .otherwise("OTRO")
    )
    
    print(f"    ✓ Columna es_entrega_rutina: ZPRE, ZVE1 = True")
    print(f"    ✓ Columna es_entrega_bonificacion: Z04, Z05 = True")
    return df


def add_extra_columns(df):
    """
    REQUISITO 10: Columnas adicionales con fundamento.
    """
    print("[10] Agregando columnas adicionales con fundamento analítico...")
    
    # Precio unitario (precio / cantidad en unidades)
    # Fundamento: Permite comparar precios entre productos de diferentes tamaños
    df = df.withColumn(
        "precio_unitario",
        F.when(F.col("cantidad_unidades") > 0,
               F.round(F.col("precio_original") / F.col("cantidad_unidades"), 4))
         .otherwise(F.lit(0.0))
    )
    print("    ✓ precio_unitario: Precio por unidad individual (comparabilidad de precios)")
    
    # Valor total de la entrega
    # Fundamento: Confirma el valor monetario total de cada línea
    df = df.withColumn(
        "valor_total",
        F.col("precio_original")
    )
    print("    ✓ valor_total: Valor monetario total de la línea")
    
    # Categoría del material basada en prefijo
    # Fundamento: Permite análisis por categoría de producto
    df = df.withColumn(
        "categoria_producto",
        F.when(F.col("codigo_material").startswith("AA"), "BEBIDAS_ALCOHOLICAS")
         .when(F.col("codigo_material").startswith("BA"), "BEBIDAS_NO_ALCOHOLICAS")
         .when(F.col("codigo_material").startswith("CA"), "CONCENTRADOS")
         .when(F.col("codigo_material").startswith("EN"), "ENERGIA")
         .otherwise("OTROS")
    )
    print("    ✓ categoria_producto: Clasificación por tipo de bebida (análisis por categoría)")
    
    # Nombre del país completo
    # Fundamento: Mayor legibilidad en reportes
    df = df.withColumn(
        "nombre_pais",
        F.when(F.col("codigo_pais") == "GT", "Guatemala")
         .when(F.col("codigo_pais") == "PE", "Peru")
         .when(F.col("codigo_pais") == "EC", "Ecuador")
         .when(F.col("codigo_pais") == "SV", "El Salvador")
         .when(F.col("codigo_pais") == "HN", "Honduras")
         .when(F.col("codigo_pais") == "JM", "Jamaica")
         .otherwise(F.col("codigo_pais"))
    )
    print("    ✓ nombre_pais: Nombre completo del país (legibilidad)")
    
    # Fecha formateada
    # Fundamento: Facilita consultas por fecha y ordenamiento
    df = df.withColumn(
        "fecha_formateada",
        F.to_date(F.col("fecha_proceso").cast(StringType()), "yyyyMMdd")
    )
    print("    ✓ fecha_formateada: Fecha como tipo Date (ordenamiento y filtros)")
    
    # Indicador de precio cero
    # Fundamento: Identifica productos en promoción o muestras gratis
    df = df.withColumn(
        "es_promocion_gratis",
        F.when(F.col("precio_original") == 0, True).otherwise(False)
    )
    print("    ✓ es_promocion_gratis: Indicador de productos sin costo (análisis de promociones)")
    
    return df


def select_final_columns(df):
    """Selecciona y ordena las columnas finales del dataset."""
    final_columns = [
        # Identificadores geográficos
        "codigo_pais",
        "nombre_pais",
        
        # Fecha
        "fecha_proceso",
        "fecha_formateada",
        
        # Identificadores de transporte
        "id_transporte",
        "id_ruta",
        
        # Producto
        "codigo_material",
        "categoria_producto",
        
        # Tipo de entrega (REQUISITO 7)
        "tipo_entrega",
        "categoria_entrega",
        "es_entrega_rutina",
        "es_entrega_bonificacion",
        
        # Cantidades (REQUISITO 6)
        "cantidad_original",
        "unidad_original",
        "cantidad_unidades",
        "unidad_estandar",
        
        # Valores monetarios (REQUISITO 10)
        "precio_original",
        "precio_unitario",
        "valor_total",
        
        # Indicadores adicionales (REQUISITO 10)
        "es_promocion_gratis"
    ]
    
    return df.select(final_columns)


def write_partitioned_output(df, config):
    """
    REQUISITO 3: Generar salidas particionadas por fecha_proceso.
    output_path: data/processed/${fecha_proceso}
    """
    output_path = config.paths.output_path
    output_format = config.output.format
    
    print(f"[3] Escribiendo salida particionada por fecha_proceso...")
    print(f"    Ruta: {output_path}/${{fecha_proceso}}")
    print(f"    Formato: {output_format}")
    
    writer = df.write.mode("overwrite").partitionBy("fecha_proceso")
    
    if output_format == "parquet":
        writer.parquet(output_path)
    elif output_format == "csv":
        writer.option("header", "true").csv(output_path)
    else:
        writer.parquet(output_path)
    
    # Mostrar particiones creadas
    print(f"    ✓ Datos escritos exitosamente")
    
    # Contar registros por fecha
    print("\n    Particiones creadas:")
    df.groupBy("fecha_proceso").count().orderBy("fecha_proceso").show(truncate=False)


def print_summary(df):
    """Imprime resumen del procesamiento."""
    print("\n" + "=" * 70)
    print("RESUMEN DEL PROCESAMIENTO")
    print("=" * 70)
    
    print(f"\nTotal registros procesados: {df.count()}")
    
    print("\n📊 Distribución por país:")
    df.groupBy("codigo_pais", "nombre_pais").count().orderBy("count", ascending=False).show()
    
    print("📊 Distribución por tipo de entrega:")
    df.groupBy("tipo_entrega", "categoria_entrega").count().orderBy("count", ascending=False).show()
    
    print("📊 Distribución por categoría de producto:")
    df.groupBy("categoria_producto").count().orderBy("count", ascending=False).show()
    
    print("\n📋 Columnas del dataset final:")
    for i, col in enumerate(df.columns, 1):
        print(f"    {i:2}. {col}")
    
    print("\n📝 Muestra de datos (5 registros):")
    df.select(
        "codigo_pais", "fecha_proceso", "codigo_material",
        "tipo_entrega", "categoria_entrega",
        "cantidad_original", "unidad_original", "cantidad_unidades"
    ).show(5, truncate=False)


def main():
    """Función principal del pipeline ETL."""
    print("=" * 70)
    print("   PIPELINE ETL - GRUPO MARIPOSA")
    print("   Prueba Técnica: Flujo de datos con PySpark y OmegaConf")
    print("=" * 70)
    
    # Parsear argumentos
    args = parse_arguments()
    
    # REQUISITO 4: Cargar configuración desde YAML usando OmegaConf
    print(f"\n[4] Cargando configuración desde: {args.config}")
    config = load_config(args)
    print(f"    ✓ Configuración cargada con OmegaConf")
    print(f"    - start_date: {config.dates.start_date}")
    print(f"    - end_date: {config.dates.end_date}")
    print(f"    - country: {config.filter.country or 'Todos'}")
    
    # Crear SparkSession
    print("\n[*] Inicializando SparkSession...")
    spark = create_spark_session(config)
    print(f"    ✓ Spark {spark.version} inicializado")
    
    try:
        # REQUISITO 1: Leer CSV
        df = read_csv(spark, config)
        
        # REQUISITO 2: Filtrar por rango de fechas
        df = filter_by_date_range(df, config)
        
        # REQUISITO 5: Filtrar por país (si está configurado)
        df = filter_by_country(df, config)
        
        # REQUISITO 7a: Filtrar tipos de entrega válidos (excluir COBR y otros)
        df = filter_valid_delivery_types(df, config)
        
        # REQUISITO 9: Eliminar anomalías
        df = remove_anomalies(df, config)
        
        # REQUISITO 8: Estandarizar nombres de columnas
        df = standardize_column_names(df)
        
        # REQUISITO 6: Convertir unidades
        df = convert_units(df, config)
        
        # REQUISITO 7b: Agregar columnas por tipo de entrega
        df = add_delivery_type_columns(df, config)
        
        # REQUISITO 10: Columnas adicionales con fundamento
        df = add_extra_columns(df)
        
        # Seleccionar columnas finales
        df = select_final_columns(df)
        
        # REQUISITO 3: Escribir salida particionada
        write_partitioned_output(df, config)
        
        # Mostrar resumen
        print_summary(df)
        
        print("\n" + "=" * 70)
        print("   ✅ PIPELINE ETL COMPLETADO EXITOSAMENTE")
        print("=" * 70)
        
        return df
        
    except Exception as e:
        print(f"\n❌ Error en el pipeline: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
        
    finally:
        spark.stop()
        print("\n[*] SparkSession detenida")


if __name__ == "__main__":
    main()

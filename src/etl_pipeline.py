"""
Pipeline ETL Principal - Grupo Mariposa
Orquesta la lectura, transformación y escritura de datos de entregas de productos
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from omegaconf import DictConfig
import os

from .transformations import (
    standardize_column_names,
    convert_units_to_standard,
    classify_delivery_types,
    filter_valid_delivery_types,
    remove_anomalies,
    add_calculated_columns,
    select_final_columns
)
from .utils import setup_logging, validate_date_range


class ETLPipeline:
    """
    Pipeline ETL para procesar datos de entregas de productos de Grupo Mariposa.
    
    Características:
    - Configuración parametrizable via OmegaConf
    - Filtrado por rango de fechas y país
    - Conversión de unidades a estándar
    - Clasificación de tipos de entrega
    - Limpieza de anomalías
    - Salida particionada por fecha
    """
    
    def __init__(self, config: DictConfig):
        """
        Inicializa el pipeline con la configuración proporcionada.
        
        Args:
            config: Configuración OmegaConf cargada desde YAML
        """
        self.config = config
        self.logger = setup_logging(config.spark.get("log_level", "INFO"))
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self) -> SparkSession:
        """
        Crea y configura la sesión de Spark.
        
        Returns:
            SparkSession configurada
        """
        # Configurar HADOOP_HOME para Windows
        import sys
        if sys.platform == 'win32':
            hadoop_home = os.environ.get('HADOOP_HOME', 'C:\\hadoop')
            os.environ['HADOOP_HOME'] = hadoop_home
            # Agregar al PATH si no está
            hadoop_bin = os.path.join(hadoop_home, 'bin')
            if hadoop_bin not in os.environ.get('PATH', ''):
                os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
        
        spark = (
            SparkSession.builder
            .appName(self.config.spark.app_name)
            .master(self.config.spark.master)
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )
        
        # Configurar nivel de log
        spark.sparkContext.setLogLevel(self.config.spark.log_level)
        
        self.logger.info(f"SparkSession creada: {self.config.spark.app_name}")
        return spark
    
    def read_source_data(self) -> DataFrame:
        """
        Lee el archivo CSV de entrada.
        
        Returns:
            DataFrame con los datos crudos
        """
        input_path = self.config.paths.input_file
        
        self.logger.info(f"Leyendo datos desde: {input_path}")
        
        df = (
            self.spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(input_path)
        )
        
        record_count = df.count()
        self.logger.info(f"Registros leídos: {record_count}")
        
        return df
    
    def filter_by_date_range(self, df: DataFrame) -> DataFrame:
        """
        Filtra los datos por el rango de fechas configurado.
        
        Args:
            df: DataFrame con columna fecha_proceso
        
        Returns:
            DataFrame filtrado por fechas
        """
        start_date = self.config.dates.start_date
        end_date = self.config.dates.end_date
        
        # Validar rango de fechas
        if not validate_date_range(start_date, end_date):
            raise ValueError(f"Rango de fechas inválido: {start_date} - {end_date}")
        
        self.logger.info(f"Filtrando por fechas: {start_date} - {end_date}")
        
        # El campo fecha_proceso puede estar como entero o string
        # Asegurar que la comparación sea correcta
        df_filtered = df.filter(
            (F.col("fecha_proceso") >= start_date) & 
            (F.col("fecha_proceso") <= end_date)
        )
        
        filtered_count = df_filtered.count()
        self.logger.info(f"Registros después de filtro de fechas: {filtered_count}")
        
        return df_filtered
    
    def filter_by_country(self, df: DataFrame) -> DataFrame:
        """
        Filtra los datos por país si está configurado.
        
        Args:
            df: DataFrame con columna pais o codigo_pais
        
        Returns:
            DataFrame filtrado por país
        """
        country = self.config.filter.get("country")
        
        if country is None:
            self.logger.info("Sin filtro de país - procesando todos los países")
            return df
        
        self.logger.info(f"Filtrando por país: {country}")
        
        # Determinar el nombre de la columna de país
        country_col = "codigo_pais" if "codigo_pais" in df.columns else "pais"
        
        df_filtered = df.filter(F.col(country_col) == country.upper())
        
        filtered_count = df_filtered.count()
        self.logger.info(f"Registros después de filtro de país: {filtered_count}")
        
        return df_filtered
    
    def apply_transformations(self, df: DataFrame) -> DataFrame:
        """
        Aplica todas las transformaciones al DataFrame.
        
        Args:
            df: DataFrame con datos filtrados
        
        Returns:
            DataFrame completamente transformado
        """
        self.logger.info("Aplicando transformaciones...")
        
        # 1. Estandarizar nombres de columnas
        df = standardize_column_names(df)
        self.logger.info("  ✓ Nombres de columnas estandarizados")
        
        # 2. Filtrar solo tipos de entrega válidos
        routine_types = list(self.config.delivery_types.routine)
        bonus_types = list(self.config.delivery_types.bonus)
        
        df = filter_valid_delivery_types(df, routine_types, bonus_types)
        self.logger.info(f"  ✓ Tipos de entrega filtrados (rutina: {routine_types}, bonificación: {bonus_types})")
        
        # 3. Eliminar anomalías
        df = remove_anomalies(
            df,
            remove_empty_materials=self.config.data_quality.remove_empty_materials,
            remove_duplicates=self.config.data_quality.remove_duplicates
        )
        self.logger.info("  ✓ Anomalías eliminadas")
        
        # 4. Convertir unidades a estándar
        cs_multiplier = self.config.units.cs_to_units
        df = convert_units_to_standard(df, cs_multiplier)
        self.logger.info(f"  ✓ Unidades convertidas (CS × {cs_multiplier})")
        
        # 5. Clasificar tipos de entrega
        df = classify_delivery_types(df, routine_types, bonus_types)
        self.logger.info("  ✓ Tipos de entrega clasificados")
        
        # 6. Agregar columnas calculadas
        df = add_calculated_columns(df)
        self.logger.info("  ✓ Columnas calculadas agregadas")
        
        # 7. Seleccionar columnas finales
        df = select_final_columns(df)
        self.logger.info("  ✓ Columnas finales seleccionadas")
        
        final_count = df.count()
        self.logger.info(f"Total de registros transformados: {final_count}")
        
        return df
    
    def write_output(self, df: DataFrame) -> None:
        """
        Escribe el DataFrame de salida particionado por fecha.
        
        Args:
            df: DataFrame transformado
        """
        output_path = self.config.paths.output_path
        output_format = self.config.output.format
        output_mode = self.config.output.mode
        partition_by = self.config.output.partition_by
        
        self.logger.info(f"Escribiendo salida en: {output_path}")
        self.logger.info(f"  Formato: {output_format}, Modo: {output_mode}, Partición: {partition_by}")
        
        writer = (
            df.write
            .mode(output_mode)
            .partitionBy(partition_by)
        )
        
        if output_format == "parquet":
            writer.parquet(output_path)
        elif output_format == "csv":
            writer.option("header", "true").csv(output_path)
        elif output_format == "json":
            writer.json(output_path)
        else:
            raise ValueError(f"Formato de salida no soportado: {output_format}")
        
        self.logger.info("✓ Datos escritos exitosamente")
    
    def run(self) -> DataFrame:
        """
        Ejecuta el pipeline ETL completo.
        
        Returns:
            DataFrame procesado
        """
        self.logger.info("=" * 60)
        self.logger.info("INICIANDO PIPELINE ETL - GRUPO MARIPOSA")
        self.logger.info("=" * 60)
        
        try:
            # 1. Leer datos fuente
            df = self.read_source_data()
            
            # 2. Filtrar por rango de fechas
            df = self.filter_by_date_range(df)
            
            # 3. Filtrar por país (si está configurado)
            df = self.filter_by_country(df)
            
            # 4. Aplicar transformaciones
            df = self.apply_transformations(df)
            
            # 5. Escribir salida
            self.write_output(df)
            
            self.logger.info("=" * 60)
            self.logger.info("PIPELINE ETL COMPLETADO EXITOSAMENTE")
            self.logger.info("=" * 60)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error en el pipeline: {str(e)}")
            raise
        
        finally:
            self.stop()
    
    def stop(self) -> None:
        """
        Detiene la sesión de Spark.
        """
        if self.spark:
            self.spark.stop()
            self.logger.info("SparkSession detenida")

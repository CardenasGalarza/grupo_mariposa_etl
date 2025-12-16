"""
Punto de Entrada Principal - Pipeline ETL Grupo Mariposa

Este script permite ejecutar el pipeline ETL con parámetros configurables
desde línea de comandos o desde el archivo config.yaml.

Uso:
    # Ejecutar con configuración por defecto
    python -m src.main
    
    # Ejecutar con rango de fechas específico
    python -m src.main --start_date 20250101 --end_date 20250331
    
    # Ejecutar para un país específico
    python -m src.main --country GT
    
    # Ejecutar con todos los parámetros
    python -m src.main --start_date 20250101 --end_date 20250630 --country EC
"""

import argparse
import os
import sys
from pathlib import Path

from omegaconf import OmegaConf, DictConfig

# Agregar el directorio padre al path para imports relativos
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.etl_pipeline import ETLPipeline
from src.utils import setup_logging


def parse_arguments() -> argparse.Namespace:
    """
    Parsea los argumentos de línea de comandos.
    
    Returns:
        Namespace con los argumentos parseados
    """
    parser = argparse.ArgumentParser(
        description="Pipeline ETL para entregas de productos - Grupo Mariposa",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python -m src.main
  python -m src.main --start_date 20250101 --end_date 20250331
  python -m src.main --country GT
  python -m src.main --start_date 20250101 --end_date 20250630 --country EC
        """
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default="config/config.yaml",
        help="Ruta al archivo de configuración YAML (default: config/config.yaml)"
    )
    
    parser.add_argument(
        "--start_date",
        type=int,
        help="Fecha de inicio en formato YYYYMMDD (e.g., 20250101)"
    )
    
    parser.add_argument(
        "--end_date",
        type=int,
        help="Fecha de fin en formato YYYYMMDD (e.g., 20250630)"
    )
    
    parser.add_argument(
        "--country",
        type=str,
        choices=["GT", "PE", "EC", "SV", "HN", "JM"],
        help="Código de país para filtrar (GT, PE, EC, SV, HN, JM)"
    )
    
    parser.add_argument(
        "--input_file",
        type=str,
        help="Ruta al archivo CSV de entrada"
    )
    
    parser.add_argument(
        "--output_path",
        type=str,
        help="Ruta del directorio de salida"
    )
    
    parser.add_argument(
        "--output_format",
        type=str,
        choices=["parquet", "csv", "json"],
        help="Formato de salida (parquet, csv, json)"
    )
    
    return parser.parse_args()


def load_config(config_path: str, args: argparse.Namespace) -> DictConfig:
    """
    Carga la configuración desde YAML y aplica overrides de línea de comandos.
    
    Args:
        config_path: Ruta al archivo de configuración YAML
        args: Argumentos de línea de comandos
    
    Returns:
        Configuración combinada como DictConfig
    """
    # Cargar configuración base desde YAML
    if os.path.exists(config_path):
        config = OmegaConf.load(config_path)
    else:
        raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_path}")
    
    # Aplicar overrides de línea de comandos
    if args.start_date is not None:
        config.dates.start_date = args.start_date
    
    if args.end_date is not None:
        config.dates.end_date = args.end_date
    
    if args.country is not None:
        config.filter.country = args.country
    
    if args.input_file is not None:
        config.paths.input_file = args.input_file
    
    if args.output_path is not None:
        config.paths.output_path = args.output_path
    
    if args.output_format is not None:
        config.output.format = args.output_format
    
    return config


def validate_config(config: DictConfig) -> None:
    """
    Valida que la configuración tenga todos los campos requeridos.
    
    Args:
        config: Configuración a validar
    
    Raises:
        ValueError: Si falta algún campo requerido
    """
    required_fields = [
        ("dates.start_date", config.dates.start_date),
        ("dates.end_date", config.dates.end_date),
        ("paths.input_file", config.paths.input_file),
        ("paths.output_path", config.paths.output_path),
    ]
    
    for field_name, field_value in required_fields:
        if field_value is None:
            raise ValueError(f"Campo requerido faltante: {field_name}")
    
    # Validar que el archivo de entrada existe
    if not os.path.exists(config.paths.input_file):
        raise FileNotFoundError(
            f"Archivo de entrada no encontrado: {config.paths.input_file}"
        )


def print_config_summary(config: DictConfig, logger) -> None:
    """
    Imprime un resumen de la configuración activa.
    
    Args:
        config: Configuración activa
        logger: Logger para imprimir
    """
    logger.info("-" * 50)
    logger.info("CONFIGURACIÓN DEL PIPELINE")
    logger.info("-" * 50)
    logger.info(f"  Rango de fechas: {config.dates.start_date} - {config.dates.end_date}")
    logger.info(f"  Filtro de país: {config.filter.country or 'Todos'}")
    logger.info(f"  Archivo de entrada: {config.paths.input_file}")
    logger.info(f"  Directorio de salida: {config.paths.output_path}")
    logger.info(f"  Formato de salida: {config.output.format}")
    logger.info(f"  Conversión CS: 1 caja = {config.units.cs_to_units} unidades")
    logger.info("-" * 50)


def main() -> None:
    """
    Función principal que ejecuta el pipeline ETL.
    """
    # Parsear argumentos
    args = parse_arguments()
    
    # Configurar logging
    logger = setup_logging("INFO")
    
    try:
        # Cargar configuración
        logger.info(f"Cargando configuración desde: {args.config}")
        config = load_config(args.config, args)
        
        # Validar configuración
        validate_config(config)
        
        # Mostrar resumen de configuración
        print_config_summary(config, logger)
        
        # Crear y ejecutar pipeline
        pipeline = ETLPipeline(config)
        result_df = pipeline.run()
        
        # Mostrar resumen de resultados
        logger.info(f"Pipeline completado. Registros procesados: {result_df.count()}")
        
    except FileNotFoundError as e:
        logger.error(f"Error de archivo: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Error de configuración: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
        raise


if __name__ == "__main__":
    main()

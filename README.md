# Pipeline ETL - Grupo Mariposa 🦋

**Autor:** Giancarlos Cardenas Galarza  
**Fecha:** Diciembre 2025  
**Prueba Técnica:** Flujo de datos con PySpark y OmegaConf

---

Pipeline de procesamiento de datos para entregas de productos usando **PySpark** y **OmegaConf**.

## ✅ Requisitos Cumplidos

| # | Requisito | Implementación |
|---|-----------|----------------|
| 1 | Leer archivo CSV | `read_csv()` en `run_etl.py` |
| 2 | Filtrar por rango de fechas (start_date, end_date) | `filter_by_date_range()` con parámetros CLI |
| 3 | Salidas particionadas: `data/processed/${fecha_proceso}` | `write_partitioned_output()` con `partitionBy` |
| 4 | OmegaConf para todos los parámetros desde YAML | `config/config.yaml` |
| 5 | Parametrizable por país | `--country GT` en CLI |
| 6 | Conversión de unidades: CS×20=unidades | `convert_units()` |
| 7 | Columnas por tipo de entrega (rutina/bonificación) | `add_delivery_type_columns()` |
| 8 | Nombres de columnas estandarizados | `standardize_column_names()` |
| 9 | Detectar/eliminar anomalías | `remove_anomalies()` |
| 10 | Columnas adicionales con fundamento | `add_extra_columns()` |
| 11 | Documentación gráfica y descriptiva | `docs/flujo_datos.md` |

## 📁 Estructura del Proyecto

```
grupo_mariposa_etl/
├── config/
│   └── config.yaml              # Configuración OmegaConf (REQ 4)
├── src/
│   ├── __init__.py
│   ├── main.py                  # Punto de entrada alternativo
│   ├── etl_pipeline.py          # Pipeline orientado a objetos
│   ├── transformations.py       # Funciones de transformación
│   └── utils.py                 # Utilidades
├── data/
│   ├── input/
│   │   └── global_mobility_data_entrega_productos.csv
│   └── processed/               # Salida particionada (REQ 3)
│       ├── fecha_proceso=20250114/
│       ├── fecha_proceso=20250217/
│       ├── fecha_proceso=20250314/
│       ├── fecha_proceso=20250325/
│       ├── fecha_proceso=20250513/
│       └── fecha_proceso=20250602/
├── tests/
│   └── test_etl.py
├── docs/
│   └── flujo_datos.md           # Documentación gráfica (REQ 11)
├── run_etl.py                   # Script principal
├── requirements.txt
└── README.md
```

## 🚀 Instalación

```bash
# Requisitos previos: Python 3.8+, Java 8 o 11

# Instalar dependencias
pip install -r requirements.txt

# En Windows, descargar winutils.exe para Hadoop:
# https://github.com/cdarlint/winutils
```

## ⚙️ Configuración (config.yaml)

```yaml
# Rango de fechas (REQUISITO 2)
dates:
  start_date: 20250101
  end_date: 20250630

# Filtro de país (REQUISITO 5)
filter:
  country: null  # GT, PE, EC, SV, HN, JM o null para todos

# Conversión de unidades (REQUISITO 6)
units:
  cs_to_units: 20  # 1 caja = 20 unidades

# Tipos de entrega (REQUISITO 7)
delivery_types:
  routine: ["ZPRE", "ZVE1"]   # Entregas de rutina
  bonus: ["Z04", "Z05"]       # Entregas con bonificación
```

## 📖 Uso

### Ejecución Básica
```bash
cd grupo_mariposa_etl
python run_etl.py
```

### Con Parámetros
```bash
# Rango de fechas específico
python run_etl.py --start_date 20250101 --end_date 20250331

# Filtrar por país
python run_etl.py --country GT

# Combinación de filtros
python run_etl.py --start_date 20250101 --end_date 20250630 --country EC
```

### Todos los Parámetros CLI
| Parámetro | Descripción | Ejemplo |
|-----------|-------------|---------|
| `--config` | Ruta al YAML | `--config config/config.yaml` |
| `--start_date` | Fecha inicio (YYYYMMDD) | `--start_date 20250101` |
| `--end_date` | Fecha fin (YYYYMMDD) | `--end_date 20250630` |
| `--country` | Código de país | `--country GT` |

## 🔄 Transformaciones

### REQUISITO 6: Conversión de Unidades
```
CS (cajas) × 20 = unidades
ST (stock)  × 1 = unidades
```

### REQUISITO 7: Tipos de Entrega
| Tipo | Categoría | Columna |
|------|-----------|---------|
| ZPRE, ZVE1 | RUTINA | `es_entrega_rutina = True` |
| Z04, Z05 | BONIFICACION | `es_entrega_bonificacion = True` |
| COBR, otros | - | Excluidos del output |

### REQUISITO 8: Nombres de Columnas Estandarizados
| Original | Estándar |
|----------|----------|
| pais | codigo_pais |
| material | codigo_material |
| precio | precio_original |
| cantidad | cantidad_original |
| unidad | unidad_original |

### REQUISITO 10: Columnas Adicionales
| Columna | Fundamento |
|---------|------------|
| `precio_unitario` | Precio por unidad individual (comparabilidad) |
| `categoria_producto` | Clasificación por tipo de bebida (análisis) |
| `nombre_pais` | Nombre completo (legibilidad) |
| `fecha_formateada` | Tipo Date (ordenamiento) |
| `es_promocion_gratis` | Productos sin costo (análisis promociones) |

## 📊 Salida

Los datos se guardan en formato Parquet particionados por fecha:

```
data/processed/
├── fecha_proceso=20250114/
│   └── part-00000.parquet
├── fecha_proceso=20250217/
│   └── part-00000.parquet
├── fecha_proceso=20250314/
│   └── part-00000.parquet
├── fecha_proceso=20250325/
│   └── part-00000.parquet
├── fecha_proceso=20250513/
│   └── part-00000.parquet
└── fecha_proceso=20250602/
    └── part-00000.parquet
```

### Columnas del Dataset Final

```
1. codigo_pais            # Código ISO del país
2. nombre_pais            # Nombre completo
3. fecha_proceso          # Fecha original (YYYYMMDD)
4. fecha_formateada       # Fecha como tipo Date
5. id_transporte          # ID del transporte
6. id_ruta                # ID de la ruta
7. codigo_material        # Código del producto
8. categoria_producto     # Categoría del producto
9. tipo_entrega           # Código original
10. categoria_entrega     # RUTINA o BONIFICACION
11. es_entrega_rutina     # True si ZPRE/ZVE1
12. es_entrega_bonificacion # True si Z04/Z05
13. cantidad_original     # Cantidad original
14. unidad_original       # CS o ST
15. cantidad_unidades     # Cantidad normalizada
16. unidad_estandar       # Siempre "UNIDADES"
17. precio_original       # Precio de la línea
18. precio_unitario       # Precio por unidad
19. valor_total           # Valor monetario total
20. es_promocion_gratis   # True si precio = 0
```

## 🧪 Tests

```bash
python -m pytest tests/ -v
```

## 📝 Documentación Adicional

Ver `docs/flujo_datos.md` para documentación gráfica del flujo de datos (REQUISITO 11).

## 👨‍💻 Autor

**Giancarlos Cardenas Galarza**

Desarrollado como prueba técnica para **Grupo Mariposa** - Diciembre 2025

---

*Este proyecto fue desarrollado completamente por Giancarlos Cardenas Galarza como parte del proceso de evaluación técnica.*

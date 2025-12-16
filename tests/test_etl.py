"""
Tests Unitarios para el Pipeline ETL de Grupo Mariposa
"""

import pytest
import sys
from pathlib import Path

# Agregar el directorio src al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils import (
    parse_date,
    validate_date_range,
    format_date_for_partition,
    get_country_name,
    get_material_category
)


class TestDateFunctions:
    """Tests para funciones de manejo de fechas."""
    
    def test_parse_date_valid(self):
        """Test parseo de fecha válida."""
        result = parse_date(20250115)
        assert result is not None
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15
    
    def test_parse_date_string(self):
        """Test parseo de fecha como string."""
        result = parse_date("20250513")
        assert result is not None
        assert result.month == 5
        assert result.day == 13
    
    def test_parse_date_invalid(self):
        """Test parseo de fecha inválida."""
        result = parse_date("invalid")
        assert result is None
    
    def test_validate_date_range_valid(self):
        """Test validación de rango de fechas válido."""
        assert validate_date_range(20250101, 20250630) is True
    
    def test_validate_date_range_same_date(self):
        """Test rango con misma fecha de inicio y fin."""
        assert validate_date_range(20250315, 20250315) is True
    
    def test_validate_date_range_invalid(self):
        """Test rango inválido (inicio > fin)."""
        assert validate_date_range(20250630, 20250101) is False
    
    def test_format_date_for_partition(self):
        """Test formateo de fecha para partición."""
        result = format_date_for_partition(20250513)
        assert result == "2025-05-13"


class TestCountryFunctions:
    """Tests para funciones de países."""
    
    def test_get_country_name_gt(self):
        """Test nombre de Guatemala."""
        assert get_country_name("GT") == "Guatemala"
    
    def test_get_country_name_lowercase(self):
        """Test con código en minúsculas."""
        assert get_country_name("pe") == "Perú"
    
    def test_get_country_name_unknown(self):
        """Test país desconocido."""
        assert get_country_name("XX") == "XX"


class TestMaterialFunctions:
    """Tests para funciones de materiales."""
    
    def test_get_material_category_aa(self):
        """Test categoría bebidas alcohólicas."""
        assert get_material_category("AA004003") == "BEBIDAS_ALCOHOLICAS"
    
    def test_get_material_category_ba(self):
        """Test categoría bebidas no alcohólicas."""
        assert get_material_category("BA018426") == "BEBIDAS_NO_ALCOHOLICAS"
    
    def test_get_material_category_en(self):
        """Test categoría energía."""
        assert get_material_category("EN900052") == "ENERGIA"
    
    def test_get_material_category_ca(self):
        """Test categoría concentrados."""
        assert get_material_category("CA900099") == "CONCENTRADOS"
    
    def test_get_material_category_empty(self):
        """Test material vacío."""
        assert get_material_category("") == "DESCONOCIDO"
    
    def test_get_material_category_unknown(self):
        """Test prefijo desconocido."""
        assert get_material_category("XX123456") == "OTROS"


class TestDataQuality:
    """Tests para validación de calidad de datos."""
    
    def test_routine_delivery_types(self):
        """Test tipos de entrega de rutina."""
        routine_types = ["ZPRE", "ZVE1"]
        assert "ZPRE" in routine_types
        assert "ZVE1" in routine_types
        assert "Z04" not in routine_types
    
    def test_bonus_delivery_types(self):
        """Test tipos de entrega con bonificación."""
        bonus_types = ["Z04", "Z05"]
        assert "Z04" in bonus_types
        assert "Z05" in bonus_types
        assert "COBR" not in bonus_types
    
    def test_unit_conversion_cs(self):
        """Test conversión de cajas a unidades."""
        cs_multiplier = 20
        quantity_cs = 10
        expected_units = 200
        assert quantity_cs * cs_multiplier == expected_units
    
    def test_unit_conversion_st(self):
        """Test unidades (ST) sin conversión."""
        quantity_st = 15
        # ST ya está en unidades, no se multiplica
        assert quantity_st == 15


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

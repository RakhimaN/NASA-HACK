"""
Тесты утилит
"""
import pytest
from datetime import datetime, date
from weather_analysis.utils import (
    date_to_day_of_year,
    format_probability,
    get_probability_description,
    categorize_probability
)


class TestDateUtils:
    """Тесты работы с датами"""
    
    def test_date_to_doy_string(self):
        """Строка в день года"""
        assert date_to_day_of_year('2024-01-01') == 1
        assert date_to_day_of_year('2024-12-31') == 366  # Високосный
    
    def test_date_to_doy_june(self):
        """Июнь"""
        doy = date_to_day_of_year('2024-06-15')
        assert 160 <= doy <= 167
    
    def test_date_to_doy_leap_year(self):
        """29 февраля"""
        assert date_to_day_of_year('2024-02-29') == 60
    
    def test_date_to_doy_non_leap_year(self):
        """Невисокосный год"""
        assert date_to_day_of_year('2023-12-31') == 365


class TestProbabilityFormatting:
    """Тесты форматирования вероятностей"""
    
    def test_format_zero(self):
        """Ноль"""
        result = format_probability(0)
        assert '0' in result
        assert '%' in result
    
    def test_format_one(self):
        """Единица (100%)"""
        result = format_probability(1.0)
        assert '100' in result
        assert '%' in result
    
    def test_format_half(self):
        """Половина (50%)"""
        result = format_probability(0.5)
        assert '50' in result
        assert '%' in result
    
    def test_format_decimal(self):
        """Дробное значение"""
        result = format_probability(0.123)
        assert isinstance(result, str)
        assert '%' in result


class TestProbabilityCategorization:
    """Тесты категоризации вероятностей"""
    
    def test_categorize_very_low(self):
        """Очень низкая"""
        assert categorize_probability(0.05) == 'very_low'
    
    def test_categorize_low(self):
        """Низкая"""
        assert categorize_probability(0.20) == 'low'
    
    def test_categorize_moderate(self):
        """Средняя"""
        assert categorize_probability(0.40) == 'moderate'
    
    def test_categorize_high(self):
        """Высокая"""
        assert categorize_probability(0.60) == 'high'
    
    def test_categorize_very_high(self):
        """Очень высокая"""
        assert categorize_probability(0.90) == 'very_high'


class TestProbabilityDescription:
    """Тесты описаний вероятностей"""
    
    def test_description_returns_string(self):
        """Возвращает строку"""
        desc = get_probability_description(0.5, 'rain')
        assert isinstance(desc, str)
        assert len(desc) > 0
    
    def test_description_has_percentage(self):
        """Содержит процент"""
        desc = get_probability_description(0.75, 'hot')
        assert '%' in desc
    
    def test_description_low_probability(self):
        """Описание низкой вероятности"""
        desc = get_probability_description(0.05, 'snow')
        assert isinstance(desc, str)
    
    def test_description_high_probability(self):
        """Описание высокой вероятности"""
        desc = get_probability_description(0.95, 'rain')
        assert isinstance(desc, str)


class TestUtilsImport:
    """Тесты импорта модулей"""
    
    def test_can_import_config(self):
        """Можно импортировать config"""
        from weather_analysis.config import WeatherConfig
        config = WeatherConfig()
        assert config is not None
    
    def test_can_import_data_service(self):
        """Можно импортировать data_service"""
        from weather_analysis.data_service import WeatherDataService
        service = WeatherDataService()
        assert service is not None
    
    def test_can_import_statistical_analyzer(self):
        """Можно импортировать statistical_analyzer"""
        from weather_analysis.statistical_analyzer import StatisticalAnalyzer
        analyzer = StatisticalAnalyzer()
        assert analyzer is not None
    
    def test_can_import_multi_source_service(self):
        """Можно импортировать multi_source_service"""
        from weather_analysis.multi_source_service import MultiSourceDataService
        service = MultiSourceDataService()
        assert service is not None


class TestConfig:
    """Тесты конфигурации"""
    
    def test_config_has_temperature_thresholds(self):
        """Есть температурные пороги"""
        from weather_analysis.config import WeatherConfig
        config = WeatherConfig()
        
        assert hasattr(config, 'TEMPERATURE_THRESHOLDS')
    
    def test_config_has_precipitation_thresholds(self):
        """Есть пороги осадков"""
        from weather_analysis.config import WeatherConfig
        config = WeatherConfig()
        
        assert hasattr(config, 'PRECIPITATION_THRESHOLDS')
    
    def test_config_has_wind_thresholds(self):
        """Есть пороги ветра"""
        from weather_analysis.config import WeatherConfig
        config = WeatherConfig()
        
        assert hasattr(config, 'WIND_THRESHOLDS')

"""
Тесты сервиса данных WeatherDataService
"""
import pytest
from weather_analysis.data_service import WeatherDataService


class TestWeatherDataServiceInit:
    """Тесты инициализации"""
    
    def test_init_default(self):
        """Инициализация по умолчанию"""
        service = WeatherDataService()
        assert service is not None
    
    def test_init_nasa(self):
        """Инициализация с NASA"""
        service = WeatherDataService(preferred_source='nasa')
        assert service is not None
    
    def test_init_openmeteo(self):
        """Инициализация с Open-Meteo"""
        service = WeatherDataService(preferred_source='openmeteo')
        assert service is not None


class TestWeatherDataServiceGetData:
    """Тесты получения данных"""
    
    def test_get_data_returns_tuple(self):
        """get_data возвращает кортеж"""
        service = WeatherDataService()
        data, source = service.get_data(55.7558, 37.6173, 2020, 2023)
        
        assert data is not None
        assert source is not None
        assert isinstance(source, str)
    
    def test_get_data_nasa(self):
        """Получение данных из NASA"""
        service = WeatherDataService(preferred_source='nasa')
        data, source = service.get_data(55.7558, 37.6173, 2020, 2023)
        
        assert data is not None
        assert 'NASA' in source
        assert len(data) > 0
    
    def test_get_data_openmeteo(self):
        """Получение данных из Open-Meteo"""
        service = WeatherDataService(preferred_source='openmeteo')
        data, source = service.get_data(51.5074, -0.1278, 2020, 2023)
        
        assert data is not None
        assert source is not None  # Источник может быть NASA или Open-Meteo (зависит от доступности)
        assert len(data) > 0
    
    def test_get_data_multiple_years(self):
        """Получение данных за несколько лет"""
        service = WeatherDataService()
        data, _ = service.get_data(55.7558, 37.6173, 2020, 2023)
        
        assert data is not None
        assert len(data) > 365  # Минимум год данных


class TestWeatherDataServiceLocations:
    """Тесты разных локаций"""
    
    def test_moscow(self):
        """Москва"""
        service = WeatherDataService()
        data, source = service.get_data(55.7558, 37.6173, 2022, 2023)
        
        assert data is not None
        assert len(data) > 0
    
    def test_new_york(self):
        """Нью-Йорк"""
        service = WeatherDataService()
        data, source = service.get_data(40.7128, -74.0060, 2022, 2023)
        
        assert data is not None
        assert len(data) > 0
    
    def test_tokyo(self):
        """Токио"""
        service = WeatherDataService()
        data, source = service.get_data(35.6762, 139.6503, 2022, 2023)
        
        assert data is not None
        assert len(data) > 0
    
    def test_negative_longitude(self):
        """Отрицательная долгота"""
        service = WeatherDataService()
        data, source = service.get_data(51.5074, -0.1278, 2022, 2023)
        
        assert data is not None
    
    def test_southern_hemisphere(self):
        """Южное полушарие"""
        service = WeatherDataService()
        data, source = service.get_data(-33.8688, 151.2093, 2022, 2023)
        
        assert data is not None


class TestWeatherDataServiceYearRanges:
    """Тесты разных диапазонов лет"""
    
    def test_single_year(self):
        """Один год"""
        service = WeatherDataService()
        data, _ = service.get_data(55.7558, 37.6173, 2023, 2023)
        
        assert data is not None
        assert len(data) >= 365
    
    def test_two_years(self):
        """Два года"""
        service = WeatherDataService()
        data, _ = service.get_data(55.7558, 37.6173, 2022, 2023)
        
        assert data is not None
        assert len(data) >= 365 * 2
    
    def test_five_years(self):
        """Пять лет"""
        service = WeatherDataService()
        data, _ = service.get_data(55.7558, 37.6173, 2019, 2023)
        
        assert data is not None
        assert len(data) >= 365 * 5


class TestWeatherDataServiceDataFormat:
    """Тесты формата данных"""
    
    def test_dat-is_dataframe(self):
        """Данные это DataFrame"""
        import pandas as pd
        
        service = WeatherDataService()
        data, _ = service.get_data(55.7558, 37.6173, 2022, 2023)
        
        assert isinstance(data, pd.DataFrame)
    
    def test_has_day_of_year_column(self):
        """Есть колонка day_of_year"""
        service = WeatherDataService()
        data, _ = service.get_data(55.7558, 37.6173, 2022, 2023)
        
        assert 'day_of_year' in data.columns
    
    def test_day_of_year_range(self):
        """day_of_year в правильном диапазоне"""
        service = WeatherDataService()
        data, _ = service.get_data(55.7558, 37.6173, 2022, 2023)
        
        assert data['day_of_year'].min() >= 1
        assert data['day_of_year'].max() <= 366
    
    def test_has_temperature_data(self):
        """Есть температурные данные"""
        service = WeatherDataService()
        data, _ = service.get_data(55.7558, 37.6173, 2022, 2023)
        
        # Хотя бы одна температурная колонка должна быть
        temp_columns = [col for col in data.columns if 'T2M' in col or 'temperature' in col.lower()]
        assert len(temp_columns) > 0


class TestWeatherDataServiceCaching:
    """Тесты кэширования"""
    
    def test_repeated_calls_work(self):
        """Повторные вызовы работают"""
        service = WeatherDataService()
        
        data1, source1 = service.get_data(55.7558, 37.6173, 2022, 2023)
        data2, source2 = service.get_data(55.7558, 37.6173, 2022, 2023)
        
        assert data1 is not None
        assert data2 is not None
        assert source1 == source2
        assert len(data1) == len(data2)

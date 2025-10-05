"""
Тесты основной функции analyze_weather
"""
import pytest
from weather_analysis import analyze_weather


class TestAnalyzeWeatherBasic:
    """Базовые тесты analyze_weather()"""
    
    def test_returns_dict(self):
        """Функция возвращает словарь"""
        result = analyze_weather(55.7558, 37.6173, '2024-06-15')
        assert isinstance(result, dict)
    
    def test_has_required_keys(self):
        """Результат содержит все обязательные ключи"""
        result = analyze_weather(55.7558, 37.6173, '2024-06-15')
        
        required_keys = ['location', 'date', 'day_of_year', 'probabilities', 
                        'statistics', 'metadata']
        for key in required_keys:
            assert key in result, f"Отсутствует ключ: {key}"
    
    def test_location_structure(self):
        """Проверка структуры location"""
        result = analyze_weather(55.7558, 37.6173, '2024-06-15')
        
        assert 'latitude' in result['location']
        assert 'longitude' in result['location']
        assert result['location']['latitude'] == 55.7558
        assert result['location']['longitude'] == 37.6173
    
    def test_date_info(self):
        """Проверка информации о дате"""
        result = analyze_weather(55.7558, 37.6173, '2024-06-15')
        
        assert result['date'] == '2024-06-15'
        assert isinstance(result['day_of_year'], int)
        assert 1 <= result['day_of_year'] <= 366
    
    def test_probabilities_exist(self):
        """Вероятности присутствуют"""
        result = analyze_weather(55.7558, 37.6173, '2024-06-15')
        
        assert 'probabilities' in result
        assert isinstance(result['probabilities'], dict)
        assert len(result['probabilities']) > 0
    
    def test_probabilities_in_range(self):
        """Все вероятности в диапазоне 0-1"""
        result = analyze_weather(55.7558, 37.6173, '2024-06-15')
        
        for key, value in result['probabilities'].items():
            assert 0 <= value <= 1, f"{key} = {value} вне диапазона [0, 1]"
    
    def test_statistics_exist(self):
        """Статистика присутствует"""
        result = analyze_weather(55.7558, 37.6173, '2024-06-15')
        
        assert 'statistics' in result
        assert isinstance(result['statistics'], dict)
    
    def test_metadata_structure(self):
        """Проверка структуры метаданных"""
        result = analyze_weather(55.7558, 37.6173, '2024-06-15')
        
        meta = result['metadata']
        assert 'data_source' in meta
        assert 'years_analyzed' in meta
        assert 'data_points' in meta


class TestAnalyzeWeatherLocations:
    """Тесты разных локаций"""
    
    def test_moscow(self):
        """Москва"""
        result = analyze_weather(55.7558, 37.6173, '2024-06-15')
        assert result is not None
        assert 'probabilities' in result
    
    def test_new_york(self):
        """Нью-Йорк"""
        result = analyze_weather(40.7128, -74.0060, '2024-07-04')
        assert result is not None
        assert 'probabilities' in result
    
    def test_tokyo(self):
        """Токио"""
        result = analyze_weather(35.6762, 139.6503, '2024-03-21')
        assert result is not None
        assert 'probabilities' in result
    
    def test_london(self):
        """Лондон"""
        result = analyze_weather(51.5074, -0.1278, '2024-12-25')
        assert result is not None
        assert 'probabilities' in result
    
    def test_sydney(self):
        """Сидней (южное полушарие)"""
        result = analyze_weather(-33.8688, 151.2093, '2024-12-25')
        assert result is not None
        assert 'probabilities' in result


class TestAnalyzeWeatherDates:
    """Тесты разных дат"""
    
    def test_winter(self):
        """Зимняя дата"""
        result = analyze_weather(55.7558, 37.6173, '2024-01-15')
        assert result is not None
        assert result['day_of_year'] == 15
    
    def test_spring(self):
        """Весенняя дата"""
        result = analyze_weather(55.7558, 37.6173, '2024-04-15')
        assert result is not None
    
    def test_summer(self):
        """Летняя дата"""
        result = analyze_weather(55.7558, 37.6173, '2024-07-15')
        assert result is not None
    
    def test_fall(self):
        """Осенняя дата"""
        result = analyze_weather(55.7558, 37.6173, '2024-10-15')
        assert result is not None
    
    def test_leap_year(self):
        """29 февраля високосного года"""
        result = analyze_weather(55.7558, 37.6173, '2024-02-29')
        assert result is not None
        assert result['day_of_year'] == 60
    
    def test_new_year(self):
        """Новый год"""
        result = analyze_weather(55.7558, 37.6173, '2024-01-01')
        assert result is not None
        assert result['day_of_year'] == 1
    
    def test_end_of_year(self):
        """Конец года"""
        result = analyze_weather(55.7558, 37.6173, '2024-12-31')
        assert result is not None
        assert result['day_of_year'] == 366  # 2024 високосный


class TestAnalyzeWeatherDataSources:
    """Тесты разных источников данных"""
    
    def test_nasa_source(self):
        """NASA POWER API"""
        result = analyze_weather(55.7558, 37.6173, '2024-06-15', data_source='nasa')
        assert result is not None
        assert 'NASA' in result['metadata']['data_source']
    
    def test_openmeteo_source(self):
        """Open-Meteo API"""
        result = analyze_weather(51.5074, -0.1278, '2024-06-15', data_source='openmeteo')
        assert result is not None
        assert 'Open-Meteo' in result['metadata']['data_source']


class TestAnalyzeWeatherValidation:
    """Тесты валидации входных данных"""
    
    def test_invalid_latitude_high(self):
        """Слишком большая широта"""
        with pytest.raises(ValueError):
            analyze_weather(91.0, 37.6173, '2024-06-15')
    
    def test_invalid_latitude_low(self):
        """Слишком маленькая широта"""
        with pytest.raises(ValueError):
            analyze_weather(-91.0, 37.6173, '2024-06-15')
    
    def test_invalid_longitude_high(self):
        """Слишком большая долгота"""
        with pytest.raises(ValueError):
            analyze_weather(55.7558, 181.0, '2024-06-15')
    
    def test_invalid_longitude_low(self):
        """Слишком маленькая долгота"""
        with pytest.raises(ValueError):
            analyze_weather(55.7558, -181.0, '2024-06-15')
    
    def test_invalid_date_format(self):
        """Неправильный формат даты"""
        with pytest.raises(ValueError):
            analyze_weather(55.7558, 37.6173, '15-06-2024')


class TestAnalyzeWeatherProbabilities:
    """Тесты вероятностей"""
    
    def test_has_temperature_probabilities(self):
        """Есть температурные вероятности"""
        result = analyze_weather(55.7558, 37.6173, '2024-06-15')
        probs = result['probabilities']
        
        temp_keys = ['very_cold', 'cold', 'comfortable', 'hot', 'very_hot']
        for key in temp_keys:
            if key in probs:
                assert isinstance(probs[key], (int, float))
    
    def test_moscow_summer_not_very_cold(self):
        """Летом в Москве маловероятно очень холодно"""
        result = analyze_weather(55.7558, 37.6173, '2024-07-15')
        
        if 'very_cold' in result['probabilities']:
            # Летом вероятность очень холодной погоды должна быть низкой
            assert result['probabilities']['very_cold'] < 0.3
    
    def test_moscow_winter_not_very_hot(self):
        """Зимой в Москве маловероятно очень жарко"""
        result = analyze_weather(55.7558, 37.6173, '2024-01-15')
        
        if 'very_hot' in result['probabilities']:
            # Зимой вероятность очень жаркой погоды должна быть низкой
            assert result['probabilities']['very_hot'] < 0.3


class TestAnalyzeWeatherStatistics:
    """Тесты статистики"""
    
    def test_has_temperature_stats(self):
        """Есть температурная статистика"""
        result = analyze_weather(55.7558, 37.6173, '2024-06-15')
        stats = result['statistics']
        
        if 'temperature' in stats:
            temp_stats = stats['temperature']
            assert 'mean' in temp_stats
            assert isinstance(temp_stats['mean'], (int, float))
    
    def test_temperature_reasonable(self):
        """Температура в разумных пределах"""
        result = analyze_weather(55.7558, 37.6173, '2024-06-15')
        
        if 'temperature' in result['statistics']:
            mean_temp = result['statistics']['temperature']['mean']
            # Для Москвы в июне температура должна быть примерно от -10 до +40
            assert -20 < mean_temp < 50


class TestAnalyzeWeatherCaching:
    """Тесты кэширования"""
    
    def test_repeated_calls_work(self):
        """Повторные вызовы работают"""
        result1 = analyze_weather(55.7558, 37.6173, '2024-06-15')
        result2 = analyze_weather(55.7558, 37.6173, '2024-06-15')
        
        assert result1 is not None
        assert result2 is not None
        
        # Результаты должны быть идентичны
        if 'temperature' in result1['statistics']:
            assert result1['statistics']['temperature']['mean'] == \
                   result2['statistics']['temperature']['mean']

"""
Тесты функции analyze_weather_range
"""
import pytest
from weather_analysis import analyze_weather_range


class TestAnalyzeWeatherRangeBasic:
    """Базовые тесты analyze_weather_range()"""
    
    def test_returns_dict(self):
        """Функция возвращает словарь"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-05', '2025-10-10')
        assert isinstance(result, dict)
    
    def test_has_required_keys(self):
        """Результат содержит обязательные ключи"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-05', '2025-10-10')
        
        required_keys = ['location', 'date_range', 'aggregated', 'daily_breakdown', 'metadata']
        for key in required_keys:
            assert key in result, f"Отсутствует ключ: {key}"
    
    def test_location_structure(self):
        """Структура location"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-05', '2025-10-10')
        
        assert 'latitude' in result['location']
        assert 'longitude' in result['location']
    
    def test_date_range_structure(self):
        """Структура date_range"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-05', '2025-10-10')
        
        assert 'start' in result['date_range']
        assert 'end' in result['date_range']
        assert 'duration_days' in result['date_range']
    
    def test_aggregated_data_exists(self):
        """Агрегированные данные присутствуют"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-05', '2025-10-10')
        
        assert 'aggregated' in result
        assert isinstance(result['aggregated'], dict)
    
    def test_daily_breakdown_exists(self):
        """Разбивка по дням присутствует"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-05', '2025-10-10')
        
        assert 'daily_breakdown' in result
        assert isinstance(result['daily_breakdown'], list)
        assert len(result['daily_breakdown']) == 6  # 5-10 октября = 6 дней
    
    def test_metadata_exists(self):
        """Метаданные присутствуют"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-05', '2025-10-10')
        
        assert 'metadata' in result
        assert isinstance(result['metadata'], dict)


class TestAnalyzeWeatherRangeDurations:
    """Тесты разных продолжительностей"""
    
    def test_two_days(self):
        """Диапазон 2 дня"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-05', '2025-10-06')
        
        assert result['date_range']['duration_days'] == 2
        assert len(result['daily_breakdown']) == 2
    
    def test_one_week(self):
        """Диапазон неделя"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-01', '2025-10-07')
        
        assert result['date_range']['duration_days'] == 7
        assert len(result['daily_breakdown']) == 7
    
    def test_two_weeks(self):
        """Диапазон 2 недели"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-01', '2025-10-14')
        
        assert result['date_range']['duration_days'] == 14
        assert len(result['daily_breakdown']) == 14
    
    def test_one_month(self):
        """Диапазон месяц"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-01', '2025-10-31')
        
        assert result['date_range']['duration_days'] == 31
        assert len(result['daily_breakdown']) == 31


class TestAnalyzeWeatherRangeLocations:
    """Тесты разных локаций"""
    
    def test_moscow(self):
        """Москва"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-05', '2025-10-10')
        assert result is not None
        assert result['location']['latitude'] == 55.7558
    
    def test_new_york(self):
        """Нью-Йорк"""
        result = analyze_weather_range(40.7128, -74.0060, '2025-07-01', '2025-07-07')
        assert result is not None
    
    def test_tokyo(self):
        """Токио"""
        result = analyze_weather_range(35.6762, 139.6503, '2025-03-20', '2025-03-26')
        assert result is not None
    
    def test_sochi(self):
        """Сочи (курорт)"""
        result = analyze_weather_range(43.6028, 39.7342, '2025-07-01', '2025-07-14')
        assert result is not None


class TestAnalyzeWeatherRangeValidation:
    """Тесты валидации"""
    
    def test_start_after_end_fails(self):
        """Начало после конца - ошибка"""
        with pytest.raises(ValueError):
            analyze_weather_range(55.7558, 37.6173, '2025-10-10', '2025-10-05')
    
    def test_invalid_latitude(self):
        """Неправильная широта"""
        with pytest.raises(ValueError):
            analyze_weather_range(100.0, 37.6173, '2025-10-05', '2025-10-10')
    
    def test_invalid_longitude(self):
        """Неправильная долгота"""
        with pytest.raises(ValueError):
            analyze_weather_range(55.7558, 200.0, '2025-10-05', '2025-10-10')


class TestAnalyzeWeatherRangeAggregation:
    """Тесты агрегации данных"""
    
    def test_has_statistics(self):
        """Есть статистика"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-05', '2025-10-10')
        
        assert 'statistics' in result['aggregated']
    
    def test_has_probabilities(self):
        """Есть вероятности"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-05', '2025-10-10')
        
        assert 'probabilities' in result['aggregated']
    
    def test_has_best_days(self):
        """Есть рекомендации лучших дней"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-05', '2025-10-10')
        
        assert 'best_days' in result['aggregated']
        best_days = result['aggregated']['best_days']
        
        # Должны быть рекомендации
        assert 'warmest' in best_days or 'outdoor_activity' in best_days


class TestAnalyzeWeatherRangeDailyBreakdown:
    """Тесты разбивки по дням"""
    
    def test_each_day_has_date(self):
        """Каждый день имеет дату"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-05', '2025-10-10')
        
        for day in result['daily_breakdown']:
            assert 'date' in day
            assert isinstance(day['date'], str)
    
    def test_each_day_has_day_of_year(self):
        """Каждый день имеет день года"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-05', '2025-10-10')
        
        for day in result['daily_breakdown']:
            assert 'day_of_year' in day
            assert 1 <= day['day_of_year'] <= 366
    
    def test_days_are_sequential(self):
        """Дни идут последовательно"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-05', '2025-10-10')
        
        days = result['daily_breakdown']
        for i in range(len(days) - 1):
            # День года должен увеличиваться на 1
            assert days[i+1]['day_of_year'] == days[i]['day_of_year'] + 1


class TestAnalyzeWeatherRangeSpecialCases:
    """Тесты особых случаев"""
    
    def test_month_boundary(self):
        """Границы месяцев"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-09-28', '2025-10-03')
        assert result is not None
        assert result['date_range']['duration_days'] == 6
    
    def test_year_boundary(self):
        """Граница года"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-12-29', '2026-01-02')
        assert result is not None
        assert result['date_range']['duration_days'] == 5
    
    def test_leap_year_feb(self):
        """Февраль високосного года"""
        result = analyze_weather_range(55.7558, 37.6173, '2024-02-27', '2024-03-02')
        assert result is not None
        # Должен включать 29 февраля
        assert result['date_range']['duration_days'] == 5


class TestAnalyzeWeatherRangeDataSources:
    """Тесты источников данных"""
    
    def test_nasa_source(self):
        """NASA источник"""
        result = analyze_weather_range(
            55.7558, 37.6173, 
            '2025-10-05', '2025-10-10',
            data_source='nasa'
        )
        assert result is not None
        assert 'NASA' in result['metadata']['data_source']
    
    def test_openmeteo_source(self):
        """Open-Meteo источник"""
        result = analyze_weather_range(
            51.5074, -0.1278,
            '2025-10-05', '2025-10-10',
            data_source='openmeteo'
        )
        assert result is not None
        assert 'Open-Meteo' in result['metadata']['data_source']


class TestAnalyzeWeatherRangeRealScenarios:
    """Тесты реальных сценариев"""
    
    def test_vacation_planning(self):
        """Планирование отпуска"""
        # 2 недели на море
        result = analyze_weather_range(43.6028, 39.7342, '2025-07-01', '2025-07-14')
        
        assert result is not None
        assert len(result['daily_breakdown']) == 14
        assert 'best_days' in result['aggregated']
    
    def test_weekend_trip(self):
        """Поездка на выходные"""
        result = analyze_weather_range(55.7558, 37.6173, '2025-10-11', '2025-10-12')
        
        assert result is not None
        assert len(result['daily_breakdown']) == 2
    
    def test_business_trip(self):
        """Командировка на неделю"""
        result = analyze_weather_range(59.9343, 30.3351, '2025-09-15', '2025-09-21')
        
        assert result is not None
        assert len(result['daily_breakdown']) == 7

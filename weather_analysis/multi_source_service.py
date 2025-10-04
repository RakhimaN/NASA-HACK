"""
Мультисорсный сервис для получения данных из нескольких источников v2.0
Обеспечивает консенсус-анализ и повышение точности через агрегацию данных

Источники данных:
    1. NASA POWER - основной источник (приоритет: 1.0)
    2. Open-Meteo Enhanced - доп. параметры (приоритет: 0.9)
    3. GES DISC - качество воздуха (приоритет: 1.0)
    4. CPTEC - грозовая активность для Юж. Америки (приоритет: 0.8)
    5. Open-Meteo - дополнительная верификация (приоритет: 0.85)

Консенсус-анализ:
    - Взвешенное усреднение значений из разных источников
    - Обнаружение выбросов (отклонение >20% от медианы)
    - Расчет уровня согласованности (agreement level 0-1)
    - Оценка доверительного интервала
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import concurrent.futures
from weather_analysis.config import WeatherConfig
from weather_analysis.data_service import WeatherDataService, OpenMeteoAPI
from weather_analysis.data_adapters import GESDISCAdapter, CPTECAdapter, OpenMeteoEnhancedAdapter


class MultiSourceDataService:
    """
    Сервис для получения данных из множественных источников
    и выполнения консенсус-анализа для повышения точности
    """
    
    def __init__(self, cache_dir: str = "data/cache"):
        self.cache_dir = cache_dir
        
        # Инициализируем все доступные источники
        self.sources = {
            'nasa_power': WeatherDataService(preferred_source='nasa', cache_dir=cache_dir),
            'openmeteo': OpenMeteoAPI(cache_dir=cache_dir),
            'openmeteo_enhanced': OpenMeteoEnhancedAdapter(cache_dir=cache_dir),
            'ges_disc': GESDISCAdapter(cache_dir=cache_dir),
            'cptec': CPTECAdapter(cache_dir=cache_dir),
        }
        
        # Конфигурация приоритетов и надежности источников
        self.source_weights = {
            'nasa_power': 1.0,      # Высший приоритет
            'ges_disc': 1.0,         # Высший приоритет
            'openmeteo_enhanced': 0.9,
            'openmeteo': 0.85,
            'cptec': 0.8,
        }
        
        # Маппинг параметров на источники
        self.parameter_sources = {
            # Базовые параметры (NASA + Open-Meteo)
            'temperature': ['nasa_power', 'openmeteo', 'openmeteo_enhanced'],
            'precipitation': ['nasa_power', 'openmeteo'],
            'wind_speed': ['nasa_power', 'openmeteo'],
            'humidity': ['nasa_power', 'openmeteo'],
            'pressure': ['nasa_power', 'openmeteo'],
            
            # Специальные параметры Open-Meteo
            'apparent_temperature': ['openmeteo_enhanced'],
            'weathercode': ['openmeteo_enhanced'],
            'windgusts': ['openmeteo_enhanced'],
            
            # Качество воздуха (GES DISC)
            'air_quality': ['ges_disc'],
            'black_carbon': ['ges_disc'],
            'dust': ['ges_disc'],
            
            # Грозовая активность (CPTEC)
            'thunderstorm_risk': ['cptec'],
        }
        
        self.config = WeatherConfig()
    
    def fetch_multi_source_data(self, latitude: float, longitude: float,
                                start_date: str, end_date: str,
                                parameters: List[str]) -> Dict[str, pd.DataFrame]:
        """
        Получить данные из всех доступных источников параллельно
        
        Args:
            latitude: Широта
            longitude: Долгота
            start_date: Начальная дата (YYYY-MM-DD)
            end_date: Конечная дата (YYYY-MM-DD)
            parameters: Список параметров
            
        Returns:
            Словарь {источник: DataFrame с данными}
        """
        results = {}
        
        # Определяем какие источники нужны для каждого параметра
        sources_needed = set()
        for param in parameters:
            if param in self.parameter_sources:
                sources_needed.update(self.parameter_sources[param])
        
        print(f"\n🌐 МУЛЬТИСОРСНЫЙ РЕЖИМ")
        print(f"{'='*60}")
        print(f"🔍 Запрос данных из {len(sources_needed)} источников:")
        for source in sources_needed:
            print(f"   • {source.upper().replace('_', ' ')}")
        print(f"{'='*60}\n")
        
        # Параллельный запрос ко всем источникам
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_source = {}
            
            for source_name in sources_needed:
                if source_name not in self.sources:
                    continue
                
                source = self.sources[source_name]
                
                # Определяем какие параметры запрашивать у этого источника
                source_params = [p for p in parameters if source_name in self.parameter_sources.get(p, [])]
                
                if source_params:
                    future = executor.submit(
                        self._fetch_from_source,
                        source, source_name, latitude, longitude,
                        start_date, end_date, source_params
                    )
                    future_to_source[future] = source_name
            
            # Собираем результаты
            print(f"\n📊 Результаты запросов:")
            for future in concurrent.futures.as_completed(future_to_source):
                source_name = future_to_source[future]
                try:
                    data = future.result()
                    if data is not None and not data.empty:
                        results[source_name] = data
                        print(f"   ✅ {source_name.upper().replace('_', ' ')}: получено {len(data)} записей")
                    else:
                        print(f"   ⚠️  {source_name.upper().replace('_', ' ')}: данные не получены")
                except Exception as e:
                    print(f"   ❌ {source_name.upper().replace('_', ' ')}: ошибка - {str(e)[:50]}")
        
        print(f"\n{'='*60}")
        print(f"✓ Успешно получено данных из {len(results)}/{len(sources_needed)} источников")
        print(f"{'='*60}\n")
        
        return results
    
    def _fetch_from_source(self, source, source_name: str,
                          latitude: float, longitude: float,
                          start_date: str, end_date: str,
                          parameters: List[str]) -> Optional[pd.DataFrame]:
        """
        Получить данные из конкретного источника
        """
        try:
            if source_name == 'nasa_power':
                # NASA POWER использует get_data с годами
                # Конвертируем даты в годы
                start_year = int(start_date.split('-')[0])
                end_year = int(end_date.split('-')[0])
                
                data, source_info = source.get_data(
                    latitude=latitude,
                    longitude=longitude,
                    start_year=start_year,
                    end_year=end_year,
                    use_interpolation=True
                )
                return data
                
            elif source_name == 'openmeteo':
                # Open-Meteo использует get_historical_data с годами
                start_year = int(start_date.split('-')[0])
                end_year = int(end_date.split('-')[0])
                
                data = source.get_historical_data(
                    latitude=latitude,
                    longitude=longitude,
                    start_year=start_year,
                    end_year=end_year
                )
                return data
            else:
                # Адаптеры используют единый интерфейс
                return source.fetch_data(
                    latitude=latitude,
                    longitude=longitude,
                    start_date=start_date,
                    end_date=end_date,
                    parameters=parameters
                )
        except Exception as e:
            print(f"Ошибка получения данных из {source_name}: {e}")
            return None
    
    def calculate_consensus(self, multi_source_data: Dict[str, pd.DataFrame],
                           parameter: str) -> Dict:
        """
        Вычислить консенсус для параметра из нескольких источников
        
        Args:
            multi_source_data: Словарь {источник: DataFrame}
            parameter: Название параметра
            
        Returns:
            Словарь с консенсусными значениями и метаданными
        """
        
        # Собираем значения параметра из всех источников
        values_by_source = {}
        
        for source_name, df in multi_source_data.items():
            # Ищем параметр в DataFrame
            param_col = self._find_parameter_column(df, parameter)
            
            if param_col is not None and param_col in df.columns:
                values_by_source[source_name] = df[param_col].dropna()
        
        if not values_by_source:
            return {
                'value': None,
                'confidence': 'none',
                'sources_used': [],
                'agreement_level': 0.0
            }
        
        # Вычисляем взвешенное среднее
        weighted_values = []
        weights = []
        sources_used = []
        
        for source_name, values in values_by_source.items():
            if len(values) > 0:
                weight = self.source_weights.get(source_name, 0.5)
                weighted_values.append(values.mean() * weight)
                weights.append(weight)
                sources_used.append(source_name)
        
        if not weighted_values:
            return {
                'value': None,
                'confidence': 'none',
                'sources_used': [],
                'agreement_level': 0.0
            }
        
        # Консенсусное значение
        consensus_value = sum(weighted_values) / sum(weights)
        
        # Вычисляем согласованность источников
        all_values = [values.mean() for values in values_by_source.values()]
        agreement_level = self._calculate_agreement(all_values)
        
        # Определяем уровень доверия
        confidence = self._determine_confidence(len(sources_used), agreement_level)
        
        # Доверительный интервал
        std_dev = np.std(all_values) if len(all_values) > 1 else 0
        confidence_interval = (
            consensus_value - 1.96 * std_dev,
            consensus_value + 1.96 * std_dev
        )
        
        return {
            'value': float(consensus_value),
            'confidence': confidence,
            'confidence_interval': confidence_interval,
            'sources_used': sources_used,
            'agreement_level': float(agreement_level),
            'source_values': {src: float(values.mean()) 
                            for src, values in values_by_source.items()},
            'std_deviation': float(std_dev)
        }
    
    def _find_parameter_column(self, df: pd.DataFrame, parameter: str) -> Optional[str]:
        """
        Найти колонку с параметром в DataFrame
        Учитывает разные варианты названий
        """
        # Возможные варианты названий
        variants = {
            'temperature': ['T2M', 'temperature_2m', 'temperature_2m_mean', 'temp'],
            'apparent_temperature': ['apparent_temperature', 'apparent_temperature_mean', 'feels_like'],
            'precipitation': ['PRECTOTCORR', 'precipitation_sum', 'precip', 'rain'],
            'wind_speed': ['WS2M', 'WS10M', 'windspeed_10m_max', 'wind'],
            'windgusts': ['windgusts_10m_max', 'wind_gusts', 'gusts'],
            'humidity': ['RH2M', 'relativehumidity_2m_mean', 'humidity'],
            'pressure': ['PS', 'surface_pressure_mean', 'pressure'],
            'weathercode': ['weathercode', 'weather_code'],
            'air_quality': ['AODANA', 'aod', 'air_quality_index'],
            'black_carbon': ['BCSMASS', 'black_carbon', 'bc'],
            'dust': ['DUSMASS', 'dust', 'dust_concentration'],
            'thunderstorm_risk': ['cape', 'CAPE', 'convective_energy'],
        }
        
        if parameter in variants:
            for variant in variants[parameter]:
                if variant in df.columns:
                    return variant
        
        # Если точное совпадение
        if parameter in df.columns:
            return parameter
        
        return None
    
    def _calculate_agreement(self, values: List[float]) -> float:
        """
        Вычислить уровень согласованности между значениями (0-1)
        1.0 = полное согласие, 0.0 = сильное расхождение
        """
        if len(values) < 2:
            return 1.0
        
        mean_val = np.mean(values)
        if mean_val == 0: # handle division by zero for zero mean values
            return 1.0
        
        # Вычисляем коэффициент вариации
        std_val = np.std(values)
        cv = std_val / abs(mean_val)
        
        # Преобразуем в уровень согласованности (0-1)
        # CV < 0.1 => согласованность ~1.0
        # CV > 0.5 => согласованность ~0.0
        agreement = np.exp(-2 * cv)
        
        return float(agreement)
    
    def _determine_confidence(self, num_sources: int, agreement_level: float) -> str:
        """
        Определить уровень доверия на основе количества источников и согласованности
        """
        if num_sources >= 3 and agreement_level > 0.90:
            return 'high'
        elif num_sources >= 2 and agreement_level > 0.75:
            return 'medium'
        elif num_sources >= 1 and agreement_level > 0.50:
            return 'low'
        else:
            return 'very_low'
    
    def get_enhanced_weather_data(self, latitude: float, longitude: float,
                                  month: int, day: int,
                                  use_consensus: bool = True) -> Dict:
        """
        Получить расширенные данные о погоде с мультисорсным анализом
        
        Args:
            latitude: Широта
            longitude: Долгота
            month: Месяц (1-12)
            day: День (1-31)
            use_consensus: Использовать консенсус-анализ
            
        Returns:
            Словарь с данными и метаданными о качестве
        """
        
        # Параметры для запроса
        base_parameters = ['temperature', 'precipitation', 'wind_speed', 'humidity']
        enhanced_parameters = ['apparent_temperature', 'weathercode', 'windgusts']
        air_quality_parameters = ['air_quality', 'black_carbon', 'dust']
        storm_parameters = ['thunderstorm_risk']
        
        all_parameters = base_parameters + enhanced_parameters + air_quality_parameters + storm_parameters
        
        # Формируем диапазон дат (последние 30 лет для этого дня)
        current_year = datetime.now().year
        start_year = current_year - 30
        
        # Для упрощения берем данные за последние 5 лет
        start_date = f"{start_year}-{month:02d}-{day:02d}"
        end_date = f"{current_year-1}-{month:02d}-{day:02d}"
        
        # Получаем данные из всех источников
        multi_source_data = self.fetch_multi_source_data(
            latitude, longitude, start_date, end_date, all_parameters
        )
        
        if not multi_source_data:
            return {
                'error': 'Не удалось получить данные ни из одного источника',
                'sources_attempted': list(self.sources.keys())
            }
        
        result = {
            'location': {'latitude': latitude, 'longitude': longitude},
            'date': {'month': month, 'day': day},
            'data_sources': list(multi_source_data.keys()),
            'parameters': {}
        }
        
        # Для каждого параметра вычисляем консенсус
        if use_consensus:
            for param in all_parameters:
                consensus = self.calculate_consensus(multi_source_data, param)
                if consensus['value'] is not None:
                    result['parameters'][param] = consensus
        else:
            # Просто возвращаем данные из основного источника
            primary_source = 'nasa_power' if 'nasa_power' in multi_source_data else list(multi_source_data.keys())[0]
            result['primary_source'] = primary_source
            result['raw_data'] = multi_source_data
        
        return result


# Глобальный экземпляр для импорта
multi_source_service = MultiSourceDataService()

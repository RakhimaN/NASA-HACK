"""
Сервис для получения данных из нескольких источников и проведения консенсус-анализа.
"""

import pandas as pd
import numpy as np
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Tuple, Optional

from weather_analysis.config import AppConfig
from weather_analysis.data_service import WeatherDataService
from weather_analysis.data_adapters import GESDISCAdapter, CPTECAdapter, OpenMeteoEnhancedAdapter
from weather_analysis.utils import AppLogger

logger = AppLogger.get_logger()

class MultiSourceDataService:
    """
    Объединяет данные из нескольких погодных источников и проводит консенсус-анализ.
    """
    
    def __init__(self, 
                 nasa_power_service: WeatherDataService,
                 open_meteo_enhanced_adapter: OpenMeteoEnhancedAdapter,
                 ges_disc_adapter: GESDISCAdapter,
                 cptec_adapter: CPTECAdapter):
        
        self.nasa_power_service = nasa_power_service
        self.open_meteo_enhanced_adapter = open_meteo_enhanced_adapter
        self.ges_disc_adapter = ges_disc_adapter
        self.cptec_adapter = cptec_adapter
        self.data_sources = {
            'nasa': self.nasa_power_service, # WeatherDataService напрямую, т.к. он уже унифицирован
            'openmeteo_enhanced': self.open_meteo_enhanced_adapter,
            'ges_disc': self.ges_disc_adapter,
            'cptec': self.cptec_adapter
        }
    
    def _fetch_data_from_source(self, source_name: str, 
                                date_obj: date, 
                                latitude: float, 
                                longitude: float,
                                use_interpolation: bool = False) -> Optional[Tuple[pd.DataFrame, str]]:
        """
        Внутренняя функция для получения данных от одного источника.
        """
        try:
            logger.info(f"Запрос данных из источника: {source_name} для {date_obj}")
            if source_name == 'nasa':
                # NASA POWER service get_data ожидает диапазон лет
                # Для одного дня, передаем один и тот же год
                data, source_info = self.nasa_power_service.get_data(
                    latitude, longitude, date_obj.year, date_obj.year,
                    use_interpolation=use_interpolation
                )
                # Фильтруем DataFrame для конкретного дня
                data = data[data['day_of_year'] == date_obj.timetuple().tm_yday].copy()
                if not data.empty:
                    return data, source_info
            else:
                # Для адаптеров, которые ожидают одну дату
                data, source_info = self.data_sources[source_name].get_data(
                    latitude, longitude, date_obj
                )
                if data is not None and not data.empty:
                    return data, source_info
            logger.warning(f"Данные из источника {source_name} для {date_obj} не получены.")
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении данных из {source_name} для {date_obj}: {e}")
            return None

    def fetch_all_sources_for_date(self, 
                                   date_obj: date, 
                                   latitude: float, 
                                   longitude: float,
                                   use_interpolation: bool = False,
                                   excluded_sources: Optional[List[str]] = None
                                   ) -> Dict[str, pd.DataFrame]:
        """
        Получает данные для одной даты из всех доступных источников параллельно.
        
        Args:
            date_obj: Целевая дата.
            latitude: Широта.
            longitude: Долгота.
            use_interpolation: Использовать интерполяцию для NASA POWER.
            excluded_sources: Список источников для исключения.
            
        Returns:
            Словарь, где ключ - название источника, значение - DataFrame с данными.
        """
        if excluded_sources is None:
            excluded_sources = []

        all_data = {}
        # Используем ThreadPoolExecutor для параллельного выполнения запросов
        with ThreadPoolExecutor(max_workers=len(self.data_sources)) as executor:
            future_to_source = {
                executor.submit(self._fetch_data_from_source, 
                                source_name, date_obj, latitude, longitude,
                                use_interpolation):
                source_name 
                for source_name in self.data_sources 
                if source_name not in excluded_sources
            }
            
            for future in as_completed(future_to_source):
                source_name = future_to_source[future]
                try:
                    result = future.result()
                    if result:
                        all_data[source_name] = result[0] # result[0] это DataFrame
                        logger.info(f"Данные успешно получены из {source_name} для {date_obj}")
                except Exception as exc:
                    logger.error(f'{source_name} сгенерировал исключение: {exc}')
        
        return all_data

    def get_consensus_for_date(self, 
                               date_obj: date, 
                               latitude: float, 
                               longitude: float,
                               parameters: List[str],
                               use_interpolation: bool = False,
                               excluded_sources: Optional[List[str]] = None
                               ) -> Dict[str, Any]:
        """
        Получает данные из всех источников и рассчитывает консенсус для указанных параметров.
        
        Args:
            date_obj: Целевая дата.
            latitude: Широта.
            longitude: Долгота.
            parameters: Список параметров для консенсус-анализа (например, ['T2M', 'PRECTOTCORR']).
            use_interpolation: Использовать интерполяцию для NASA POWER.
            excluded_sources: Список источников для исключения.
            
        Returns:
            Словарь с консенсусными значениями, их статистикой и уровнем согласия.
        """
        all_data = self.fetch_all_sources_for_date(
            date_obj, latitude, longitude, use_interpolation, excluded_sources
        )
        
        consensus_results = {
            "date": date_obj.isoformat(),
            "location": {"latitude": latitude, "longitude": longitude},
            "consensus_analysis": {},
            "available_sources": list(all_data.keys())
        }
        
        if not all_data:
            logger.warning(f"Нет доступных данных из источников для {date_obj}.")
            return consensus_results
        
        for param in parameters:
            param_values = []
            source_values = {}
            
            for source_name, df in all_data.items():
                # Пытаемся найти параметр в DataFrame
                if param in df.columns:
                    value = df.iloc[0][param] # Берем первое (единственное) значение для дня
                    param_values.append(value)
                    source_values[source_name] = float(value) # Сохраняем значения от каждого источника
                else:
                    # Логируем, если параметр отсутствует в источнике, но не прерываем работу
                    logger.debug(f"Параметр '{param}' отсутствует в данных от источника {source_name}.")
            
            if param_values:
                mean_val = np.mean(param_values)
                median_val = np.median(param_values)
                std_dev = np.std(param_values)
                min_val = np.min(param_values)
                max_val = np.max(param_values)
                
                # Уровень согласия (процент значений в пределах 1 стандартного отклонения от медианы)
                agreement_level = (np.sum(np.abs(param_values - median_val) <= std_dev) / len(param_values)) if len(param_values) > 1 else 1.0
                
                # Простое определение уверенности на основе количества источников и согласия
                confidence = "high" if agreement_level > 0.75 and len(param_values) >= 3 else \
                             "medium" if agreement_level > 0.5 and len(param_values) >= 2 else \
                             "low"
                
                consensus_results["consensus_analysis"][param] = {
                    "mean": float(mean_val),
                    "median": float(median_val),
                    "std_dev": float(std_dev),
                    "min": float(min_val),
                    "max": float(max_val),
                    "agreement_level": float(agreement_level),
                    "confidence": confidence,
                    "source_values": source_values # Значения от каждого источника
                }
            else:
                logger.warning(f"Недостаточно данных для консенсус-анализа параметра '{param}' для {date_obj}.")
                consensus_results["consensus_analysis"][param] = {
                    "mean": None, "median": None, "std_dev": None,
                    "min": None, "max": None, "agreement_level": 0.0, 
                    "confidence": "none", "source_values": {}
                }
        
        return consensus_results


if __name__ == "__main__":
    # Пример использования
    logger.info("Тестирование MultiSourceDataService...")
    
    # Инициализация отдельных сервисов и адаптеров
    nasa_power = WeatherDataService(preferred_source='nasa')
    open_meteo = OpenMeteoEnhancedAdapter() # Использует OpenMeteoDataService внутри
    ges_disc = GESDISCAdapter()
    cptec = CPTECAdapter()
    
    multi_service = MultiSourceDataService(
        nasa_power_service=nasa_power,
        open_meteo_enhanced_adapter=open_meteo,
        ges_disc_adapter=ges_disc,
        cptec_adapter=cptec
    )
    
    test_date = date(2023, 7, 15)
    test_lat, test_lon = 55.7558, 37.6173 # Москва
    test_params = [
        'T2M', 'PRECTOTCORR', 'WS2M', 'RH2M', 
        'AOD_550', 'CAPE', 'weather_code', 'apparent_temperature_max'
    ]
    
    print(f"\nПолучение данных из всех источников для {test_date}...")
    all_data_for_date = multi_service.fetch_all_sources_for_date(
        test_date, test_lat, test_lon, use_interpolation=True
    )
    
    for source, df in all_data_for_date.items():
        print(f"  {source}: {len(df)} записей")
        # print(df.to_string())
        
    print(f"\nПроведение консенсус-анализа для {test_date} и параметров: {test_params}...")
    consensus_analysis_result = multi_service.get_consensus_for_date(
        test_date, test_lat, test_lon, test_params, use_interpolation=True
    )
    
    import json
    print(json.dumps(consensus_analysis_result, indent=2, ensure_ascii=False))
    
    print("\nТестирование с исключенными источниками...")
    consensus_analysis_result_excluded = multi_service.get_consensus_for_date(
        test_date, test_lat, test_lon, test_params, use_interpolation=True, excluded_sources=['ges_disc', 'cptec']
    )
    print(json.dumps(consensus_analysis_result_excluded, indent=2, ensure_ascii=False))
    
    logger.info("MultiSourceDataService тестирование завершено.")

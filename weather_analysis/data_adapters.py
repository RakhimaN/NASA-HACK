"""
Адаптеры для различных источников погодных данных v2.0
Обеспечивают единообразный интерфейс для работы с разными API

Адаптеры:
    1. GESDISCAdapter - NASA GES DISC (MERRA-2) для качества воздуха
       • AODANA - оптическая толщина аэрозоля
       • BCSMASS - черный углерод (black carbon)
       • DUSMASS - пыль (dust)
       
    2. CPTECAdapter - Бразильский CPTEC для грозовой активности
       • CAPE - энергия конвекции (грозы, торнадо)
       • CIN - индекс подавления конвекции
       
    3. OpenMeteoEnhancedAdapter - Расширенный Open-Meteo API
       • apparent_temperature - ощущаемая температура (wind chill/heat index)
       • weathercode - коды погоды WMO (0-99)
       • windgusts - порывы ветра

Все адаптеры используют кэширование для оптимизации запросов.
"""

import requests
import hashlib
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import numpy as np


class BaseDataAdapter:
    """Базовый класс для всех адаптеров данных"""
    
    def __init__(self, cache_dir: str = "data/cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.source_name = "base"
    
    def _get_cache_key(self, **kwargs) -> str:
        """Генерирует ключ кэша на основе параметров запроса"""
        cache_string = f"{self.source_name}_{json.dumps(kwargs, sort_keys=True)}"
        return hashlib.md5(cache_string.encode()).hexdigest()
    
    def _get_from_cache(self, cache_key: str) -> Optional[pd.DataFrame]:
        """Получить данные из кэша"""
        cache_file = self.cache_dir / f"{cache_key}.parquet"
        
        if cache_file.exists():
            try:
                df = pd.read_parquet(cache_file)
                print(f"✓ Данные загружены из кэша: {self.source_name}")
                return df
            except Exception as e:
                print(f"⚠ Ошибка чтения кэша: {e}")
                return None
        return None
    
    def _save_to_cache(self, cache_key: str, data: pd.DataFrame):
        """Сохранить данные в кэш"""
        cache_file = self.cache_dir / f"{cache_key}.parquet"
        try:
            data.to_parquet(cache_file)
            print(f"✓ Данные сохранены в кэш: {self.source_name}")
        except Exception as e:
            print(f"⚠ Ошибка сохранения в кэш: {e}")
    
    def fetch_data(self, latitude: float, longitude: float, 
                   start_date: str, end_date: str, 
                   parameters: List[str]) -> Optional[pd.DataFrame]:
        """
        Получить данные из источника (должен быть переопределен в подклассах)
        
        Args:
            latitude: Широта
            longitude: Долгота
            start_date: Начальная дата (YYYY-MM-DD)
            end_date: Конечная дата (YYYY-MM-DD)
            parameters: Список параметров для получения
            
        Returns:
            DataFrame с данными или None при ошибке
        """
        raise NotImplementedError("Метод должен быть переопределен в подклассе")


class GESDISCAdapter(BaseDataAdapter):
    """
    Адаптер для NASA GES DISC (Global Earth Data и Science Center)
    Используется для получения данных о качестве воздуха, аэрозолях, озоне
    """
    
    def __init__(self, cache_dir: str = "data/cache"):
        super().__init__(cache_dir)
        self.source_name = "ges_disc"
        self.base_url = "https://goldsmr4.gesdisc.eosdis.nasa.gov/opendap"
        
        # Маппинг параметров на датасеты MERRA-2
        self.dataset_mapping = {
            'AODANA': 'M2T1NXAER',  # Aerosol Optical Depth Analysis
            'BCSMASS': 'M2T1NXAER',  # Black Carbon Surface Mass
            'DUSMASS': 'M2T1NXAER',  # Dust Surface Mass
            'SO2SMASS': 'M2T1NXAER', # SO2 Surface Mass
            'SO4SMASS': 'M2T1NXAER', # SO4 Surface Mass
            'SSSMASS': 'M2T1NXAER',  # Sea Salt Surface Mass
            'TO3': 'M2T1NXCHM',      # Total Ozone
            'TROPO3': 'M2T1NXCHM',   # Tropospheric Ozone
        }
    
    def fetch_data(self, latitude: float, longitude: float,
                   start_date: str, end_date: str,
                   parameters: List[str]) -> Optional[pd.DataFrame]:
        """
        Получить данные из GES DISC
        
        Примечание: GES DISC требует авторизацию через NASA Earthdata
        Для упрощения используем mock данные или Open-Meteo как альтернативу
        """
        
        # Проверяем кэш
        cache_key = self._get_cache_key(
            lat=latitude, lon=longitude,
            start=start_date, end=end_date,
            params=parameters
        )
        
        cached_data = self._get_from_cache(cache_key)
        if cached_data is not None:
            return cached_data
        
        print(f"⚠ GES DISC: Требуется авторизация NASA Earthdata")
        print(f"📊 Генерируем mock данные для: {parameters}")
        
        # Генерируем mock данные на основе исторических средних
        data = self._generate_mock_air_quality_data(
            latitude, longitude, start_date, end_date, parameters
        )
        
        if data is not None:
            self._save_to_cache(cache_key, data)
        
        return data
    
    def _generate_mock_air_quality_data(self, latitude: float, longitude: float,
                                        start_date: str, end_date: str,
                                        parameters: List[str]) -> pd.DataFrame:
        """
        Генерирует реалистичные данные о качестве воздуха
        На основе географической локации и времени года
        """
        
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        data = {'date': dates}
        
        # Базовые значения зависят от региона
        is_urban = abs(latitude) < 60  # Урбанизированные регионы
        is_desert = (abs(latitude) < 35) and (abs(longitude) > 30)  # Пустынные регионы
        
        for param in parameters:
            if param == 'AODANA':  # Aerosol Optical Depth
                base = 0.2 if is_urban else 0.1
                if is_desert:
                    base = 0.35
                # Сезонные вариации
                seasonal = 0.1 * np.sin(2 * np.pi * dates.dayofyear / 365)
                noise = np.random.normal(0, 0.05, len(dates))
                data[param] = np.maximum(0.05, base + seasonal + noise)
            
            elif param == 'BCSMASS':  # Black Carbon (μg/m³)
                base = 2.5 if is_urban else 0.5
                seasonal = 0.5 * np.sin(2 * np.pi * dates.dayofyear / 365)
                noise = np.random.normal(0, 0.3, len(dates))
                data[param] = np.maximum(0.1, base + seasonal + noise)
            
            elif param == 'DUSMASS':  # Dust (μg/m³)
                base = 50 if is_desert else 10
                if is_urban:
                    base = 20
                # Пыль выше весной и летом
                seasonal = base * 0.5 * np.sin(2 * np.pi * (dates.dayofyear - 80) / 365)
                noise = np.random.normal(0, base * 0.2, len(dates))
                data[param] = np.maximum(1, base + seasonal + noise)
            
            elif param == 'SO2SMASS':  # SO2 (μg/m³)
                base = 10 if is_urban else 2
                seasonal = 2 * np.sin(2 * np.pi * dates.dayofyear / 365)
                noise = np.random.normal(0, 1, len(dates))
                data[param] = np.maximum(0.5, base + seasonal + noise)
            
            elif param == 'SO4SMASS':  # Sulfates (μg/m³)
                base = 5 if is_urban else 1
                seasonal = 1 * np.sin(2 * np.pi * dates.dayofyear / 365)
                noise = np.random.normal(0, 0.5, len(dates))
                data[param] = np.maximum(0.2, base + seasonal + noise)
            
            elif param == 'SSSMASS':  # Sea Salt (μg/m³)
                # Зависит от близости к океану
                distance_to_coast = min(abs(latitude - 0), abs(90 - abs(latitude)))
                base = 15 if distance_to_coast > 20 else 3
                seasonal = 3 * np.sin(2 * np.pi * dates.dayofyear / 365)
                noise = np.random.normal(0, 2, len(dates))
                data[param] = np.maximum(0.5, base + seasonal + noise)
        
        df = pd.DataFrame(data)
        df['source'] = 'GES_DISC_mock'
        
        return df


class CPTECAdapter(BaseDataAdapter):
    """
    Адаптер для Brazilian CPTEC (Centro de Previsão de Tempo e Estudos Climáticos)
    Используется для получения данных о грозовой активности (CAPE, CIN)
    """
    
    def __init__(self, cache_dir: str = "data/cache"):
        super().__init__(cache_dir)
        self.source_name = "cptec"
        self.base_url = "http://ftp.cptec.inpe.br/modelos"
    
    def fetch_data(self, latitude: float, longitude: float,
                   start_date: str, end_date: str,
                   parameters: List[str]) -> Optional[pd.DataFrame]:
        """
        Получить данные из CPTEC
        
        Примечание: CPTEC API сложный и специфичный для Южной Америки
        Используем mock данные или альтернативные источники
        """
        
        # Проверяем кэш
        cache_key = self._get_cache_key(
            lat=latitude, lon=longitude,
            start=start_date, end=end_date,
            params=parameters
        )
        
        cached_data = self._get_from_cache(cache_key)
        if cached_data is not None:
            return cached_data
        
        # Проверяем, находится ли локация в Южной Америке
        is_south_america = (-60 < latitude < 15) and (-85 < longitude < -30)
        
        if not is_south_america:
            print(f"⚠ CPTEC: Локация вне зоны покрытия (только Южная Америка)")
            return None
        
        print(f"📊 CPTEC: Генерируем данные о грозовой активности")
        
        data = self._generate_mock_thunderstorm_data(
            latitude, longitude, start_date, end_date, parameters
        )
        
        if data is not None:
            self._save_to_cache(cache_key, data)
        
        return data
    
    def _generate_mock_thunderstorm_data(self, latitude: float, longitude: float,
                                         start_date: str, end_date: str,
                                         parameters: List[str]) -> pd.DataFrame:
        """
        Генерирует реалистичные данные о грозовой активности
        CAPE (Convective Available Potential Energy) - энергия конвекции
        """
        
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        data = {'date': dates}
        
        # CAPE зависит от сезона и температуры
        is_tropical = abs(latitude) < 23.5
        
        for param in parameters:
            if param == 'cape':  # CAPE (J/kg)
                # Тропики: высокий CAPE круглый год
                # Умеренные широты: высокий CAPE летом
                if is_tropical:
                    base = 1500
                    seasonal = 800 * np.sin(2 * np.pi * dates.dayofyear / 365)
                else:
                    base = 800
                    # Максимум летом (день года ~180)
                    seasonal = 1200 * np.sin(2 * np.pi * (dates.dayofyear - 80) / 365)
                    seasonal = np.maximum(seasonal, -500)  # Зимой может быть близко к 0
                
                # Случайные всплески (грозовые дни)
                spikes = np.random.choice([0, 1], size=len(dates), p=[0.7, 0.3])
                spike_values = np.random.uniform(500, 2000, len(dates)) * spikes
                
                noise = np.random.normal(0, 200, len(dates))
                data[param] = np.maximum(0, base + seasonal + spike_values + noise)
            
            elif param == 'cin':  # CIN (J/kg) - обычно отрицательное
                # CIN подавляет конвекцию
                base = -30
                noise = np.random.normal(0, 20, len(dates))
                data[param] = np.minimum(0, base + noise)
            
            elif param == 'lifted_index':  # Lifted Index (°C)
                # Отрицательный LI = нестабильная атмосфера
                base = -2
                seasonal = -3 * np.sin(2 * np.pi * (dates.dayofyear - 80) / 365)
                noise = np.random.normal(0, 2, len(dates))
                data[param] = base + seasonal + noise
        
        df = pd.DataFrame(data)
        df['source'] = 'CPTEC_mock'
        
        return df


class OpenMeteoEnhancedAdapter(BaseDataAdapter):
    """
    Расширенный адаптер для Open-Meteo API
    Добавляет поддержку новых параметров: apparent_temperature, weathercode, windgusts
    """
    
    def __init__(self, cache_dir: str = "data/cache"):
        super().__init__(cache_dir)
        self.source_name = "openmeteo_enhanced"
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
    
    def fetch_data(self, latitude: float, longitude: float,
                   start_date: str, end_date: str,
                   parameters: List[str]) -> Optional[pd.DataFrame]:
        """
        Получить данные из Open-Meteo с новыми параметрами
        """
        
        # Проверяем кэш
        cache_key = self._get_cache_key(
            lat=latitude, lon=longitude,
            start=start_date, end=end_date,
            params=parameters
        )
        
        cached_data = self._get_from_cache(cache_key)
        if cached_data is not None:
            return cached_data
        
        # Маппинг наших параметров на Open-Meteo параметры
        param_mapping = {
            'apparent_temperature': 'apparent_temperature_mean',
            'weathercode': 'weathercode',
            'windgusts': 'windgusts_10m_max',
            'temperature': 'temperature_2m_mean',
            'wind_speed': 'windspeed_10m_max',
            'precipitation': 'precipitation_sum',
            'humidity': 'relativehumidity_2m_mean',
        }
        
        # Формируем список параметров для запроса
        openmeteo_params = []
        for param in parameters:
            if param in param_mapping:
                openmeteo_params.append(param_mapping[param])
            else:
                openmeteo_params.append(param)
        
        # Запрос к API
        params = {
            'latitude': latitude,
            'longitude': longitude,
            'start_date': start_date,
            'end_date': end_date,
            'daily': ','.join(openmeteo_params),
            'timezone': 'auto'
        }
        
        try:
            print(f"🌐 Запрос к Open-Meteo API...")
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            
            # Парсим ответ
            if 'daily' in result:
                daily_data = result['daily']
                df = pd.DataFrame({
                    'date': pd.to_datetime(daily_data['time']),
                })
                
                # Добавляем все параметры
                for param in openmeteo_params:
                    if param in daily_data:
                        df[param] = daily_data[param]
                
                df['source'] = 'Open-Meteo'
                
                self._save_to_cache(cache_key, df)
                print(f"✓ Данные получены из Open-Meteo")
                
                return df
            else:
                print(f"⚠ Неожиданный формат ответа от Open-Meteo")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"⚠ Ошибка запроса к Open-Meteo: {e}")
            return None
        except Exception as e:
            print(f"⚠ Ошибка обработки данных Open-Meteo: {e}")
            return None


# Фабрика для создания адаптеров
def get_adapter(source_name: str, cache_dir: str = "data/cache") -> BaseDataAdapter:
    """
    Получить адаптер для указанного источника данных
    
    Args:
        source_name: Название источника ('ges_disc', 'cptec', 'openmeteo_enhanced')
        cache_dir: Директория для кэша
        
    Returns:
        Экземпляр адаптера
    """
    adapters = {
        'ges_disc': GESDISCAdapter,
        'cptec': CPTECAdapter,
        'openmeteo_enhanced': OpenMeteoEnhancedAdapter,
    }
    
    if source_name not in adapters:
        raise ValueError(f"Неизвестный источник: {source_name}. Доступные: {list(adapters.keys())}")
    
    return adapters[source_name](cache_dir)

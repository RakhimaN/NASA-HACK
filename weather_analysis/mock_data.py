"""
Mock данные для тестирования без доступа к внешним API
Создает синтетические данные похожие на реальные от NASA
"""

import pandas as pd
import numpy as np
from pathlib import Path


class MockWeatherData:
    """Генератор mock данных для тестирования"""
    
    @staticmethod
    def generate_climatology(latitude: float, longitude: float, 
                            start_year: int = 1990, end_year: int = 2023) -> pd.DataFrame:
        """
        Генерирует синтетические климатологические данные
        
        Имитирует структуру данных от NASA POWER API
        """
        days_in_year = 365
        
        # Определяем климатические параметры на основе широты
        # Москва ~55°N - континентальный климат
        base_temp = 5.0  # Средняя годовая температура
        if abs(latitude) < 30:
            base_temp = 25.0  # Тропики
        elif abs(latitude) < 50:
            base_temp = 15.0  # Умеренный пояс
        
        # Генерируем данные для каждого дня года
        data = []
        
        for day in range(1, days_in_year + 1):
            # Температура с сезонными колебаниями
            # Синусоида для имитации смены сезонов
            seasonal_effect = 15 * np.sin(2 * np.pi * (day - 80) / 365)
            
            # Добавляем случайный шум
            daily_variation = np.random.normal(0, 3)
            
            temp_mean = base_temp + seasonal_effect + daily_variation
            temp_max = temp_mean + np.random.uniform(3, 8)
            temp_min = temp_mean - np.random.uniform(3, 8)
            
            # Осадки (больше летом для континентального климата)
            precip_seasonal = 1.5 if 120 < day < 240 else 1.0
            precipitation = np.random.gamma(2, 1) * precip_seasonal
            
            # Ветер (больше зимой и весной)
            wind_seasonal = 1.3 if day < 100 or day > 300 else 0.8
            wind_speed = np.random.gamma(3, 1.5) * wind_seasonal
            
            # Влажность (выше летом)
            humidity_seasonal = 15 if 150 < day < 250 else 0
            humidity = 50 + humidity_seasonal + np.random.normal(0, 10)
            humidity = np.clip(humidity, 20, 95)
            
            # Давление
            pressure = 101 + np.random.normal(0, 1)
            
            data.append({
                'day_of_year': day,
                'T2M': temp_mean,
                'T2M_MAX': temp_max,
                'T2M_MIN': temp_min,
                'PRECTOTCORR': precipitation,
                'WS2M': wind_speed,
                'RH2M': humidity,
                'PS': pressure,
                'CLOUD_AMT': cloudiness
            })
        
        df = pd.DataFrame(data)
        return df
    
    @staticmethod
    def save_mock_data(latitude: float, longitude: float, 
                       cache_dir: str = './data/cache'):
        """Сохранить mock данные в кэш"""
        cache_path = Path(cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        
        # Генерируем данные
        df = MockWeatherData.generate_climatology(latitude, longitude)
        
        # Создаем имя файла как в data_service
        import hashlib
        key_str = f"{latitude}_{longitude}_1990_2023"
        cache_key = hashlib.md5(key_str.encode()).hexdigest()
        
        # Сохраняем
        filepath = cache_path / f"{cache_key}.parquet"
        df.to_parquet(filepath)
        
        print(f"✓ Mock данные сохранены: {filepath}")
        return df


class MockWeatherDataSource:
    """
    Источник mock данных для тестирования
    Заменяет NASA и Open-Meteo API
    """
    
    def __init__(self, cache_dir: str = "./data/cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get_historical_data(self, latitude: float, longitude: float,
                           start_year: int = 1990, end_year: int = 2023) -> pd.DataFrame:
        """
        Получить mock данные
        """
        print(f"🧪 Использование MOCK данных для тестирования")
        print(f"   Локация: ({latitude}, {longitude})")
        print(f"   Период: {start_year}-{end_year}")
        
        # Генерируем данные
        df = MockWeatherData.generate_climatology(latitude, longitude, start_year, end_year)
        
        print(f"✓ Сгенерировано {len(df)} дней данных")
        
        return df


# Функция для быстрого создания тестовых данных
def create_test_dataset(locations: list = None):
    """
    Создать тестовые наборы данных для популярных городов
    
    Args:
        locations: Список кортежей (название, широта, долгота)
    """
    if locations is None:
        locations = [
            ("Москва", 55.7558, 37.6173),
            ("Нью-Йорк", 40.7128, -74.0060),
            ("Токио", 35.6762, 139.6503),
            ("Лондон", 51.5074, -0.1278),
            ("Сидней", -33.8688, 151.2093),
        ]
    
    print("🧪 Создание тестовых наборов данных...\n")
    
    for name, lat, lon in locations:
        print(f"📍 {name}:")
        MockWeatherData.save_mock_data(lat, lon)
        print()
    
    print("✅ Все тестовые данные созданы!")


if __name__ == "__main__":
    # Создаем тестовые данные
    create_test_dataset()
    
    # Пример использования
    print("\n" + "="*60)
    print("ПРИМЕР ИСПОЛЬЗОВАНИЯ MOCK ДАННЫХ")
    print("="*60 + "\n")
    
    mock_source = MockWeatherDataSource()
    data = mock_source.get_historical_data(55.7558, 37.6173)
    
    print(f"\nПервые 5 строк:")
    print(data.head())
    
    print(f"\nСтатистика:")
    print(data.describe())

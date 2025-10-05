"""
Модуль для генерации мок-данных для тестирования и разработки.
Используется, когда нет доступа к реальным API или для быстрой отладки.
"""

import pandas as pd
import numpy as np
from datetime import date, timedelta
from typing import Dict, Any, List


def generate_mock_nasa_power_data(
    start_year: int,
    end_year: int,
    latitude: float,
    longitude: float,
    params: List[str] = None
) -> Dict[str, Any]:
    """
    Генерирует мок-данные в формате, похожем на вывод NASA POWER API.
    
    Args:
        start_year: Начальный год.
        end_year: Конечный год.
        latitude: Широта.
        longitude: Долгота.
        params: Список параметров для генерации. Если None, генерирует стандартный набор.
        
    Returns:
        Словарь с мок-данными.
    """
    if params is None:
        params = [
            "T2M", "T2M_MAX", "T2M_MIN", "RH2M", "PRECTOTCORR", 
            "WS2M", "WD2M", "ALLSKY_SFC_SW_DWN", "CLRSKY_SFC_SW_DWN",
            "ALLSKY_SFC_LW_DWN", "PS", "QV2M", "WSPD10M", "WSPD50M",
            "FRANCIS_THUNDER"
        ]
        
    data = {"parameters": {}}
    
    for param in params:
        param_data = {}
        for year in range(start_year, end_year + 1):
            for day_of_year in range(1, 366):
                current_date = date(year, 1, 1) + timedelta(day_of_year - 1)
                date_str = current_date.strftime("%Y%m%d")
                
                # Генерация случайных, но правдоподобных значений
                if "T2M" in param:
                    value = np.random.uniform(10, 30) + np.sin(day_of_year / 365 * 2 * np.pi) * 15 # Сезонный цикл
                elif "RH2M" in param:
                    value = np.random.uniform(50, 90)
                elif "PRECTOTCORR" in param:
                    value = np.random.uniform(0, 10) if np.random.rand() < 0.3 else 0 # С вероятностью 30% есть осадки
                elif "WS2M" in param:
                    value = np.random.uniform(1, 10)
                elif "WD2M" in param:
                    value = np.random.uniform(0, 360)
                elif "ALLSKY_SFC_SW_DWN" in param:
                    value = np.random.uniform(100, 1000) + np.sin(day_of_year / 365 * 2 * np.pi) * 500
                elif "PS" in param:
                    value = np.random.uniform(98, 102)
                elif "FRANCIS_THUNDER" in param:
                    value = 1 if np.random.rand() < 0.05 else 0 # 5% шанс грозы
                else:
                    value = np.random.uniform(0, 100) # Общие значения
                
                param_data[date_str] = round(float(value), 2)
        data["parameters"][param] = param_data
        
    # Добавляем общие метаданные
    data["header"] = {
        "title": "POWER Daily Climatology",
        "api_version": "V2.0",
        "query": {"latitude": latitude, "longitude": longitude, "start": start_year, "end": end_year}
    }
    
    return data


def generate_mock_openmeteo_data(
    start_date: date,
    end_date: date,
    latitude: float,
    longitude: float,
    params: List[str] = None
) -> Dict[str, Any]:
    """
    Генерирует мок-данные в формате, похожем на вывод Open-Meteo API.
    
    Args:
        start_date: Начальная дата.
        end_date: Конечная дата.
        latitude: Широта.
        longitude: Долгота.
        params: Список параметров для генерации. Если None, генерирует стандартный набор.
        
    Returns:
        Словарь с мок-данными.
    """
    if params is None:
        params = [
            "temperature_2m", "relative_humidity_2m", "precipitation", 
            "wind_speed_10m", "wind_direction_10m", "shortwave_radiation",
            "pressure_msl", "apparent_temperature", "wind_gusts_10m",
            "weather_code", "is_day"
        ]

    daily_data = {
        "time": [],
        "temperature_2m_max": [],
        "temperature_2m_min": [],
        "apparent_temperature_max": [],
        "apparent_temperature_min": [],
        "precipitation_sum": [],
        "rain_sum": [],
        "snowfall_sum": [],
        "precipitation_hours": [],
        "wind_speed_10m_max": [],
        "wind_gusts_10m_max": [],
        "wind_direction_10m_dominant": [],
        "shortwave_radiation_sum": [],
        "weather_code": [],
        "pressure_msl_mean": [],
        "et0_fao_evapotranspiration": []
    }

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        daily_data["time"].append(date_str)
        
        # Генерация случайных, но правдоподобных значений
        day_of_year = current_date.timetuple().tm_yday
        season_factor = np.sin(day_of_year / 365 * 2 * np.pi) * 0.5 + 0.5 # 0 to 1 for seasonality

        temp_base = 15 + (season_factor * 15) # Base temperature for the season
        daily_data["temperature_2m_max"].append(round(temp_base + np.random.uniform(3, 8), 1))
        daily_data["temperature_2m_min"].append(round(temp_base - np.random.uniform(3, 8), 1))
        daily_data["apparent_temperature_max"].append(round(daily_data["temperature_2m_max"][-1] + np.random.uniform(-2, 2), 1))
        daily_data["apparent_temperature_min"].append(round(daily_data["temperature_2m_min"][-1] + np.random.uniform(-2, 2), 1))
        
        precipitation = np.random.uniform(0, 15) if np.random.rand() < 0.3 else 0
        daily_data["precipitation_sum"].append(round(precipitation, 1))
        daily_data["rain_sum"].append(round(precipitation * 0.8, 1) if precipitation > 0 else 0)
        daily_data["snowfall_sum"].append(round(precipitation * 0.2, 1) if precipitation > 0 and temp_base < 5 else 0)
        daily_data["precipitation_hours"].append(np.random.randint(1, 8) if precipitation > 0 else 0)

        daily_data["wind_speed_10m_max"].append(round(np.random.uniform(5, 20), 1))
        daily_data["wind_gusts_10m_max"].append(round(daily_data["wind_speed_10m_max"][-1] * np.random.uniform(1.2, 1.5), 1))
        daily_data["wind_direction_10m_dominant"].append(np.random.randint(0, 360))
        daily_data["shortwave_radiation_sum"].append(round(np.random.uniform(500, 3000) * season_factor + np.random.uniform(0, 500), 1))
        daily_data["weather_code"].append(np.random.choice([0, 1, 2, 3, 51, 61, 63, 65, 71, 73, 75, 80, 81, 82, 95]))
        daily_data["pressure_msl_mean"].append(round(np.random.uniform(1000, 1025), 1))
        daily_data["et0_fao_evapotranspiration"].append(round(np.random.uniform(1, 5) * season_factor, 1))

        current_date += timedelta(days=1)
    
    return {
        "latitude": latitude,
        "longitude": longitude,
        "generationtime_ms": np.random.uniform(5, 50),
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "elevation": np.random.uniform(0, 1000),
        "daily_units": {
            "time": "iso8601",
            "temperature_2m_max": "°C",
            "temperature_2m_min": "°C",
            "apparent_temperature_max": "°C",
            "apparent_temperature_min": "°C",
            "precipitation_sum": "mm",
            "rain_sum": "mm",
            "snowfall_sum": "cm",
            "precipitation_hours": "h",
            "wind_speed_10m_max": "km/h",
            "wind_gusts_10m_max": "km/h",
            "wind_direction_10m_dominant": "°",
            "shortwave_radiation_sum": "MJ/m²",
            "weather_code": "wmo code",
            "pressure_msl_mean": "hPa",
            "et0_fao_evapotranspiration": "mm"
        },
        "daily": daily_data
    }


def generate_mock_ges_disc_data(
    start_date: date,
    end_date: date,
    latitude: float,
    longitude: float,
) -> Dict[str, Any]:
    """
    Генерирует мок-данные для GES DISC (упрощенный формат для AOD/Air Quality).
    """
    data = {"data": []}
    current_date = start_date
    while current_date <= end_date:
        day_data = {
            "date": current_date.strftime("%Y-%m-%d"),
            "AOD_550": round(np.random.uniform(0.05, 0.5), 2),  # Aerosol Optical Depth
            "BC_MASS": round(np.random.uniform(0.1, 5.0), 2),  # Black Carbon Mass
            "DUST_MASS": round(np.random.uniform(0.01, 2.0), 2) # Dust Mass
        }
        data["data"].append(day_data)
        current_date += timedelta(days=1)
    return data


def generate_mock_cptec_data(
    start_date: date,
    end_date: date,
    latitude: float,
    longitude: float,
) -> Dict[str, Any]:
    """
    Генерирует мок-данные для CPTEC (упрощенный формат для CAPE/Thunderstorm Risk).
    """
    data = {"data": []}
    current_date = start_date
    while current_date <= end_date:
        day_data = {
            "date": current_date.strftime("%Y-%m-%d"),
            "CAPE": round(np.random.uniform(0, 3000), 0),  # Convective Available Potential Energy
            "CIN": round(np.random.uniform(-300, 0), 0)   # Convective Inhibition
        }
        data["data"].append(day_data)
        current_date += timedelta(days=1)
    return data


if __name__ == "__main__":
    print("Генерация мок-данных NASA POWER...")
    nasa_mock = generate_mock_nasa_power_data(2020, 2022, 34.05, -118.25)
    # print(json.dumps(nasa_mock, indent=2))
    print(f"Сгенерировано данных для NASA POWER для {len(nasa_mock["parameters"]["T2M"])} дней.")

    print("\nГенерация мок-данных Open-Meteo...")
    openmeteo_mock = generate_mock_openmeteo_data(date(2023, 1, 1), date(2023, 1, 5), 34.05, -118.25)
    # print(json.dumps(openmeteo_mock, indent=2))
    print(f"Сгенерировано данных для Open-Meteo для {len(openmeteo_mock["daily"]["time"])} дней.")

    print("\nГенерация мок-данных GES DISC...")
    ges_disc_mock = generate_mock_ges_disc_data(date(2023, 1, 1), date(2023, 1, 3), 34.05, -118.25)
    print(f"Сгенерировано данных для GES DISC для {len(ges_disc_mock["data"])} дней.")

    print("\nГенерация мок-данных CPTEC...")
    cptec_mock = generate_mock_cptec_data(date(2023, 1, 1), date(2023, 1, 3), 34.05, -118.25)
    print(f"Сгенерировано данных для CPTEC для {len(cptec_mock["data"])} дней.")

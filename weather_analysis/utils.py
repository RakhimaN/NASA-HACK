"""
Вспомогательные утилиты для работы с погодными данными
"""

from datetime import datetime
import pandas as pd


def date_to_day_of_year(date_str: str) -> int:
    """
    Конвертировать дату в день года
    
    Args:
        date_str: Дата в формате 'YYYY-MM-DD'
        
    Returns:
        День года (1-365/366)
    """
    date = datetime.strptime(date_str, '%Y-%m-%d')
    return date.timetuple().tm_yday


def day_of_year_to_date(day: int, year: int = 2024) -> str:
    """
    Конвертировать день года в дату
    
    Args:
        day: День года (1-365)
        year: Год (по умолчанию 2024)
        
    Returns:
        Дата в формате 'YYYY-MM-DD'
    """
    date = datetime(year, 1, 1) + pd.Timedelta(days=day - 1)
    return date.strftime('%Y-%m-%d')


def format_probability(probability: float) -> str:
    """
    Форматировать вероятность для отображения
    
    Args:
        probability: Вероятность (0.0 - 1.0)
        
    Returns:
        Строка с процентами
    """
    return f"{probability * 100:.1f}%"


def categorize_probability(probability: float) -> str:
    """
    Категоризировать вероятность для удобного описания
    
    Args:
        probability: Вероятность (0.0 - 1.0)
        
    Returns:
        Категория: 'very_low', 'low', 'moderate', 'high', 'very_high'
    """
    if probability < 0.10:
        return 'very_low'  # Очень маловероятно
    elif probability < 0.25:
        return 'low'  # Маловероятно
    elif probability < 0.50:
        return 'moderate'  # Умеренная вероятность
    elif probability < 0.75:
        return 'high'  # Высокая вероятность
    else:
        return 'very_high'  # Очень высокая вероятность


def get_probability_description(probability: float, condition: str) -> str:
    """
    Получить текстовое описание вероятности
    
    Args:
        probability: Вероятность (0.0 - 1.0)
        condition: Погодное условие
        
    Returns:
        Текстовое описание
    """
    category = categorize_probability(probability)
    percent = format_probability(probability)
    
    descriptions = {
        'very_low': f"Очень маловероятно ({percent})",
        'low': f"Маловероятно ({percent})",
        'moderate': f"Умеренная вероятность ({percent})",
        'high': f"Высокая вероятность ({percent})",
        'very_high': f"Очень высокая вероятность ({percent})"
    }
    
    return descriptions[category]


def validate_coordinates(latitude: float, longitude: float) -> bool:
    """
    Проверить корректность координат
    
    Args:
        latitude: Широта
        longitude: Долгота
        
    Returns:
        True если координаты валидны
    """
    if not (-90 <= latitude <= 90):
        return False
    if not (-180 <= longitude <= 180):
        return False
    return True


def get_season(day_of_year: int, hemisphere: str = 'north') -> str:
    """
    Определить сезон по дню года
    
    Args:
        day_of_year: День года (1-365)
        hemisphere: 'north' или 'south'
        
    Returns:
        Название сезона: 'winter', 'spring', 'summer', 'autumn'
    """
    # Для северного полушария
    if hemisphere == 'north':
        if day_of_year < 80 or day_of_year >= 355:  # Dec 21 - Mar 20
            return 'winter'
        elif day_of_year < 172:  # Mar 21 - Jun 20
            return 'spring'
        elif day_of_year < 266:  # Jun 21 - Sep 22
            return 'summer'
        else:  # Sep 23 - Dec 20
            return 'autumn'
    else:  # Южное полушарие (инвертировано)
        if day_of_year < 80 or day_of_year >= 355:
            return 'summer'
        elif day_of_year < 172:
            return 'autumn'
        elif day_of_year < 266:
            return 'winter'
        else:
            return 'spring'


def export_to_csv(data: dict, filename: str):
    """
    Экспортировать результаты в CSV
    
    Args:
        data: Словарь с результатами анализа
        filename: Имя файла для сохранения
    """
    df = pd.DataFrame([data])
    df.to_csv(filename, index=False)
    print(f"✓ Данные сохранены в {filename}")


def export_to_json(data: dict, filename: str):
    """
    Экспортировать результаты в JSON
    
    Args:
        data: Словарь с результатами анализа
        filename: Имя файла для сохранения
    """
    import json
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Данные сохранены в {filename}")


# ============================================================================
# КОНВЕРТАЦИЯ ЕДИНИЦ ИЗМЕРЕНИЯ
# ============================================================================

def celsius_to_fahrenheit(celsius: float) -> float:
    """
    Конвертировать Цельсий в Фаренгейт
    
    Args:
        celsius: Температура в °C
        
    Returns:
        Температура в °F
    """
    return celsius * 9/5 + 32


def fahrenheit_to_celsius(fahrenheit: float) -> float:
    """
    Конвертировать Фаренгейт в Цельсий
    
    Args:
        fahrenheit: Температура в °F
        
    Returns:
        Температура в °C
    """
    return (fahrenheit - 32) * 5/9


def ms_to_kmh(ms: float) -> float:
    """
    Конвертировать м/с в км/ч
    
    Args:
        ms: Скорость в м/с
        
    Returns:
        Скорость в км/ч
    """
    return ms * 3.6


def ms_to_mph(ms: float) -> float:
    """
    Конвертировать м/с в мили/ч
    
    Args:
        ms: Скорость в м/с
        
    Returns:
        Скорость в mph
    """
    return ms * 2.23694


def kmh_to_ms(kmh: float) -> float:
    """
    Конвертировать км/ч в м/с
    
    Args:
        kmh: Скорость в км/ч
        
    Returns:
        Скорость в м/с
    """
    return kmh / 3.6


def mph_to_ms(mph: float) -> float:
    """
    Конвертировать мили/ч в м/с
    
    Args:
        mph: Скорость в mph
        
    Returns:
        Скорость в м/с
    """
    return mph / 2.23694


def mm_to_inches(mm: float) -> float:
    """
    Конвертировать миллиметры в дюймы
    
    Args:
        mm: Длина в мм
        
    Returns:
        Длина в дюймах
    """
    return mm / 25.4


def inches_to_mm(inches: float) -> float:
    """
    Конвертировать дюймы в миллиметры
    
    Args:
        inches: Длина в дюймах
        
    Returns:
        Длина в мм
    """
    return inches * 25.4


def kpa_to_mmhg(kpa: float) -> float:
    """
    Конвертировать кПа в мм рт.ст.
    
    Args:
        kpa: Давление в кПа
        
    Returns:
        Давление в мм рт.ст.
    """
    return kpa * 7.50062


def kpa_to_inhg(kpa: float) -> float:
    """
    Конвертировать кПа в дюймы рт.ст.
    
    Args:
        kpa: Давление в кПа
        
    Returns:
        Давление в дюймах рт.ст.
    """
    return kpa * 0.2953


def convert_temperature(value: float, from_unit: str, to_unit: str) -> float:
    """
    Универсальная конвертация температуры
    
    Args:
        value: Значение температуры
        from_unit: Исходная единица ('celsius' или 'fahrenheit')
        to_unit: Целевая единица ('celsius' или 'fahrenheit')
        
    Returns:
        Конвертированное значение
    """
    if from_unit == to_unit:
        return value
    
    if from_unit == 'celsius' and to_unit == 'fahrenheit':
        return celsius_to_fahrenheit(value)
    elif from_unit == 'fahrenheit' and to_unit == 'celsius':
        return fahrenheit_to_celsius(value)
    else:
        raise ValueError(f"Unsupported conversion: {from_unit} to {to_unit}")


def convert_wind_speed(value: float, from_unit: str, to_unit: str) -> float:
    """
    Универсальная конвертация скорости ветра
    
    Args:
        value: Значение скорости
        from_unit: Исходная единица ('ms', 'kmh', 'mph')
        to_unit: Целевая единица ('ms', 'kmh', 'mph')
        
    Returns:
        Конвертированное значение
    """
    if from_unit == to_unit:
        return value
    
    # Сначала конвертируем в м/с (базовая единица)
    if from_unit == 'kmh':
        value_ms = kmh_to_ms(value)
    elif from_unit == 'mph':
        value_ms = mph_to_ms(value)
    else:  # уже м/с
        value_ms = value
    
    # Затем конвертируем в целевую единицу
    if to_unit == 'ms':
        return value_ms
    elif to_unit == 'kmh':
        return ms_to_kmh(value_ms)
    elif to_unit == 'mph':
        return ms_to_mph(value_ms)
    else:
        raise ValueError(f"Unsupported wind speed unit: {to_unit}")


def convert_precipitation(value: float, from_unit: str, to_unit: str) -> float:
    """
    Универсальная конвертация осадков
    
    Args:
        value: Значение осадков
        from_unit: Исходная единица ('mm' или 'inches')
        to_unit: Целевая единица ('mm' или 'inches')
        
    Returns:
        Конвертированное значение
    """
    if from_unit == to_unit:
        return value
    
    if from_unit == 'mm' and to_unit == 'inches':
        return mm_to_inches(value)
    elif from_unit == 'inches' and to_unit == 'mm':
        return inches_to_mm(value)
    else:
        raise ValueError(f"Unsupported conversion: {from_unit} to {to_unit}")


def convert_pressure(value: float, from_unit: str, to_unit: str) -> float:
    """
    Универсальная конвертация давления
    
    Args:
        value: Значение давления
        from_unit: Исходная единица ('kpa', 'mmhg', 'inhg')
        to_unit: Целевая единица ('kpa', 'mmhg', 'inhg')
        
    Returns:
        Конвертированное значение
    """
    if from_unit == to_unit:
        return value
    
    # Конвертируем в кПа (базовая единица)
    if from_unit == 'mmhg':
        value_kpa = value / 7.50062
    elif from_unit == 'inhg':
        value_kpa = value / 0.2953
    else:  # уже кПа
        value_kpa = value
    
    # Конвертируем в целевую единицу
    if to_unit == 'kpa':
        return value_kpa
    elif to_unit == 'mmhg':
        return kpa_to_mmhg(value_kpa)
    elif to_unit == 'inhg':
        return kpa_to_inhg(value_kpa)
    else:
        raise ValueError(f"Unsupported pressure unit: {to_unit}")


def convert_result_units(result: dict, units: dict = None) -> dict:
    """
    Конвертировать все единицы в результате анализа
    
    Args:
        result: Результат analyze_weather или analyze_weather_range
        units: Словарь с желаемыми единицами:
               {
                   'temperature': 'fahrenheit',  # 'celsius' или 'fahrenheit'
                   'wind_speed': 'mph',          # 'ms', 'kmh', 'mph'
                   'precipitation': 'inches',    # 'mm' или 'inches'
                   'pressure': 'inhg'            # 'kpa', 'mmhg', 'inhg'
               }
        
    Returns:
        Результат с конвертированными единицами
    """
    if not units:
        return result  # Без конвертации
    
    result = result.copy()
    
    # Конвертация температуры
    if units.get('temperature') == 'fahrenheit' and 'statistics' in result:
        stats = result['statistics']
        if 'temperature' in stats:
            temp = stats['temperature']
            for key in ['mean', 'min', 'max', 'percentile_10', 'percentile_90']:
                if key in temp and temp[key] is not None:
                    temp[key] = celsius_to_fahrenheit(temp[key])
            temp['unit'] = '°F'
        
        if 'dew_point' in stats:
            dew = stats['dew_point']
            for key in ['mean', 'min', 'max']:
                if key in dew and dew[key] is not None:
                    dew[key] = celsius_to_fahrenheit(dew[key])
    
    # Конвертация скорости ветра
    wind_unit = units.get('wind_speed')
    if wind_unit and wind_unit != 'ms' and 'statistics' in result:
        stats = result['statistics']
        for wind_key in ['wind', 'wind_10m']:
            if wind_key in stats:
                wind = stats[wind_key]
                for key in ['mean', 'max', 'percentile_90']:
                    if key in wind and wind[key] is not None:
                        wind[key] = convert_wind_speed(wind[key], 'ms', wind_unit)
                wind['unit'] = wind_unit
    
    # Конвертация осадков
    if units.get('precipitation') == 'inches' and 'statistics' in result:
        stats = result['statistics']
        if 'precipitation' in stats:
            precip = stats['precipitation']
            for key in ['mean', 'max', 'total', 'percentile_90']:
                if key in precip and precip[key] is not None:
                    precip[key] = mm_to_inches(precip[key])
            precip['unit'] = 'inches'
    
    # Конвертация давления
    pressure_unit = units.get('pressure')
    if pressure_unit and pressure_unit != 'kpa' and 'statistics' in result:
        stats = result['statistics']
        if 'pressure' in stats:
            press = stats['pressure']
            for key in ['mean', 'min', 'max']:
                if key in press and press[key] is not None:
                    press[key] = convert_pressure(press[key], 'kpa', pressure_unit)
            press['unit'] = pressure_unit
    
    return result

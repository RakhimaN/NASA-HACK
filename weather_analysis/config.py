"""
Конфигурация для анализа погодных условий
Содержит пороговые значения и настройки
"""

class WeatherConfig:
    """Конфигурация порогов для разных погодных условий"""
    
    # === ТЕМПЕРАТУРНЫЕ ПОРОГИ (°C) ===
    TEMPERATURE_THRESHOLDS = {
        'very_cold': -10,      # Очень холодно
        'cold': 0,             # Холодно
        'cool': 10,            # Прохладно
        'comfortable': (15, 25),  # Комфортно
        'warm': 27,            # Тепло
        'hot': 32,             # Жарко
        'very_hot': 38,        # Очень жарко
        'extreme_heat': 43     # Экстремальная жара (опасно)
    }
    
    # === ОСАДКИ (mm/день) ===
    PRECIPITATION_THRESHOLDS = {
        'no_rain': 0.1,        # Нет дождя (менее 0.1мм)
        'light_rain': 2.5,     # Легкий дождь
        'moderate_rain': 10,   # Умеренный дождь
        'heavy_rain': 50,      # Сильный дождь
        'very_heavy_rain': 100 # Очень сильный дождь
    }
    
    # === ВЕТЕР (m/s) ===
    WIND_THRESHOLDS = {
        'calm': 1,             # Штиль/слабый
        'light_breeze': 3,     # Легкий бриз
        'moderate_breeze': 6,  # Умеренный бриз
        'fresh_breeze': 10,    # Свежий бриз (ощутимый)
        'strong_breeze': 15,   # Сильный бриз (может гнуть деревья)
        'gale': 20,            # Шторм (опасность)
        'storm': 25,           # Сильный шторм
        'hurricane': 33        # Ураган (очень опасно)
    }
    
    # === ВЛАЖНОСТЬ (%) ===
    HUMIDITY_THRESHOLDS = {
        'dry': 30,             # Сухо
        'comfortable': (30, 70), # Комфортно
        'humid': 80,           # Влажно
        'very_humid': 90       # Очень влажно
    }
    
    # === ОБЛАЧНОСТЬ (%) ===
    CLOUD_THRESHOLDS = {
        'clear': 25,        # Ясно
        'partly_cloudy': 50,  # Переменная облачность
        'mostly_cloudy': 75,  # Облачно
        'overcast': 90      # Пасмурно
    }
    
    # === UV ИНДЕКС ===
    UV_THRESHOLDS = {
        'low': 2,           # Низкий
        'moderate': 5,      # Умеренный
        'high': 7,          # Высокий
        'very_high': 10,    # Очень высокий
        'extreme': 11       # Экстремальный
    }
    
    # === АТМОСФЕРНОЕ ДАВЛЕНИЕ (кПа) ===
    PRESSURE_THRESHOLDS = {
        'very_low': 98.0,   # Сильный циклон
        'low': 100.0,       # Низкое (дождь вероятен)
        'normal': 101.3,    # Нормальное
        'high': 103.0,      # Высокое (ясная погода)
        'very_high': 104.5  # Антициклон
    }
    
    # === СНЕГ (см) ===
    SNOW_THRESHOLDS = {
        'no_snow': 0,
        'light_snow': 5,     # Легкий снег
        'moderate_snow': 15,  # Умеренный снег
        'heavy_snow': 30,     # Сильный снег
        'very_heavy_snow': 50 # Очень сильный снег
    }
    
    # === ВИДИМОСТЬ (км) ===
    VISIBILITY_THRESHOLDS = {
        'very_poor': 1,      # Туман
        'poor': 4,           # Плохая
        'moderate': 10,      # Умеренная
        'good': 20,          # Хорошая
        'excellent': 50      # Отличная
    }
    
    # === ОЩУЩАЕМАЯ ТЕМПЕРАТУРА (°C) ===
    APPARENT_TEMPERATURE_THRESHOLDS = {
        'extreme_cold': -20,   # Экстремально холодно (опасно)
        'very_cold': -10,      # Очень холодно
        'cold': 0,             # Холодно
        'cool': 10,            # Прохладно
        'comfortable': (15, 25),  # Комфортно
        'warm': 27,            # Тепло
        'hot': 32,             # Жарко
        'very_hot': 38,        # Очень жарко
        'extreme_heat': 43     # Экстремальная жара (опасно)
    }
    
    # === ПОРЫВЫ ВЕТРА (m/s) ===
    WIND_GUST_THRESHOLDS = {
        'calm': 5,              # Слабые порывы
        'moderate': 10,         # Умеренные порывы
        'strong': 15,           # Сильные порывы (осторожность)
        'very_strong': 20,      # Очень сильные (опасно)
        'storm': 25,            # Штормовые порывы (очень опасно)
        'hurricane': 33         # Ураганные порывы (критично)
    }
    
    # === КОДЫ ПОГОДЫ (WMO Weather Code) ===
    WEATHER_CODE_DESCRIPTION = {
        0: 'Ясно', 1: 'Преимущественно ясно', 2: 'Переменная облачность', 3: 'Пасмурно',
        45: 'Туман', 48: 'Изморозь', 51: 'Легкая морось', 53: 'Умеренная морось',
        55: 'Сильная морось', 56: 'Ледяная морось (слабая)', 57: 'Ледяная морось (сильная)',
        61: 'Слабый дождь', 63: 'Умеренный дождь', 65: 'Сильный дождь',
        66: 'Ледяной дождь (слабый)', 67: 'Ледяной дождь (сильный)',
        71: 'Слабый снег', 73: 'Умеренный снег', 75: 'Сильный снег', 77: 'Снежные зерна',
        80: 'Слабые ливни', 81: 'Умеренные ливни', 82: 'Сильные ливни',
        85: 'Слабые снежные ливни', 86: 'Сильные снежные ливни',
        95: 'Гроза', 96: 'Гроза с легким градом', 99: 'Гроза с сильным градом'
    }
    
    WEATHER_CODE_CATEGORIES = {
        'clear': [0, 1], 'cloudy': [2, 3], 'fog': [45, 48], 'drizzle': [51, 53, 55, 56, 57],
        'rain': [61, 63, 65, 66, 67, 80, 81, 82], 'snow': [71, 73, 75, 77, 85, 86],
        'thunderstorm': [95, 96, 99]
    }
    
    # === КАЧЕСТВО ВОЗДУХА (AOD - Aerosol Optical Depth) ===
    AIR_QUALITY_THRESHOLDS = {
        'excellent': 0.05, 'good': 0.15, 'moderate': 0.35, 'poor': 0.65, 'very_poor': 1.0, 'hazardous': 2.0
    }
    
    # === ПЫЛЬ В АТМОСФЕРЕ (μg/m³) ===
    DUST_THRESHOLDS = {
        'minimal': 10, 'low': 50, 'moderate': 150, 'high': 250, 'very_high': 500, 'extreme': 1000
    }
    
    # === ЧЕРНЫЙ УГЛЕРОД (μg/m³) ===
    BLACK_CARBON_THRESHOLDS = {
        'clean': 0.5, 'low': 2, 'moderate': 5, 'high': 10, 'very_high': 20, 'extreme': 50
    }
    
    # === ВЕРОЯТНОСТЬ ГРОЗЫ (CAPE - J/kg) ===
    THUNDERSTORM_THRESHOLDS = {
        'none': 0, 'very_low': 300, 'low': 1000, 'moderate': 1500, 'high': 2500, 'very_high': 3500, 'extreme': 5000
    }
    
    # === ИНДЕКС КОМФОРТА (Heat Index) ===
    COMFORT_INDEX_THRESHOLDS = {
        'comfortable': 26,
        'uncomfortable': 32,
        'very_uncomfortable': 39,
        'dangerous': 54
    }

    # === ПАРАМЕТРЫ ДАННЫХ NASA ===
    NASA_PARAMETERS = {
        'T2M': 'Температура на 2м (°C)',
        'T2M_MAX': 'Макс. температура на 2м (°C)',
        'T2M_MIN': 'Мин. температура на 2м (°C)',
        'T2MDEW': 'Температура точки росы на 2м (°C)',
        'PRECTOTCORR': 'Общие осадки (мм/день)',
        'WS2M': 'Скорость ветра на 2м (м/с)',
        'WS10M': 'Скорость ветра на 10м (м/с)',
        'RH2M': 'Относительная влажность на 2м (%)',
        'PS': 'Приземное давление (кПа)',
        'CLOUD_AMT': 'Общая облачность (%)',
        'ALLSKY_SFC_SW_DWN': 'Солнечная радиация на поверхности (Вт/м^2)',
        'ALLSKY_SFC_UV_INDEX': 'УФ-индекс',
        'QV2M': 'Удельная влажность на 2м (г/кг)',
        'SNODP': 'Глубина снега (см)',
        'EVPTRNS': 'Испарение с транспирацией (мм/день)',
        'T2MWET': 'Температура влажного термометра на 2м (°C)',
        'WS50M': 'Скорость ветра на 50м (м/с)',
        'WD2M': 'Направление ветра на 2м (°)',
        'WD10M': 'Направление ветра на 10м (°)',
    }
    
    NASA_PARAMETERS_BASIC = [
        'T2M', 'T2M_MAX', 'T2M_MIN', 'T2MDEW',
        'PRECTOTCORR', 'WS2M', 'WS10M', 'RH2M', 
        'PS', 'CLOUD_AMT', 'ALLSKY_SFC_SW_DWN'
    ]
    
    NASA_PARAMETERS_EXTENDED = [
        'T2MWET', 'WS50M', 'WD2M', 'WD10M', 'QV2M', 'PSC',
        'CLRSKY_SFC_SW_DWN', 'ALLSKY_SFC_UV_INDEX', 'CLRSKY_SFC_UV_INDEX',
        'SNODP', 'EVPTRNS'
    ]
    
    # === ПАРАМЕТРЫ OPEN-METEO API ===
    OPENMETEO_PARAMETERS = {
        'temperature_2m_max': 'Макс. температура на 2м (°C)',
        'temperature_2m_min': 'Мин. температура на 2м (°C)',
        'temperature_2m_mean': 'Средняя температура на 2м (°C)',
        'precipitation_sum': 'Общие осадки (мм/день)',
        'wind_speed_10m_max': 'Макс. скорость ветра на 10м (м/с)',
        'relative_humidity_2m_mean': 'Средняя относительная влажность на 2м (%)',
        'apparent_temperature_mean': 'Ощущаемая температура (°C)',
        'weathercode': 'Код погоды WMO',
        'windgusts_10m_max': 'Макс. порывы ветра на 10м (м/с)',
    }
    
    # === ПАРАМЕТРЫ GES DISC (NASA Earth Data) ===
    GES_DISC_PARAMETERS = {
        'AODANA': 'Оптическая толщина аэрозоля (AOD)',
        'BCSMASS': 'Концентрация черного углерода (мкг/м³)',
        'DUSMASS': 'Концентрация пыли (мкг/м³)',
        'SO2SMASS': 'Концентрация диоксида серы (мкг/м³)',
        'SO4SMASS': 'Концентрация сульфатов (мкг/м³)',
        'SSSMASS': 'Концентрация морской соли (мкг/м³)',
        'TO3': 'Общий озон (добсон)',
        'TROPO3': 'Тропосферный озон (добсон)',
    }
    
    # === ПАРАМЕТРЫ CPTEC (Brazilian Weather) ===
    CPTEC_PARAMETERS = {
        'cape': 'CAPE - энергия конвекции (J/kg)',
        'cin': 'CIN - конвективное торможение (J/kg)',
        'lifted_index': 'Индекс неустойчивости атмосферы (°C)',
    }
    
    # === НАСТРОЙКИ АНАЛИЗА ===
    ANALYSIS_CONFIG = {
        'min_years_data': 10,  # Минимум лет для надежной статистики
        'default_years': 30,   # По умолчанию берем 30 лет
        'cache_enabled': True,
        'cache_ttl_days': 30   # Кэш на 30 дней
    }
    
    # === МУЛЬТИСОРСНЫЙ ПОДХОД ===
    DATA_SOURCES = {
        'nasa_power': {
            'name': 'NASA POWER', 'priority': 1, 'reliability': 0.95, 'coverage': 'global', 'parameters': 'NASA_PARAMETERS'
        },
        'openmeteo': {
            'name': 'Open-Meteo', 'priority': 2, 'reliability': 0.90, 'coverage': 'global', 'parameters': 'OPENMETEO_PARAMETERS'
        },
        'ges_disc': {
            'name': 'GES DISC', 'priority': 1, 'reliability': 0.95, 'coverage': 'global', 'parameters': 'GES_DISC_PARAMETERS'
        },
        'cptec': {
            'name': 'Brazilian CPTEC', 'priority': 3, 'reliability': 0.85, 'coverage': 'south_america', 'parameters': 'CPTEC_PARAMETERS'
        }
    }
    
    CONSENSUS_CONFIG = {
        'min_sources': 2, 'outlier_threshold': 0.20,
        'confidence_levels': {
            'high': 0.90, 'medium': 0.75, 'low': 0.50
        }
    }
    
    # === КАТЕГОРИИ ДЛЯ ВЫВОДА ===
    WEATHER_CATEGORIES = [
        'very_cold', 'cold', 'comfortable', 'hot', 'very_hot', 'very_wet', 'very_windy', 'very_uncomfortable',
        'extreme_heat', 'extreme_cold', 'storm_warning', 'air_quality_poor', 'thunderstorm_likely', 'dust_storm', 'high_pollution'
    ]
    
    @staticmethod
    def calculate_heat_index(temperature_c, humidity_percent):
        # Steadman (1984) simplified formula, good for general use
        # More complex formulas exist, but this is a good approximation
        if temperature_c < 27.0 or humidity_percent < 40:
            return temperature_c # Heat index not applicable

        # Simplified version of Rothfusz (1990) multiple regression equation
        # This formula is for Fahrenheit, convert to Celsius at the end.
        temp_f = (temperature_c * 9/5) + 32
        rh = humidity_percent

        hi_f = -42.379 + 2.04901523*temp_f + 10.14333127*rh - .22475541*temp_f*rh - .00683783*temp_f*temp_f - \
               .05481717*rh*rh + .00122874*temp_f*temp_f*rh + .00085282*temp_f*rh*rh - .00000199*temp_f*temp_f*rh*rh

        return (hi_f - 32) * 5/9 # Convert back to Celsius
    
    @staticmethod
    def calculate_wind_chill(temperature_c, wind_speed_ms):
        # Применяется только при температуре ниже 10°C и скорости ветра выше 1.3 м/с
        if temperature_c > 10 or wind_speed_ms < 1.3:
            return temperature_c
        
        # Формула Wind Chill Index (US National Weather Service) - для °C и км/ч
        wind_speed_kmh = wind_speed_ms * 3.6
        
        wci_c = 13.12 + 0.6215 * temperature_c - 11.37 * (wind_speed_kmh**0.16) + 
                0.3965 * temperature_c * (wind_speed_kmh**0.16)
        
        return wci_c
    
    @staticmethod
    def calculate_apparent_temperature(temperature_c, humidity_percent, wind_speed_ms, solar_radiation=None):
        # Australian Apparent Temperature (AT) formula
        # AT = Ta + 0.33*e - 0.70*ws - 4.00
        # where Ta = dry bulb temperature (°C), e = water vapour pressure (hPa), ws = wind speed (m/s)

        # Convert humidity to vapor pressure (e) in hPa
        # Saturation vapor pressure (es) in hPa
        es = 6.112 * (2.71828**((17.67 * temperature_c) / (temperature_c + 243.5)))
        e = (humidity_percent / 100) * es
        
        # Add solar radiation effect if provided (simplified, needs more detailed model for accuracy)
        solar_effect = 0
        if solar_radiation is not None and solar_radiation > 100: # Arbitrary threshold for significant solar impact
            solar_effect = 0.04 * (solar_radiation / 100) # Simple linear scaling

        at = temperature_c + (0.33 * e) - (0.70 * wind_speed_ms) - 4.00 + solar_effect
        return at
    
    @staticmethod
    def get_weather_code_category(weather_code: int) -> str:
        for category, codes in WeatherConfig.WEATHER_CODE_CATEGORIES.items():
            if weather_code in codes:
                return category
        return 'unknown'
    
    @staticmethod
    def get_air_quality_level(aod_value: float) -> str:
        thresholds = WeatherConfig.AIR_QUALITY_THRESHOLDS
        if aod_value < thresholds['excellent']:
            return 'excellent'
        elif aod_value < thresholds['good']:
            return 'good'
        elif aod_value < thresholds['moderate']:
            return 'moderate'
        elif aod_value < thresholds['poor']:
            return 'poor'
        elif aod_value < thresholds['very_poor']:
            return 'very_poor'
        else:
            return 'hazardous'
    
    @staticmethod
    def get_thunderstorm_risk(cape_value: float) -> str:
        thresholds = WeatherConfig.THUNDERSTORM_THRESHOLDS
        if cape_value < thresholds['none']:
            return 'none'
        elif cape_value < thresholds['very_low']:
            return 'very_low'
        elif cape_value < thresholds['low']:
            return 'low'
        elif cape_value < thresholds['moderate']:
            return 'moderate'
        elif cape_value < thresholds['high']:
            return 'high'
        elif cape_value < thresholds['very_high']:
            return 'very_high'
        else:
            return 'extreme'
    
    @staticmethod
    def get_weather_description(weather_code: int) -> str:
        return WeatherConfig.WEATHER_CODE_DESCRIPTION.get(weather_code, 'Неизвестно')

config = WeatherConfig()

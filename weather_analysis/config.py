"""
Конфигурация для анализа погодных условий
Содержит пороговые значения и настройки
"""

class WeatherConfig:
    """Конфигурация порогов для разных погодных условий"""
    
    # === ТЕМПЕРАТУРНЫЕ ПОРОГИ (°C) ===
    # Используем относительные пороги (перцентили) + абсолютные пороги
    
    TEMPERATURE_THRESHOLDS = {
        'very_cold': {
            'percentile': 10,  # 10-й перцентиль для локации
            'absolute_min': -10  # Абсолютный минимум (для любого региона)
        },
        'cold': {
            'percentile': 25,
            'absolute_max': 10
        },
        'hot': {
            'percentile': 75,
            'absolute_min': 25
        },
        'very_hot': {
            'percentile': 90,  # 90-й перцентиль для локации
            'absolute_min': 30  # Абсолютный минимум (для жарких регионов выше)
        }
    }
    
    # === ОСАДКИ (mm/день) ===
    PRECIPITATION_THRESHOLDS = {
        'very_dry': 0.1,      # Почти нет осадков
        'light_rain': 2.5,    # Легкий дождь
        'moderate_rain': 10,  # Умеренный дождь
        'heavy_rain': 50,     # Сильный дождь
        'very_wet': 100       # Очень сильные осадки
    }
    
    # === ВЕТЕР (m/s) ===
    WIND_THRESHOLDS = {
        'calm': 2,           # Штиль
        'light_breeze': 5,   # Легкий ветер
        'moderate_wind': 10, # Умеренный ветер
        'strong_wind': 15,   # Сильный ветер
        'very_windy': 20     # Очень ветрено (шторм)
    }
    
    # === ВЛАЖНОСТЬ (%) ===
    HUMIDITY_THRESHOLDS = {
        'very_dry': 30,
        'comfortable': 60,
        'humid': 80,
        'very_humid': 90
    }
    
    # === КАЧЕСТВО ВОЗДУХА (AOD - Aerosol Optical Depth) ===
    AIR_QUALITY_THRESHOLDS = {
        'excellent': 0.05,      # Отличное (чистый воздух)
        'good': 0.15,           # Хорошее
        'moderate': 0.35,       # Умеренное
        'poor': 0.65,           # Плохое
        'very_poor': 1.0,       # Очень плохое
        'hazardous': 2.0        # Опасное (смог, дым от пожаров)
    }
    
    # === ПЫЛЬ В АТМОСФЕРЕ (μg/m³) ===
    DUST_THRESHOLDS = {
        'minimal': 10,          # Минимальная концентрация
        'low': 50,              # Низкая
        'moderate': 150,        # Умеренная
        'high': 250,            # Высокая (песчаная буря)
        'very_high': 500,       # Очень высокая (опасно)
        'extreme': 1000         # Экстремальная (критично)
    }
    
    # === ЧЕРНЫЙ УГЛЕРОД (μg/m³) ===
    BLACK_CARBON_THRESHOLDS = {
        'clean': 0.5,           # Чистый воздух
        'low': 2,               # Низкое загрязнение
        'moderate': 5,          # Умеренное
        'high': 10,             # Высокое (транспорт, промышленность)
        'very_high': 20,        # Очень высокое
        'extreme': 50           # Экстремальное (опасно для здоровья)
    }
    
    # === ВЕРОЯТНОСТЬ ГРОЗЫ (CAPE - J/kg) ===
    THUNDERSTORM_THRESHOLDS = {
        'none': 0,              # Гроза невозможна
        'very_low': 300,        # Очень низкая вероятность
        'low': 1000,            # Низкая вероятность
        'moderate': 1500,       # Умеренная вероятность
        'high': 2500,           # Высокая вероятность (возможны грозы)
        'very_high': 3500,      # Очень высокая (сильные грозы)
        'extreme': 5000         # Экстремальная (суперячейки, торнадо)
    }
    
    # === ИНДЕКС КОМФОРТА (Heat Index) ===
    # Учитывает температуру + влажность
    COMFORT_INDEX = {
        'comfortable': {
            'temp_range': (15, 25),  # °C
            'humidity_max': 70
        },
        'uncomfortable_hot': {
            'temp_min': 27,
            'humidity_min': 40  # Жарко + влажно = дискомфорт
        },
        'uncomfortable_cold': {
            'temp_max': 5,
            'wind_min': 5  # Холодно + ветер = дискомфорт
        },
        'very_uncomfortable': {
            'heat_index_min': 40  # Heat index > 40°C = опасно
        }
    }
    
    # === ПАРАМЕТРЫ ДАННЫХ NASA ===
    NASA_PARAMETERS = {
        'T2M': 'Температура на высоте 2м (°C)',
        'T2M_MAX': 'Максимальная температура (°C)',
        'T2M_MIN': 'Минимальная температура (°C)',
        'PRECTOTCORR': 'Осадки (мм/день)',
        'WS2M': 'Скорость ветра на высоте 2м (м/с)',
        'RH2M': 'Относительная влажность (%)',
        'PS': 'Атмосферное давление (кПа)',
        'CLOUD_AMT': 'Облачность (%)',
    }
    
    # === НАСТРОЙКИ АНАЛИЗА ===
    ANALYSIS_CONFIG = {
        'min_years_data': 10,  # Минимум лет для надежной статистики
        'default_years': 30,   # По умолчанию берем 30 лет
        'cache_enabled': True,
        'cache_ttl_days': 30   # Кэш на 30 дней
    }
    
    # === КАТЕГОРИИ ДЛЯ ВЫВОДА ===
    WEATHER_CATEGORIES = [
        'very_cold',
        'cold', 
        'comfortable',
        'hot',
        'very_hot',
        'very_wet',
        'very_windy',
        'very_uncomfortable'
    ]
    
    @staticmethod
    def calculate_heat_index(temperature_c, humidity_percent):
        """
        Рассчитать Heat Index (индекс жары)
        Учитывает как температуру, так и влажность
        
        Args:
            temperature_c: Температура в градусах Цельсия
            humidity_percent: Относительная влажность в процентах
            
        Returns:
            Heat index в градусах Цельсия
        """
        # Конвертируем в Фаренгейты для формулы
        T = temperature_c * 9/5 + 32
        RH = humidity_percent
        
        # Упрощенная формула Heat Index (работает при T > 80°F)
        if T < 80:
            return temperature_c
        
        HI = -42.379 + 2.04901523*T + 10.14333127*RH - 0.22475541*T*RH
        HI += -0.00683783*T*T - 0.05481717*RH*RH + 0.00122874*T*T*RH
        HI += 0.00085282*T*RH*RH - 0.00000199*T*T*RH*RH
        
        # Конвертируем обратно в Цельсий
        return (HI - 32) * 5/9
    
    @staticmethod
    def calculate_wind_chill(temperature_c, wind_speed_ms):
        """
        Рассчитать Wind Chill (ощущаемая температура с учетом ветра)
        
        Args:
            temperature_c: Температура в градусах Цельсия
            wind_speed_ms: Скорость ветра в м/с
            
        Returns:
            Wind chill в градусах Цельсия
        """
        # Конвертируем скорость ветра в км/ч
        V = wind_speed_ms * 3.6
        T = temperature_c
        
        # Формула работает при T <= 10°C и V >= 4.8 км/ч
        if T > 10 or V < 4.8:
            return temperature_c
        
        # Формула Wind Chill
        WC = 13.12 + 0.6215*T - 11.37*(V**0.16) + 0.3965*T*(V**0.16)
        
        return WC


# Для удобства импорта создаем экземпляр
config = WeatherConfig()

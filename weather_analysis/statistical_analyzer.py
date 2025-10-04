"""
Статистический анализ погодных данных v2.0
Расчет вероятностей различных погодных условий

🆕 v2.0 Новые методы анализа:
    - _analyze_apparent_temperature() - ощущаемая температура (7 категорий)
    - _analyze_weather_conditions() - коды погоды WMO (6 категорий)
    - _analyze_wind_gusts() - порывы ветра (6 категорий от штиля до урагана)
    - _analyze_air_quality() - качество воздуха (AOD, черный углерод, пыль)
    - _analyze_thunderstorm_risk() - риск грозы по CAPE (7 уровней)

Всего: 13 методов анализа, 30+ вероятностей, 20+ статистик
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from .config import WeatherConfig


class StatisticalAnalyzer:
    """
    Анализатор для расчета вероятностей погодных условий
    на основе исторических данных
    """
    
    def __init__(self, config: WeatherConfig = None):
        self.config = config or WeatherConfig()
    
    def analyze_day(self, data: pd.DataFrame, day_of_year: int, 
                    latitude: float = None) -> Dict:
        """
        Анализировать погодные условия для конкретного дня года
        
        Args:
            data: DataFrame с историческими данными (должен содержать day_of_year)
            day_of_year: День года (1-365)
            latitude: Широта (опционально, для корректировки порогов)
            
        Returns:
            Словарь с вероятностями и статистикой
        """
        # Фильтруем данные для этого дня
        day_data = data[data['day_of_year'] == day_of_year].copy()
        
        if len(day_data) == 0:
            return {
                'error': f'Нет данных для дня {day_of_year}',
                'day_of_year': day_of_year
            }
        
        # Рассчитываем вероятности
        probabilities = {}
        
        # === ТЕМПЕРАТУРНЫЕ УСЛОВИЯ ===
        if 'T2M_MAX' in day_data.columns and 'T2M_MIN' in day_data.columns:
            temp_probs = self._analyze_temperature(day_data, latitude)
            probabilities.update(temp_probs)
        
        # === ОСАДКИ ===
        if 'PRECTOTCORR' in day_data.columns:
            precip_probs = self._analyze_precipitation(day_data)
            probabilities.update(precip_probs)
        
        # === ВЕТЕР ===
        if 'WS2M' in day_data.columns:
            wind_probs = self._analyze_wind(day_data)
            probabilities.update(wind_probs)
        
        # === ИНДЕКС КОМФОРТА ===
        if 'T2M' in day_data.columns and 'RH2M' in day_data.columns:
            comfort_probs = self._analyze_comfort(day_data)
            probabilities.update(comfort_probs)
        
        # === ОБЛАЧНОСТЬ ===
        if 'CLOUD_AMT' in day_data.columns:
            cloud_probs = self._analyze_cloudiness(day_data)
            probabilities.update(cloud_probs)
        
        # === UV ИНДЕКС ===
        if 'ALLSKY_SFC_UV_INDEX' in day_data.columns:
            uv_probs = self._analyze_uv_index(day_data)
            probabilities.update(uv_probs)
        
        # === АТМОСФЕРНОЕ ДАВЛЕНИЕ ===
        if 'PS' in day_data.columns:
            pressure_probs = self._analyze_pressure(day_data)
            probabilities.update(pressure_probs)
        
        # === СНЕГ (для зимних месяцев) ===
        if 'SNODP' in day_data.columns:
            snow_probs = self._analyze_snow(day_data)
            probabilities.update(snow_probs)
        
        # === ОЩУЩАЕМАЯ ТЕМПЕРАТУРА ===
        apparent_temp_probs = self._analyze_apparent_temperature(day_data)
        if apparent_temp_probs:
            probabilities.update(apparent_temp_probs)
        
        # === ПОГОДНЫЕ УСЛОВИЯ (WMO коды) ===
        weather_probs = self._analyze_weather_conditions(day_data)
        if weather_probs:
            probabilities.update(weather_probs)
        
        # === ПОРЫВЫ ВЕТРА ===
        gust_probs = self._analyze_wind_gusts(day_data)
        if gust_probs:
            probabilities.update(gust_probs)
        
        # === КАЧЕСТВО ВОЗДУХА ===
        air_quality_probs = self._analyze_air_quality(day_data)
        if air_quality_probs:
            probabilities.update(air_quality_probs)
        
        # === РИСК ГРОЗЫ ===
        thunderstorm_probs = self._analyze_thunderstorm_risk(day_data)
        if thunderstorm_probs:
            probabilities.update(thunderstorm_probs)
        
        # === СТАТИСТИКА ===
        statistics = self._calculate_statistics(day_data)
        
        return {
            'day_of_year': day_of_year,
            'date_example': self._day_to_date_string(day_of_year),
            'probabilities': probabilities,
            'statistics': statistics,
            'data_points': len(day_data)
        }
    
    def _analyze_temperature(self, day_data: pd.DataFrame, 
                            latitude: Optional[float] = None) -> Dict:
        """Анализ температурных условий"""
        probabilities = {}
        
        # Используем относительные пороги (перцентили)
        temp_max = day_data['T2M_MAX']
        temp_min = day_data['T2M_MIN']
        
        # Очень жарко (>90-й перцентиль ИЛИ >30°C)
        percentile_90 = temp_max.quantile(0.90)
        very_hot_threshold = max(percentile_90, 
                                self.config.TEMPERATURE_THRESHOLDS['very_hot']['absolute_min'])
        probabilities['very_hot'] = (temp_max > very_hot_threshold).mean()
        
        # Жарко (>75-й перцентиль ИЛИ >25°C)
        percentile_75 = temp_max.quantile(0.75)
        hot_threshold = max(percentile_75,
                           self.config.TEMPERATURE_THRESHOLDS['hot']['absolute_min'])
        probabilities['hot'] = (temp_max > hot_threshold).mean()
        
        # Очень холодно (<10-й перцентиль ИЛИ <-10°C)
        percentile_10 = temp_min.quantile(0.10)
        very_cold_threshold = min(percentile_10,
                                 self.config.TEMPERATURE_THRESHOLDS['very_cold']['absolute_min'])
        probabilities['very_cold'] = (temp_min < very_cold_threshold).mean()
        
        # Холодно (<25-й перцентиль ИЛИ <10°C)
        percentile_25 = temp_min.quantile(0.25)
        cold_threshold = min(percentile_25,
                            self.config.TEMPERATURE_THRESHOLDS['cold']['absolute_max'])
        probabilities['cold'] = (temp_min < cold_threshold).mean()
        
        # Комфортная температура (15-25°C)
        if 'T2M' in day_data.columns:
            temp_mean = day_data['T2M']
            comfortable_temp = (temp_mean >= 15) & (temp_mean <= 25)
            probabilities['comfortable_temperature'] = comfortable_temp.mean()
        
        return probabilities
    
    def _analyze_precipitation(self, day_data: pd.DataFrame) -> Dict:
        """Анализ осадков"""
        probabilities = {}
        
        precip = day_data['PRECTOTCORR']
        
        # Очень влажно (сильные осадки)
        very_wet = precip > self.config.PRECIPITATION_THRESHOLDS['very_wet']
        probabilities['very_wet'] = very_wet.mean()
        
        # Сильный дождь
        heavy_rain = precip > self.config.PRECIPITATION_THRESHOLDS['heavy_rain']
        probabilities['heavy_rain'] = heavy_rain.mean()
        
        # Умеренный дождь
        moderate_rain = (precip > self.config.PRECIPITATION_THRESHOLDS['moderate_rain']) & \
                       (precip <= self.config.PRECIPITATION_THRESHOLDS['heavy_rain'])
        probabilities['moderate_rain'] = moderate_rain.mean()
        
        # Легкий дождь
        light_rain = (precip > self.config.PRECIPITATION_THRESHOLDS['light_rain']) & \
                    (precip <= self.config.PRECIPITATION_THRESHOLDS['moderate_rain'])
        probabilities['light_rain'] = light_rain.mean()
        
        # Сухо
        dry = precip < self.config.PRECIPITATION_THRESHOLDS['very_dry']
        probabilities['dry'] = dry.mean()
        
        return probabilities
    
    def _analyze_wind(self, day_data: pd.DataFrame) -> Dict:
        """Анализ ветра"""
        probabilities = {}
        
        wind = day_data['WS2M']
        
        # Очень ветрено
        very_windy = wind > self.config.WIND_THRESHOLDS['very_windy']
        probabilities['very_windy'] = very_windy.mean()
        
        # Сильный ветер
        strong_wind = (wind > self.config.WIND_THRESHOLDS['strong_wind']) & \
                     (wind <= self.config.WIND_THRESHOLDS['very_windy'])
        probabilities['strong_wind'] = strong_wind.mean()
        
        # Умеренный ветер
        moderate_wind = (wind > self.config.WIND_THRESHOLDS['moderate_wind']) & \
                       (wind <= self.config.WIND_THRESHOLDS['strong_wind'])
        probabilities['moderate_wind'] = moderate_wind.mean()
        
        # Штиль
        calm = wind < self.config.WIND_THRESHOLDS['calm']
        probabilities['calm'] = calm.mean()
        
        return probabilities
    
    def _analyze_comfort(self, day_data: pd.DataFrame) -> Dict:
        """Анализ индекса комфорта (учитывает температуру + влажность)"""
        probabilities = {}
        
        temp = day_data['T2M']
        humidity = day_data['RH2M']
        
        # Рассчитываем Heat Index для каждой строки
        heat_indices = []
        for t, h in zip(temp, humidity):
            hi = self.config.calculate_heat_index(t, h)
            heat_indices.append(hi)
        
        day_data = day_data.copy()
        day_data['heat_index'] = heat_indices
        
        # Очень некомфортно (высокий heat index)
        very_uncomfortable = day_data['heat_index'] > \
            self.config.COMFORT_INDEX['very_uncomfortable']['heat_index_min']
        probabilities['very_uncomfortable'] = very_uncomfortable.mean()
        
        # Некомфортно жарко (жарко + влажно)
        uncomfortable_hot = (
            (temp > self.config.COMFORT_INDEX['uncomfortable_hot']['temp_min']) &
            (humidity > self.config.COMFORT_INDEX['uncomfortable_hot']['humidity_min'])
        )
        probabilities['uncomfortable_hot'] = uncomfortable_hot.mean()
        
        # Комфортно
        temp_range = self.config.COMFORT_INDEX['comfortable']['temp_range']
        humidity_max = self.config.COMFORT_INDEX['comfortable']['humidity_max']
        
        comfortable = (
            (temp >= temp_range[0]) &
            (temp <= temp_range[1]) &
            (humidity <= humidity_max)
        )
        probabilities['comfortable'] = comfortable.mean()
        
        return probabilities
    
    def _analyze_cloudiness(self, day_data: pd.DataFrame) -> Dict:
        """Анализ облачности"""
        probabilities = {}
        
        cloud = day_data['CLOUD_AMT']
        
        # Ясно
        clear = cloud < self.config.CLOUD_THRESHOLDS['clear']
        probabilities['clear'] = clear.mean()
        
        # Переменная облачность
        partly_cloudy = (cloud >= self.config.CLOUD_THRESHOLDS['clear']) & \
                       (cloud < self.config.CLOUD_THRESHOLDS['partly_cloudy'])
        probabilities['partly_cloudy'] = partly_cloudy.mean()
        
        # Облачно
        mostly_cloudy = (cloud >= self.config.CLOUD_THRESHOLDS['partly_cloudy']) & \
                       (cloud < self.config.CLOUD_THRESHOLDS['mostly_cloudy'])
        probabilities['mostly_cloudy'] = mostly_cloudy.mean()
        
        # Пасмурно
        overcast = cloud >= self.config.CLOUD_THRESHOLDS['overcast']
        probabilities['overcast'] = overcast.mean()
        
        return probabilities
    
    def _analyze_uv_index(self, day_data: pd.DataFrame) -> Dict:
        """Анализ UV индекса"""
        probabilities = {}
        
        uv = day_data['ALLSKY_SFC_UV_INDEX']
        
        # Низкий UV
        low_uv = uv < self.config.UV_THRESHOLDS['low']
        probabilities['low_uv'] = low_uv.mean()
        
        # Умеренный UV
        moderate_uv = (uv >= self.config.UV_THRESHOLDS['low']) & \
                     (uv < self.config.UV_THRESHOLDS['moderate'])
        probabilities['moderate_uv'] = moderate_uv.mean()
        
        # Высокий UV
        high_uv = (uv >= self.config.UV_THRESHOLDS['moderate']) & \
                 (uv < self.config.UV_THRESHOLDS['high'])
        probabilities['high_uv'] = high_uv.mean()
        
        # Очень высокий UV
        very_high_uv = (uv >= self.config.UV_THRESHOLDS['high']) & \
                      (uv < self.config.UV_THRESHOLDS['very_high'])
        probabilities['very_high_uv'] = very_high_uv.mean()
        
        # Экстремальный UV
        extreme_uv = uv >= self.config.UV_THRESHOLDS['extreme']
        probabilities['extreme_uv'] = extreme_uv.mean()
        
        return probabilities
    
    def _analyze_pressure(self, day_data: pd.DataFrame) -> Dict:
        """Анализ атмосферного давления"""
        probabilities = {}
        
        pressure = day_data['PS']
        
        # Очень низкое (циклон)
        very_low_pressure = pressure < self.config.PRESSURE_THRESHOLDS['very_low']
        probabilities['very_low_pressure'] = very_low_pressure.mean()
        
        # Низкое (дождь вероятен)
        low_pressure = (pressure >= self.config.PRESSURE_THRESHOLDS['very_low']) & \
                      (pressure < self.config.PRESSURE_THRESHOLDS['low'])
        probabilities['low_pressure'] = low_pressure.mean()
        
        # Нормальное
        normal_pressure = (pressure >= self.config.PRESSURE_THRESHOLDS['low']) & \
                         (pressure < self.config.PRESSURE_THRESHOLDS['high'])
        probabilities['normal_pressure'] = normal_pressure.mean()
        
        # Высокое (ясная погода)
        high_pressure = (pressure >= self.config.PRESSURE_THRESHOLDS['high']) & \
                       (pressure < self.config.PRESSURE_THRESHOLDS['very_high'])
        probabilities['high_pressure'] = high_pressure.mean()
        
        # Очень высокое (антициклон)
        very_high_pressure = pressure >= self.config.PRESSURE_THRESHOLDS['very_high']
        probabilities['very_high_pressure'] = very_high_pressure.mean()
        
        return probabilities
    
    def _analyze_snow(self, day_data: pd.DataFrame) -> Dict:
        """Анализ снежного покрова"""
        probabilities = {}
        
        snow = day_data['SNODP']
        
        # Нет снега
        no_snow = snow <= self.config.SNOW_THRESHOLDS['no_snow']
        probabilities['no_snow'] = no_snow.mean()
        
        # Легкий снег
        light_snow = (snow > self.config.SNOW_THRESHOLDS['no_snow']) & \
                    (snow <= self.config.SNOW_THRESHOLDS['light_snow'])
        probabilities['light_snow'] = light_snow.mean()
        
        # Умеренный снег
        moderate_snow = (snow > self.config.SNOW_THRESHOLDS['light_snow']) & \
                       (snow <= self.config.SNOW_THRESHOLDS['moderate_snow'])
        probabilities['moderate_snow'] = moderate_snow.mean()
        
        # Сильный снег
        heavy_snow = (snow > self.config.SNOW_THRESHOLDS['moderate_snow']) & \
                    (snow <= self.config.SNOW_THRESHOLDS['heavy_snow'])
        probabilities['heavy_snow'] = heavy_snow.mean()
        
        # Очень сильный снег
        very_heavy_snow = snow > self.config.SNOW_THRESHOLDS['very_heavy_snow']
        probabilities['very_heavy_snow'] = very_heavy_snow.mean()
        
        return probabilities
    
    def _calculate_statistics(self, day_data: pd.DataFrame) -> Dict:
        """Рассчитать базовую статистику для всех параметров"""
        statistics = {}
        
        # Температура
        if 'T2M' in day_data.columns:
            statistics['temperature'] = {
                'mean': float(day_data['T2M'].mean()),
                'min': float(day_data['T2M_MIN'].min()) if 'T2M_MIN' in day_data.columns else None,
                'max': float(day_data['T2M_MAX'].max()) if 'T2M_MAX' in day_data.columns else None,
                'std': float(day_data['T2M'].std()),
                'percentile_10': float(day_data['T2M'].quantile(0.10)),
                'percentile_90': float(day_data['T2M'].quantile(0.90))
            }
        
        # Осадки
        if 'PRECTOTCORR' in day_data.columns:
            statistics['precipitation'] = {
                'mean': float(day_data['PRECTOTCORR'].mean()),
                'max': float(day_data['PRECTOTCORR'].max()),
                'std': float(day_data['PRECTOTCORR'].std()),
                'percentile_90': float(day_data['PRECTOTCORR'].quantile(0.90))
            }
        
        # Ветер
        if 'WS2M' in day_data.columns:
            statistics['wind'] = {
                'mean': float(day_data['WS2M'].mean()),
                'max': float(day_data['WS2M'].max()),
                'std': float(day_data['WS2M'].std()),
                'percentile_90': float(day_data['WS2M'].quantile(0.90))
            }
        
        # Влажность
        if 'RH2M' in day_data.columns:
            statistics['humidity'] = {
                'mean': float(day_data['RH2M'].mean()),
                'min': float(day_data['RH2M'].min()),
                'max': float(day_data['RH2M'].max()),
                'std': float(day_data['RH2M'].std())
            }
        
        # Точка росы
        if 'T2MDEW' in day_data.columns:
            statistics['dew_point'] = {
                'mean': float(day_data['T2MDEW'].mean()),
                'min': float(day_data['T2MDEW'].min()),
                'max': float(day_data['T2MDEW'].max())
            }
        
        # Облачность
        if 'CLOUD_AMT' in day_data.columns:
            statistics['cloudiness'] = {
                'mean': float(day_data['CLOUD_AMT'].mean()),
                'min': float(day_data['CLOUD_AMT'].min()),
                'max': float(day_data['CLOUD_AMT'].max())
            }
        
        # UV индекс
        if 'ALLSKY_SFC_UV_INDEX' in day_data.columns:
            statistics['uv_index'] = {
                'mean': float(day_data['ALLSKY_SFC_UV_INDEX'].mean()),
                'max': float(day_data['ALLSKY_SFC_UV_INDEX'].max()),
                'percentile_90': float(day_data['ALLSKY_SFC_UV_INDEX'].quantile(0.90))
            }
        
        # Солнечная радиация
        if 'ALLSKY_SFC_SW_DWN' in day_data.columns:
            statistics['solar_radiation'] = {
                'mean': float(day_data['ALLSKY_SFC_SW_DWN'].mean()),
                'max': float(day_data['ALLSKY_SFC_SW_DWN'].max())
            }
        
        # Атмосферное давление
        if 'PS' in day_data.columns:
            statistics['pressure'] = {
                'mean': float(day_data['PS'].mean()),
                'min': float(day_data['PS'].min()),
                'max': float(day_data['PS'].max()),
                'std': float(day_data['PS'].std())
            }
        
        # Снег
        if 'SNODP' in day_data.columns:
            statistics['snow'] = {
                'mean': float(day_data['SNODP'].mean()),
                'max': float(day_data['SNODP'].max()),
                'days_with_snow': int((day_data['SNODP'] > 0).sum())
            }
        
        # Ветер на 10м
        if 'WS10M' in day_data.columns:
            statistics['wind_10m'] = {
                'mean': float(day_data['WS10M'].mean()),
                'max': float(day_data['WS10M'].max())
            }
        
        # Ощущаемая температура
        if 'apparent_temperature_mean' in day_data.columns:
            statistics['apparent_temperature'] = {
                'mean': float(day_data['apparent_temperature_mean'].mean()),
                'min': float(day_data['apparent_temperature_mean'].min()),
                'max': float(day_data['apparent_temperature_mean'].max()),
                'std': float(day_data['apparent_temperature_mean'].std())
            }
        
        # Порывы ветра
        if 'windgusts_10m_max' in day_data.columns:
            statistics['wind_gusts'] = {
                'mean': float(day_data['windgusts_10m_max'].mean()),
                'max': float(day_data['windgusts_10m_max'].max()),
                'percentile_90': float(day_data['windgusts_10m_max'].quantile(0.90))
            }
        
        # Код погоды (наиболее частый)
        if 'weathercode' in day_data.columns:
            most_common_code = int(day_data['weathercode'].mode()[0]) if len(day_data['weathercode'].mode()) > 0 else None
            if most_common_code is not None:
                statistics['weather_code'] = {
                    'most_common': most_common_code,
                    'description': self.config.WEATHER_CODE_DESCRIPTION.get(most_common_code, 'Unknown'),
                    'category': self.config.get_weather_code_category(most_common_code)
                }
        
        # Качество воздуха (AOD)
        if 'AODANA' in day_data.columns:
            aod_mean = float(day_data['AODANA'].mean())
            statistics['air_quality'] = {
                'aod_mean': aod_mean,
                'aod_max': float(day_data['AODANA'].max()),
                'level': self.config.get_air_quality_level(aod_mean)
            }
        
        # Черный углерод
        if 'BCSMASS' in day_data.columns:
            statistics['black_carbon'] = {
                'mean': float(day_data['BCSMASS'].mean()),
                'max': float(day_data['BCSMASS'].max()),
                'percentile_90': float(day_data['BCSMASS'].quantile(0.90))
            }
        
        # Пыль
        if 'DUSMASS' in day_data.columns:
            statistics['dust'] = {
                'mean': float(day_data['DUSMASS'].mean()),
                'max': float(day_data['DUSMASS'].max()),
                'percentile_90': float(day_data['DUSMASS'].quantile(0.90))
            }
        
        # Грозовая активность (CAPE)
        if 'cape' in day_data.columns:
            cape_mean = float(day_data['cape'].mean())
            statistics['thunderstorm'] = {
                'cape_mean': cape_mean,
                'cape_max': float(day_data['cape'].max()),
                'risk_level': self.config.get_thunderstorm_risk(cape_mean)
            }
        
        return statistics
    
    def analyze_date_range(self, data: pd.DataFrame, start_day: int, 
                          end_day: int, latitude: float = None) -> List[Dict]:
        """
        Анализировать диапазон дней
        
        Args:
            data: DataFrame с историческими данными
            start_day: Начальный день года (1-365)
            end_day: Конечный день года (1-365)
            latitude: Широта
            
        Returns:
            Список результатов анализа для каждого дня
        """
        results = []
        
        for day in range(start_day, end_day + 1):
            result = self.analyze_day(data, day, latitude)
            results.append(result)
        
        return results
    
    def get_summary_probabilities(self, data: pd.DataFrame, day_of_year: int,
                                  latitude: float = None) -> Dict:
        """
        Получить упрощенные вероятности (только главные категории)
        для удобного отображения пользователю
        """
        full_analysis = self.analyze_day(data, day_of_year, latitude)
        
        if 'error' in full_analysis:
            return full_analysis
        
        probs = full_analysis['probabilities']
        
        # Возвращаем только ключевые категории
        summary = {
            'very_cold': probs.get('very_cold', 0.0),
            'cold': probs.get('cold', 0.0),
            'comfortable': probs.get('comfortable', 0.0),
            'hot': probs.get('hot', 0.0),
            'very_hot': probs.get('very_hot', 0.0),
            'very_wet': probs.get('very_wet', 0.0),
            'very_windy': probs.get('very_windy', 0.0),
            'very_uncomfortable': probs.get('very_uncomfortable', 0.0)
        }
        
        return {
            'day_of_year': day_of_year,
            'date_example': full_analysis['date_example'],
            'probabilities': summary,
            'statistics': full_analysis['statistics']
        }
    
    def _analyze_apparent_temperature(self, day_data: pd.DataFrame) -> Dict:
        """
        Анализ ощущаемой температуры (apparent temperature)
        Учитывает температуру, влажность и ветер
        """
        if 'apparent_temperature_mean' not in day_data.columns:
            # Если нет готового значения, вычисляем сами
            if all(col in day_data.columns for col in ['T2M', 'RH2M', 'WS2M']):
                day_data['apparent_temperature_calc'] = day_data.apply(
                    lambda row: self.config.calculate_apparent_temperature(
                        row['T2M'], row['RH2M'], row['WS2M']
                    ), axis=1
                )
                apparent_col = 'apparent_temperature_calc'
            else:
                return {}
        else:
            apparent_col = 'apparent_temperature_mean'
        
        thresholds = self.config.APPARENT_TEMPERATURE_THRESHOLDS
        total = len(day_data)
        
        return {
            'extreme_cold_feels_like': len(day_data[day_data[apparent_col] < thresholds['extreme_cold']]) / total,
            'very_cold_feels_like': len(day_data[(day_data[apparent_col] >= thresholds['extreme_cold']) &
                                                  (day_data[apparent_col] < thresholds['very_cold'])]) / total,
            'cold_feels_like': len(day_data[(day_data[apparent_col] >= thresholds['very_cold']) &
                                            (day_data[apparent_col] < thresholds['cold'])]) / total,
            'comfortable_feels_like': len(day_data[(day_data[apparent_col] >= thresholds['comfortable'][0]) &
                                                    (day_data[apparent_col] <= thresholds['comfortable'][1])]) / total,
            'hot_feels_like': len(day_data[(day_data[apparent_col] > thresholds['comfortable'][1]) &
                                           (day_data[apparent_col] < thresholds['hot'])]) / total,
            'very_hot_feels_like': len(day_data[(day_data[apparent_col] >= thresholds['hot']) &
                                                (day_data[apparent_col] < thresholds['very_hot'])]) / total,
            'extreme_heat_feels_like': len(day_data[day_data[apparent_col] >= thresholds['extreme_heat']]) / total,
        }
    
    def _analyze_weather_conditions(self, day_data: pd.DataFrame) -> Dict:
        """
        Анализ погодных условий по кодам WMO
        weathercode: 0-99 (ясно, облачно, дождь, снег, гроза и т.д.)
        """
        if 'weathercode' not in day_data.columns:
            return {}
        
        total = len(day_data)
        categories = self.config.WEATHER_CODE_CATEGORIES
        
        probs = {}
        for category, codes in categories.items():
            count = len(day_data[day_data['weathercode'].isin(codes)])
            probs[f'weather_{category}'] = count / total
        
        return probs
    
    def _analyze_wind_gusts(self, day_data: pd.DataFrame) -> Dict:
        """
        Анализ порывов ветра (windgusts)
        Критично для безопасности при активностях на открытом воздухе
        """
        gust_col = None
        if 'windgusts_10m_max' in day_data.columns:
            gust_col = 'windgusts_10m_max'
        elif 'wind_gusts' in day_data.columns:
            gust_col = 'wind_gusts'
        
        if gust_col is None:
            return {}
        
        thresholds = self.config.WIND_GUST_THRESHOLDS
        total = len(day_data)
        
        return {
            'calm_gusts': len(day_data[day_data[gust_col] < thresholds['calm']]) / total,
            'moderate_gusts': len(day_data[(day_data[gust_col] >= thresholds['calm']) &
                                           (day_data[gust_col] < thresholds['moderate'])]) / total,
            'strong_gusts': len(day_data[(day_data[gust_col] >= thresholds['moderate']) &
                                         (day_data[gust_col] < thresholds['strong'])]) / total,
            'very_strong_gusts': len(day_data[(day_data[gust_col] >= thresholds['strong']) &
                                              (day_data[gust_col] < thresholds['very_strong'])]) / total,
            'storm_gusts': len(day_data[(day_data[gust_col] >= thresholds['very_strong']) &
                                        (day_data[gust_col] < thresholds['storm'])]) / total,
            'hurricane_gusts': len(day_data[day_data[gust_col] >= thresholds['hurricane']]) / total,
        }
    
    def _analyze_air_quality(self, day_data: pd.DataFrame) -> Dict:
        """
        Анализ качества воздуха по AOD (Aerosol Optical Depth)
        Также анализирует черный углерод и пыль
        """
        probs = {}
        
        # Анализ AOD (качество воздуха)
        if 'AODANA' in day_data.columns or 'aod' in day_data.columns:
            aod_col = 'AODANA' if 'AODANA' in day_data.columns else 'aod'
            thresholds = self.config.AIR_QUALITY_THRESHOLDS
            total = len(day_data)
            
            probs['air_quality_excellent'] = len(day_data[day_data[aod_col] < thresholds['excellent']]) / total
            probs['air_quality_good'] = len(day_data[(day_data[aod_col] >= thresholds['excellent']) &
                                                      (day_data[aod_col] < thresholds['good'])]) / total
            probs['air_quality_moderate'] = len(day_data[(day_data[aod_col] >= thresholds['good']) &
                                                          (day_data[aod_col] < thresholds['moderate'])]) / total
            probs['air_quality_poor'] = len(day_data[(day_data[aod_col] >= thresholds['moderate']) &
                                                      (day_data[aod_col] < thresholds['poor'])]) / total
            probs['air_quality_very_poor'] = len(day_data[(day_data[aod_col] >= thresholds['poor']) &
                                                           (day_data[aod_col] < thresholds['very_poor'])]) / total
            probs['air_quality_hazardous'] = len(day_data[day_data[aod_col] >= thresholds['hazardous']]) / total
        
        # Анализ черного углерода
        if 'BCSMASS' in day_data.columns:
            bc_thresholds = self.config.BLACK_CARBON_THRESHOLDS
            total = len(day_data)
            
            probs['black_carbon_clean'] = len(day_data[day_data['BCSMASS'] < bc_thresholds['clean']]) / total
            probs['black_carbon_low'] = len(day_data[(day_data['BCSMASS'] >= bc_thresholds['clean']) &
                                                      (day_data['BCSMASS'] < bc_thresholds['low'])]) / total
            probs['black_carbon_high'] = len(day_data[day_data['BCSMASS'] >= bc_thresholds['high']]) / total
        
        # Анализ пыли
        if 'DUSMASS' in day_data.columns:
            dust_thresholds = self.config.DUST_THRESHOLDS
            total = len(day_data)
            
            probs['dust_minimal'] = len(day_data[day_data['DUSMASS'] < dust_thresholds['minimal']]) / total
            probs['dust_low'] = len(day_data[(day_data['DUSMASS'] >= dust_thresholds['minimal']) &
                                             (day_data['DUSMASS'] < dust_thresholds['low'])]) / total
            probs['dust_high'] = len(day_data[(day_data['DUSMASS'] >= dust_thresholds['high']) &
                                              (day_data['DUSMASS'] < dust_thresholds['very_high'])]) / total
            probs['dust_storm'] = len(day_data[day_data['DUSMASS'] >= dust_thresholds['very_high']]) / total
        
        return probs
    
    def _analyze_thunderstorm_risk(self, day_data: pd.DataFrame) -> Dict:
        """
        Анализ риска грозы по CAPE (Convective Available Potential Energy)
        CAPE измеряется в J/kg
        """
        cape_col = None
        if 'cape' in day_data.columns:
            cape_col = 'cape'
        elif 'CAPE' in day_data.columns:
            cape_col = 'CAPE'
        
        if cape_col is None:
            return {}
        
        thresholds = self.config.THUNDERSTORM_THRESHOLDS
        total = len(day_data)
        
        return {
            'thunderstorm_none': len(day_data[day_data[cape_col] < thresholds['very_low']]) / total,
            'thunderstorm_very_low': len(day_data[(day_data[cape_col] >= thresholds['very_low']) &
                                                   (day_data[cape_col] < thresholds['low'])]) / total,
            'thunderstorm_low': len(day_data[(day_data[cape_col] >= thresholds['low']) &
                                             (day_data[cape_col] < thresholds['moderate'])]) / total,
            'thunderstorm_moderate': len(day_data[(day_data[cape_col] >= thresholds['moderate']) &
                                                  (day_data[cape_col] < thresholds['high'])]) / total,
            'thunderstorm_high': len(day_data[(day_data[cape_col] >= thresholds['high']) &
                                              (day_data[cape_col] < thresholds['very_high'])]) / total,
            'thunderstorm_very_high': len(day_data[(day_data[cape_col] >= thresholds['very_high']) &
                                                    (day_data[cape_col] < thresholds['extreme'])]) / total,
            'thunderstorm_extreme': len(day_data[day_data[cape_col] >= thresholds['extreme']]) / total,
        }
    
    @staticmethod
    def _day_to_date_string(day_of_year: int) -> str:
        """Конвертировать день года в строку с датой (пример для 2024)"""
        date = datetime(2024, 1, 1) + pd.Timedelta(days=day_of_year - 1)
        return date.strftime('%B %d')

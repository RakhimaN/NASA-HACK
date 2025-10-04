"""
ML модуль для анализа трендов и предсказаний
Анализирует изменения климата и экстраполирует на будущее
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LinearRegression
from scipy import stats
import joblib
from pathlib import Path
from typing import Dict, Tuple, Optional


class TrendAnalyzer:
    """
    Анализатор трендов в погодных данных
    Обнаруживает изменения вероятностей со временем (глобальное потепление)
    """
    
    def __init__(self, trend_years: int = 10):
        """
        Args:
            trend_years: Количество последних лет для анализа тренда
        """
        self.trend_years = trend_years
        self.model = LinearRegression()
    
    def analyze_temperature_trend(self, data: pd.DataFrame, 
                                  day_of_year: int = None) -> Dict:
        """
        Анализировать тренд температуры
        
        Args:
            data: DataFrame с историческими данными (должен содержать year, T2M)
            day_of_year: Опционально - анализировать только для конкретного дня
            
        Returns:
            Словарь с информацией о тренде:
            {
                'trend_direction': 'warming',  # warming, cooling, stable
                'change_per_year': 0.05,       # °C в год
                'change_per_decade': 0.5,      # °C за декаду
                'confidence': 0.95,            # R² score
                'p_value': 0.001,              # Статистическая значимость
                'recent_years_avg': 16.5,      # Средняя за последние годы
                'early_years_avg': 15.2        # Средняя за ранние годы
            }
        """
        if 'T2M' not in data.columns or 'year' not in data.columns:
            return {'error': 'Missing required columns: T2M, year'}
        
        # Фильтруем по дню года если указан
        if day_of_year:
            data = data[data['day_of_year'] == day_of_year].copy()
        
        # Группируем по годам
        yearly_avg = data.groupby('year')['T2M'].mean().reset_index()
        
        if len(yearly_avg) < 5:
            return {'error': 'Insufficient data for trend analysis'}
        
        # Берем последние N лет для тренда
        recent_data = yearly_avg.tail(self.trend_years)
        
        # Линейная регрессия
        X = recent_data['year'].values.reshape(-1, 1)
        y = recent_data['T2M'].values
        
        self.model.fit(X, y)
        
        # Вычисляем метрики
        predictions = self.model.predict(X)
        r_squared = self.model.score(X, y)
        
        # Статистическая значимость (p-value)
        slope = self.model.coef_[0]
        _, _, _, p_value, _ = stats.linregress(X.flatten(), y)
        
        # Изменение за год и декаду
        change_per_year = slope
        change_per_decade = slope * 10
        
        # Средние значения
        recent_years_avg = recent_data['T2M'].mean()
        early_data = yearly_avg.head(self.trend_years)
        early_years_avg = early_data['T2M'].mean() if len(early_data) > 0 else recent_years_avg
        
        # Определяем направление
        if abs(change_per_year) < 0.02:  # Менее 0.02°C в год считаем стабильным
            trend_direction = 'stable'
        elif change_per_year > 0:
            trend_direction = 'warming'
        else:
            trend_direction = 'cooling'
        
        # Уровень достоверности
        if p_value < 0.01 and r_squared > 0.7:
            significance = 'high'
        elif p_value < 0.05 and r_squared > 0.5:
            significance = 'medium'
        else:
            significance = 'low'
        
        return {
            'trend_direction': trend_direction,
            'change_per_year': float(change_per_year),
            'change_per_decade': float(change_per_decade),
            'confidence': float(r_squared),
            'p_value': float(p_value),
            'significance': significance,
            'recent_years_avg': float(recent_years_avg),
            'early_years_avg': float(early_years_avg),
            'total_change': float(recent_years_avg - early_years_avg),
            'years_analyzed': len(recent_data)
        }
    
    def extrapolate_to_year(self, data: pd.DataFrame, 
                           target_year: int,
                           day_of_year: int = None) -> Dict:
        """
        Экстраполировать температуру на будущий год
        
        Args:
            data: Исторические данные
            target_year: Год для прогноза (например, 2026)
            day_of_year: Опционально - для конкретного дня
            
        Returns:
            Словарь с прогнозом температуры
        """
        # Анализируем тренд
        trend = self.analyze_temperature_trend(data, day_of_year)
        
        if 'error' in trend:
            return trend
        
        # Базовая статистика (без тренда)
        if day_of_year:
            day_data = data[data['day_of_year'] == day_of_year]
        else:
            day_data = data
        
        base_mean = day_data['T2M'].mean()
        base_std = day_data['T2M'].std()
        
        # Последний год в данных
        last_year = data['year'].max()
        years_ahead = target_year - last_year
        
        # Экстраполяция с учетом тренда
        temperature_adjustment = trend['change_per_year'] * years_ahead
        adjusted_mean = base_mean + temperature_adjustment
        
        # Диапазон прогноза (с учетом неопределенности)
        confidence_multiplier = 1 + (years_ahead * 0.1)  # +10% неопределенности за год
        adjusted_std = base_std * confidence_multiplier
        
        return {
            'target_year': target_year,
            'base_temperature': float(base_mean),
            'predicted_temperature': float(adjusted_mean),
            'temperature_adjustment': float(temperature_adjustment),
            'prediction_range': {
                'min': float(adjusted_mean - adjusted_std),
                'max': float(adjusted_mean + adjusted_std),
                'std': float(adjusted_std)
            },
            'trend': trend,
            'years_ahead': years_ahead,
            'confidence_note': f'Прогноз на {years_ahead} лет вперед имеет {"высокую" if years_ahead < 5 else "среднюю" if years_ahead < 10 else "низкую"} достоверность'
        }
    
    def adjust_probabilities_for_trend(self, base_probabilities: Dict,
                                      trend_analysis: Dict,
                                      years_ahead: int) -> Dict:
        """
        Скорректировать вероятности с учетом тренда
        
        Args:
            base_probabilities: Базовые вероятности (из статистического анализа)
            trend_analysis: Результат analyze_temperature_trend
            years_ahead: На сколько лет вперед
            
        Returns:
            Скорректированные вероятности
        """
        if trend_analysis.get('significance') == 'low':
            # Если тренд незначителен, не корректируем
            return base_probabilities.copy()
        
        adjusted = base_probabilities.copy()
        
        # Изменение температуры
        temp_change = trend_analysis['change_per_year'] * years_ahead
        
        # Корректировка для warming тренда
        if trend_analysis['trend_direction'] == 'warming' and abs(temp_change) > 0.5:
            # Увеличиваем вероятность жары
            if 'very_hot' in adjusted:
                adjusted['very_hot'] = min(1.0, adjusted['very_hot'] + 0.05 * years_ahead)
            if 'hot' in adjusted:
                adjusted['hot'] = min(1.0, adjusted['hot'] + 0.03 * years_ahead)
            
            # Уменьшаем вероятность холода
            if 'very_cold' in adjusted:
                adjusted['very_cold'] = max(0.0, adjusted['very_cold'] - 0.05 * years_ahead)
            if 'cold' in adjusted:
                adjusted['cold'] = max(0.0, adjusted['cold'] - 0.03 * years_ahead)
        
        # Корректировка для cooling тренда
        elif trend_analysis['trend_direction'] == 'cooling' and abs(temp_change) > 0.5:
            # Увеличиваем вероятность холода
            if 'very_cold' in adjusted:
                adjusted['very_cold'] = min(1.0, adjusted['very_cold'] + 0.05 * years_ahead)
            if 'cold' in adjusted:
                adjusted['cold'] = min(1.0, adjusted['cold'] + 0.03 * years_ahead)
            
            # Уменьшаем вероятность жары
            if 'very_hot' in adjusted:
                adjusted['very_hot'] = max(0.0, adjusted['very_hot'] - 0.05 * years_ahead)
            if 'hot' in adjusted:
                adjusted['hot'] = max(0.0, adjusted['hot'] - 0.03 * years_ahead)
        
        return adjusted


class WeatherClassifier:
    """
    ML классификатор погодных условий
    Может обучаться на данных и классифицировать дни
    """
    
    def __init__(self):
        self.model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42
        )
        self.is_trained = False
    
    def prepare_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Подготовить признаки для ML модели
        
        Features:
        - Temperature (mean, min, max)
        - Precipitation
        - Wind speed
        - Humidity
        - Day of year (cyclical encoding)
        - Latitude, longitude
        """
        features = data.copy()
        
        # Циклическое кодирование дня года
        features['day_sin'] = np.sin(2 * np.pi * features['day_of_year'] / 365)
        features['day_cos'] = np.cos(2 * np.pi * features['day_of_year'] / 365)
        
        # TODO: Добавить больше признаков
        
        return features
    
    def train(self, X_train, y_train):
        """Обучить модель"""
        self.model.fit(X_train, y_train)
        self.is_trained = True
        print("✓ Модель обучена")
    
    def predict_probabilities(self, X):
        """Получить вероятности классов"""
        if not self.is_trained:
            raise Exception("Модель не обучена! Вызовите train() сначала")
        
        return self.model.predict_proba(X)
    
    def save(self, filepath: str):
        """
        Сохранить модель"""
        if not self.is_trained:
            raise Exception("Нечего сохранять - модель не обучена")
        
        joblib.dump(self.model, filepath)
        print(f"✓ Модель сохранена в {filepath}")
    
    def load(self, filepath: str):
        """
        Загрузить модель"""
        self.model = joblib.load(filepath)
        self.is_trained = True
        print(f"✓ Модель загружена из {filepath}")


class MLPredictor:
    """
    Главный класс для ML предсказаний
    Объединяет статистику + ML для улучшенных прогнозов
    """
    
    def __init__(self):
        self.trend_analyzer = TrendAnalyzer()
        self.classifier = WeatherClassifier()
    
    def analyze_with_ml(self, data: pd.DataFrame, day_of_year: int) -> dict:
        """
        Анализ с использованием ML
        Возвращает вероятности + тренды
        """
        # TODO: Реализовать комбинированный анализ
        
        return {
            'probabilities': {},  # Вероятности от ML модели
            'trends': {},  # Информация о трендах
            'forecast': {}  # Прогноз на будущее
        }


# =============================================================================
# ПРИМЕР ИСПОЛЬЗОВАНИЯ (когда будет реализовано)
# =============================================================================

if __name__ == "__main__":
    print("🤖 ML модуль - ЗАГОТОВКА")
    print("Этот модуль будет реализован если останется время на хакатоне")
    print("\nПланируемый функционал:")
    print("  ✨ Анализ трендов (глобальное потепление)")
    print("  ✨ Предсказание вероятностей с учетом изменений")
    print("  ✨ Классификация дней на комфортные/некомфортные")
    print("  ✨ Обнаружение аномалий")

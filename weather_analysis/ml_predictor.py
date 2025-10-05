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
        Анализирует температурный тренд для заданного дня года.
        
        Args:
            data: DataFrame с историческими данными.
            day_of_year: День года (1-366) для анализа. Если None, анализирует весь датасет.
            
        Returns:
            Словарь с результатом тренда.
        """
        if day_of_year is not None:
            day_data = data[data['day_of_year'] == day_of_year].copy()
        else:
            day_data = data.copy()
            
        if len(day_data) < self.trend_years: # Минимум данных для тренда
            return {'trend_detected': False, 'message': 'Недостаточно данных для анализа тренда.'}
        
        # Используем данные за последние self.trend_years
        latest_year = day_data['year'].max()
        recent_data = day_data[day_data['year'] >= latest_year - self.trend_years].copy()
        
        if len(recent_data) < self.trend_years / 2: # Еще одна проверка на достаточность
            return {'trend_detected': False, 'message': 'Недостаточно свежих данных для анализа тренда.'}

        # Для анализа тренда используем среднюю температуру
        X = recent_data[['year']].values
        y = recent_data['T2M'].values # Средняя температура на 2м
        
        if len(np.unique(X)) < 2: # Нужно хотя бы 2 уникальных года
             return {'trend_detected': False, 'message': 'Недостаточно уникальных лет для расчета тренда.'}
        
        self.model.fit(X, y)
        slope = self.model.coef_[0] # Изменение температуры за год
        
        # Проверка значимости тренда (упрощенно)
        # Более строго - использовать p-value из статистики
        trend_significant = abs(slope) > 0.05 # Если изменение > 0.05 градуса в год
        
        return {
            'trend_detected': trend_significant,
            'slope_c_per_year': float(slope),
            'message': f'Температурный тренд: {slope:.2f}°C в год.'
        }
    
    def extrapolate_to_year(self, data: pd.DataFrame, 
                            target_year: int,
                            day_of_year: int = None) -> Dict:
        """
        Экстраполирует температуру для заданного дня года в целевой год.
        
        Args:
            data: DataFrame с историческими данными.
            target_year: Год, для которого нужно сделать экстраполяцию.
            day_of_year: День года.
            
        Returns:
            Словарь с экстраполированной температурой.
        """
        trend_analysis = self.analyze_temperature_trend(data, day_of_year)
        
        if trend_analysis['trend_detected']:
            slope = trend_analysis['slope_c_per_year']
            
            if day_of_year is not None:
                day_data = data[data['day_of_year'] == day_of_year].copy()
            else:
                day_data = data.copy()
                
            # Средняя температура за последний год в данных
            latest_year_data = day_data[day_data['year'] == day_data['year'].max()]
            if latest_year_data.empty:
                return {'extrapolated_temperature': None, 'message': 'Нет данных для экстраполяции.'}
            
            base_temp = latest_year_data['T2M'].mean()
            years_ahead = target_year - latest_year_data['year'].max()
            
            extrapolated_temp = base_temp + (slope * years_ahead)
            
            return {
                'extrapolated_temperature': float(extrapolated_temp),
                'message': f'Экстраполированная температура на {target_year} год: {extrapolated_temp:.2f}°C.'
            }
        
        return {'extrapolated_temperature': None, 'message': 'Тренд не обнаружен для экстраполяции.'}
    
    def adjust_probabilities_for_trend(self, base_probabilities: Dict,
                                       trend_analysis: Dict,
                                       years_ahead: int) -> Dict:
        """
        Корректирует вероятности на основе обнаруженного тренда.
        Это упрощенная эвристика.
        """
        adjusted_probs = base_probabilities.copy()
        
        if trend_analysis['trend_detected']:
            slope = trend_analysis['slope_c_per_year']
            
            # Если тренд положительный (потепление)
            if slope > 0:
                # Увеличиваем вероятность жарких дней, уменьшаем холодных
                adjusted_probs['very_hot'] = min(1.0, adjusted_probs.get('very_hot', 0) + (slope * years_ahead * 0.02))
                adjusted_probs['hot'] = min(1.0, adjusted_probs.get('hot', 0) + (slope * years_ahead * 0.01))
                adjusted_probs['very_cold'] = max(0.0, adjusted_probs.get('very_cold', 0) - (slope * years_ahead * 0.01))
                adjusted_probs['cold'] = max(0.0, adjusted_probs.get('cold', 0) - (slope * years_ahead * 0.005))
            
            # Если тренд отрицательный (похолодание)
            elif slope < 0:
                # Увеличиваем вероятность холодных дней, уменьшаем жарких
                adjusted_probs['very_cold'] = min(1.0, adjusted_probs.get('very_cold', 0) + (abs(slope) * years_ahead * 0.02))
                adjusted_probs['cold'] = min(1.0, adjusted_probs.get('cold', 0) + (abs(slope) * years_ahead * 0.01))
                adjusted_probs['very_hot'] = max(0.0, adjusted_probs.get('very_hot', 0) - (abs(slope) * years_ahead * 0.01))
                adjusted_probs['hot'] = max(0.0, adjusted_probs.get('hot', 0) - (abs(slope) * years_ahead * 0.005))
        
        return adjusted_probs


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
        self.features = [
            'T2M', 'PRECTOTCORR', 'WS2M', 'RH2M',
            'CLOUD_AMT', 'ALLSKY_SFC_SW_DWN', 'day_of_year'
        ]
        self.target = 'is_comfortable' # Целевая переменная
    
    def prepare_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Подготавливает признаки для обучения модели.
        Добавляет целевую переменную 'is_comfortable'.
        """
        df = data.copy()
        
        # Пример простой логики комфорта
        df['is_comfortable'] = ((df['T2M'] >= 15) & (df['T2M'] <= 25) & 
                                 (df['RH2M'] >= 30) & (df['RH2M'] <= 70) & 
                                 (df['WS2M'] <= 5)).astype(int)
        
        # Удаляем строки с NaN в признаках или целевой переменной
        df = df.dropna(subset=self.features + [self.target])
        
        return df
    
    def train(self, X_train: pd.DataFrame, y_train: pd.Series):
        """
        Обучает модель классификатора.
        """
        if X_train.empty or y_train.empty:
            print("⚠ Недостаточно данных для обучения классификатора.")
            return
            
        try:
            self.model.fit(X_train, y_train)
            self.is_trained = True
            print("✅ Модель классификатора успешно обучена.")
        except Exception as e:
            print(f"❌ Ошибка обучения модели: {e}")
            self.is_trained = False
    
    def predict_probabilities(self, X: pd.DataFrame) -> np.ndarray:
        """
        Предсказывает вероятности классов для новых данных.
        """
        if not self.is_trained:
            print("⚠ Модель не обучена. Возвращаем случайные вероятности.")
            # Возвращаем равномерное распределение, если модель не обучена
            return np.full(len(X), 0.5) 
        
        try:
            # predict_proba возвращает вероятности для каждого класса [prob_class_0, prob_class_1]
            return self.model.predict_proba(X)[:, 1] # Вероятность класса 1 (комфортно)
        except Exception as e:
            print(f"❌ Ошибка предсказания: {e}")
            return np.full(len(X), 0.5)
    
    def save(self, filepath: str):
        """
        Сохраняет обученную модель на диск.
        """
        if not self.is_trained:
            print("⚠ Модель не обучена, нечего сохранять.")
            return
        try:
            joblib.dump(self.model, filepath)
            print(f"✅ Модель классификатора сохранена в {filepath}")
        except Exception as e:
            print(f"❌ Ошибка сохранения модели: {e}")
    
    def load(self, filepath: str):
        """
        Загружает модель с диска.
        """
        try:
            self.model = joblib.load(filepath)
            self.is_trained = True
            print(f"✅ Модель классификатора загружена из {filepath}")
        except Exception as e:
            print(f"❌ Ошибка загрузки модели: {e}")
            self.is_trained = False


class MLPredictor:
    """
    Главный класс для ML предсказаний
    Объединяет статистику + ML для улучшенных прогнозов
    """
    
    def __init__(self):
        self.trend_analyzer = TrendAnalyzer()
        self.classifier = WeatherClassifier()
        self.model_path = Path('./ml_models/weather_classifier.joblib')
        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Попытка загрузить модель при инициализации
        if self.model_path.exists():
            self.classifier.load(str(self.model_path))

    def analyze_with_ml(self, data: pd.DataFrame, day_of_year: int) -> dict:
        """
        Выполняет ML-анализ данных: тренды и классификация.
        """
        results = {}
        
        # Анализ тренда температуры
        trend_result = self.trend_analyzer.analyze_temperature_trend(data, day_of_year)
        results['temperature_trend'] = trend_result
        
        # Классификация комфортности дня
        prepared_data = self.classifier.prepare_features(data)
        if not prepared_data.empty:
            X_day = prepared_data[prepared_data['day_of_year'] == day_of_year][self.classifier.features]
            if not X_day.empty:
                comfort_prob = self.classifier.predict_probabilities(X_day).mean() # Средняя вероятность комфорта
                results['comfort_probability_ml'] = float(comfort_prob)
            else:
                results['comfort_probability_ml'] = None
        else:
            results['comfort_probability_ml'] = None

        return results

    def train_classifier(self, data: pd.DataFrame):
        """
        Обучает классификатор на всем доступном датасете.
        """
        prepared_data = self.classifier.prepare_features(data)
        if not prepared_data.empty:
            X_train = prepared_data[self.classifier.features]
            y_train = prepared_data[self.classifier.target]
            self.classifier.train(X_train, y_train)
            self.classifier.save(str(self.model_path))
        else:
            print("⚠ Недостаточно данных для обучения ML классификатора.")


if __name__ == "__main__":
    print("🤖 ML модуль - ЗАГОТОВКА")
    print("Этот модуль будет реализован если останется время на хакатоне")
    print("\nПланируемый функционал:")
    print("  ✨ Анализ трендов (глобальное потепление)")
    print("  ✨ Предсказание вероятностей с учетом изменений")
    print("  ✨ Классификация дней на комфортные/некомфортные")
    print("  ✨ Обнаружение аномалий")

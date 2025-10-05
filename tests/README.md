# Тесты NASA Weather Analysis System

## 📊 Статистика тестов

**Всего тестов: 109 ✅**
- ✅ Все тесты проходят успешно
- ⏱️ Время выполнения: ~54 секунды

## 🧪 Структура тестов

### 1. `test_analyze_weather.py` (33 теста)
Тесты основной функции `analyze_weather()`

- **TestAnalyzeWeatherBasic** (8 тестов)
  - Проверка возвращаемой структуры данных
  - Валидация обязательных ключей
  - Проверка диапазонов вероятностей (0-1)
  
- **TestAnalyzeWeatherLocations** (5 тестов)
  - Москва, Нью-Йорк, Токио, Лондон, Сидней
  - Северное и южное полушарие
  
- **TestAnalyzeWeatherDates** (7 тестов)
  - Все сезоны года
  - Високосный год (29 февраля)
  - Граничные даты (1 января, 31 декабря)
  
- **TestAnalyzeWeatherDataSources** (2 теста)
  - NASA POWER API
  - Open-Meteo API
  
- **TestAnalyzeWeatherValidation** (5 тестов)
  - Неправильные координаты
  - Неправильный формат даты
  
- **TestAnalyzeWeatherProbabilities** (3 теста)
  - Наличие температурных вероятностей
  - Проверка адекватности (летом не холодно, зимой не жарко)
  
- **TestAnalyzeWeatherStatistics** (2 теста)
  - Наличие температурной статистики
  - Разумность значений температур
  
- **TestAnalyzeWeatherCaching** (1 тест)
  - Повторные вызовы используют кэш

### 2. `test_analyze_weather_range.py` (43 теста)
Тесты функции `analyze_weather_range()` для диапазонов дат

- **TestAnalyzeWeatherRangeBasic** (7 тестов)
  - Структура результата
  - Наличие агрегированных данных
  - Разбивка по дням
  
- **TestAnalyzeWeatherRangeDurations** (4 теста)
  - 2 дня, неделя, 2 недели, месяц
  
- **TestAnalyzeWeatherRangeLocations** (4 теста)
  - Москва, Нью-Йорк, Токио, Сочи
  
- **TestAnalyzeWeatherRangeValidation** (3 теста)
  - Проверка входных данных
  - Начало после конца = ошибка
  
- **TestAnalyzeWeatherRangeAggregation** (3 теста)
  - Статистика, вероятности, рекомендации лучших дней
  
- **TestAnalyzeWeatherRangeDailyBreakdown** (3 теста)
  - Каждый день имеет дату и день года
  - Дни идут последовательно
  
- **TestAnalyzeWeatherRangeSpecialCases** (3 теста)
  - Границы месяцев и года
  - Високосный февраль
  
- **TestAnalyzeWeatherRangeDataSources** (2 теста)
  - NASA и Open-Meteo источники
  
- **TestAnalyzeWeatherRangeRealScenarios** (3 теста)
  - Планирование отпуска (2 недели)
  - Поездка на выходные
  - Командировка

### 3. `test_data_service.py` (23 теста)
Тесты сервиса данных `WeatherDataService`

- **TestWeatherDataServiceInit** (3 теста)
  - Инициализация с разными источниками
  
- **TestWeatherDataServiceGetData** (4 теста)
  - Возвращает корректный формат данных
  - NASA и Open-Meteo источники
  - Данные за несколько лет
  
- **TestWeatherDataServiceLocations** (5 тестов)
  - Разные локации по миру
  - Отрицательные координаты
  - Южное полушарие
  
- **TestWeatherDataServiceYearRanges** (3 теста)
  - 1 год, 2 года, 5 лет
  
- **TestWeatherDataServiceDataFormat** (4 теста)
  - Проверка формата DataFrame
  - Наличие колонки day_of_year
  - Диапазон значений
  - Температурные данные
  
- **TestWeatherDataServiceCaching** (1 тест)
  - Кэширование работает

### 4. `test_utils.py` (13 тестов)
Тесты утилит и вспомогательных функций

- **TestDateUtils** (4 теста)
  - Конвертация дат в день года
  - Високосный год
  
- **TestProbabilityFormatting** (4 теста)
  - Форматирование 0%, 50%, 100%
  - Дробные значения
  
- **TestProbabilityCategorization** (5 теста)
  - 5 категорий вероятностей (very_low, low, moderate, high, very_high)
  
- **TestProbabilityDescription** (4 теста)
  - Текстовые описания вероятностей
  
- **TestUtilsImport** (4 теста)
  - Импорт всех модулей работает
  
- **TestConfig** (3 теста)
  - Наличие порогов температур, осадков, ветра

## 🚀 Как запустить тесты

### Все тесты
```bash
cd /home/zerotwo/NASA
source venv/bin/activate
PYTHONPATH=/home/zerotwo/NASA:$PYTHONPATH pytest tests/ -v
```

### Быстрый запуск (без вывода)
```bash
PYTHONPATH=/home/zerotwo/NASA:$PYTHONPATH pytest tests/ -q
```

### Конкретный файл
```bash
PYTHONPATH=/home/zerotwo/NASA:$PYTHONPATH pytest tests/test_analyze_weather.py -v
```

### С покрытием кода
```bash
PYTHONPATH=/home/zerotwo/NASA:$PYTHONPATH pytest tests/ --cov=weather_analysis --cov-report=html
```

## 📋 Что проверяют тесты

### ✅ Функциональность
- Основные функции работают корректно
- Все источники данных доступны (NASA, Open-Meteo)
- Кэширование работает
- Валидация входных данных

### ✅ Структура данных
- Все обязательные ключи присутствуют
- Типы данных корректные
- Вероятности в диапазоне [0, 1]

### ✅ Разные сценарии
- Различные локации по всему миру
- Все сезоны года
- Граничные случаи (високосный год, границы года)
- Реальные сценарии использования

### ✅ Производительность
- Тесты выполняются за ~54 секунды
- Кэширование ускоряет повторные запросы

## 🎯 Покрытие

Тесты покрывают:
- ✅ `analyze_weather()` - основная функция
- ✅ `analyze_weather_range()` - диапазон дат
- ✅ `WeatherDataService` - получение данных
- ✅ Утилиты (date_to_day_of_year, format_probability, и т.д.)
- ✅ Валидация входных данных
- ✅ Обработка ошибок

## 📝 Примеры использования

### Запуск одного теста
```bash
pytest tests/test_analyze_weather.py::TestAnalyzeWeatherBasic::test_returns_dict -v
```

### Запуск класса тестов
```bash
pytest tests/test_analyze_weather.py::TestAnalyzeWeatherLocations -v
```

### Показать медленные тесты
```bash
pytest tests/ --durations=10
```

## 🐛 Отладка

### Показать полный вывод ошибок
```bash
pytest tests/ -v --tb=long
```

### Остановиться на первой ошибке
```bash
pytest tests/ -x
```

### Запустить в режиме дебага
```bash
pytest tests/ -v --pdb
```

## ✨ Результаты

```
===================== 109 passed in 54.16s =======================

✅ Все тесты пройдены успешно!
✅ Система полностью функциональна
✅ Готова к использованию
```

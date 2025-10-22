# Окно в мир: ETL и дашборды
В рамках проекта был создан ETL-пайплайн для обработки данных по продажам и сделана визуализация результатов в дашбордах Metabase.

## Описание ETL-пайплайна
1. Данные извлечены из Exel-файла и преобразованы
2. Загрузка данных в PostgreSQL
3. Создание дашбордов в Metabase (![Скриншот](screenshots/2025-10-22_07-54-42.jpeg)
):
   - Динамика продаж по кварталам, месяцам
   - Распределение продаж по дням недели/странам
   - Топ-10 продуктов по выручке

## Использованные инструменты
- Airflow
- PostgreSQL
- Metabase
- Docker

## Структура проекта

    okno_v_mir_etl/
    ├── dags/
    │   └── etl_pipeline.py
    ├── data/
    │   └── Окно в мир.xlsx
    ├── docker-compose.yml
    ├── requirements.txt
    └── README.md

## Как запустить проект

1. Клонируйте репозиторий:
   ```bash
   git clone https://github.com/ваш_никнейм/okno_v_mir_etl.git
   cd okno_v_mir_etl

2. Запустите Docker Compose:
   docker-compose up -d

3. Доступ к сервисам:
   - Airflow: http://localhost:8080
      - Логин: admin
      - Пароль: admin  
   - Metabase: http://localhost:3000
   - PostgreSQL: localhost:5433

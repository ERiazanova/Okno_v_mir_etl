from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import os

# Путь для временных CSV внутри контейнера
tmp_path = "/opt/airflow/tmp_airflow/"
os.makedirs(tmp_path, exist_ok=True)

# Подключение к Postgres
engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/dwh")

# Создание таблиц
def create_tables():
    ddl_tables = [
        """
        CREATE TABLE IF NOT EXISTS dim_dates (
            date_id SERIAL PRIMARY KEY,
            date_actual DATE NOT NULL,
            year INT NOT NULL,
            month INT NOT NULL,
            quarter INT NOT NULL,
            day INT NOT NULL,
            day_of_week INT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_retailers (
            retailer_id INT PRIMARY KEY,
            retailer_name VARCHAR(255),
            retailer_type VARCHAR(100)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_countries (
            country_id INT PRIMARY KEY,
            country_name VARCHAR(100),
            region_name VARCHAR(100)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_products (
            product_id INT PRIMARY KEY,
            product_name VARCHAR(255),
            product_type VARCHAR(100),
            product_category VARCHAR(100)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_sales (
            sales_id SERIAL PRIMARY KEY,
            retailer_id INT NOT NULL REFERENCES dim_retailers(retailer_id),
            country_id INT NOT NULL REFERENCES dim_countries(country_id),
            product_id INT NOT NULL REFERENCES dim_products(product_id),
            date_id INT NOT NULL REFERENCES dim_dates(date_id),
            revenue NUMERIC(12,2),
            gross_revenue NUMERIC(12,2),
            sales_plan NUMERIC(12,2),
            quantity INT,
            quantity_returned INT
        );
        """
    ]
    try:
        with engine.begin() as conn:
            for ddl in ddl_tables:
                conn.execute(text(ddl))
                print(f"Таблица {ddl.split(' ')[2]} создана/проверена")
    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")

# Извлечение и преобразование данных
def extract_transform():
    df = pd.read_excel("/opt/airflow/data/Окно в мир_etl.xlsx")
    df['Дата'] = pd.to_datetime(df['Дата новая'])

    # Измерения
    countries = df[['Код страны', 'Страна предприятия розничной торговли', 
                    'Территория предприятия розничной торговли']].drop_duplicates()
    products = df[['Номер продукции', 'Продукция', 'Тип продукции', 'Категория продукции']].drop_duplicates()
    retailer = df[['Код розничного продавца', 
                   'Наименование розничного продавца (мультиязычн)', 
                   'Тип розничного продавца']].drop_duplicates()

    # Даты
    dates = df[['Дата новая']].drop_duplicates()
    dates['Год'] = dates['Дата новая'].dt.year
    dates['Месяц'] = dates['Дата новая'].dt.month
    dates['Квартал'] = dates['Дата новая'].dt.quarter
    dates['День'] = dates['Дата новая'].dt.day
    dates['День недели'] = dates['Дата новая'].dt.dayofweek
    dates['date_id'] = range(1, len(dates) + 1)

    # Факт
    fact = df.merge(dates[['date_id', 'Дата новая']], on='Дата новая', how='left')
    fact = fact.drop(columns=['Дата', 'Дата новая', 'Наименование розничного продавца (мультиязычн)',
                              'Тип розничного продавца', 'Страна предприятия розничной торговли',
                              'Территория предприятия розничной торговли', 'Продукция', 'Тип продукции', 
                              'Категория продукции'])
    fact = fact.rename(columns={
        'Код розничного продавца': 'retailer_id',
        'Код страны': 'country_id',
        'Номер продукции': 'product_id',
        'Доход': 'revenue',
        'Валовой доход': 'gross_revenue',
        'План продаж': 'sales_plan',
        'Количество': 'quantity',
        'Количество (шт) возврата': 'quantity_returned',
        'date_id': 'date_id'
    })

    # Сохраняем CSV во временную папку
    dates.to_csv(tmp_path + "dim_dates.csv", index=False)
    retailer.to_csv(tmp_path + "dim_retailers.csv", index=False)
    countries.to_csv(tmp_path + "dim_countries.csv", index=False)
    products.to_csv(tmp_path + "dim_products.csv", index=False)
    fact.to_csv(tmp_path + "fact_sales.csv", index=False)
    print("CSV-файлы созданы в tmp_airflow")

# Загрузка в Postgres
def load_to_postgres():
    try:
        # Удаляем таблицы в правильном порядке
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS fact_sales CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS dim_products CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS dim_countries CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS dim_retailers CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS dim_dates CASCADE"))
            print("Все таблицы удалены")

        # Загружаем данные
        dim_dates = pd.read_csv(tmp_path + "dim_dates.csv")
        dim_retailer = pd.read_csv(tmp_path + "dim_retailers.csv")
        dim_country = pd.read_csv(tmp_path + "dim_countries.csv")
        dim_products = pd.read_csv(tmp_path + "dim_products.csv")
        fact_sales = pd.read_csv(tmp_path + "fact_sales.csv")

        tables_to_load = {
            "dim_dates": dim_dates,
            "dim_retailers": dim_retailer,
            "dim_countries": dim_country,
            "dim_products": dim_products,
            "fact_sales": fact_sales
        }
        for table_name, df in tables_to_load.items():
            df.to_sql(table_name, con=engine, if_exists='append', index=False)
            print(f"{table_name} успешно загружена в Postgres")
    except Exception as e:
        print(f"Ошибка в load_to_postgres: {e}")
        import traceback
        traceback.print_exc()

# DAG
with DAG(
    dag_id='etl_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    t0 = PythonOperator(task_id='create_tables', python_callable=create_tables)
    t1 = PythonOperator(task_id='extract_transform', python_callable=extract_transform)
    t2 = PythonOperator(task_id='load_to_postgres', python_callable=load_to_postgres)

    t0 >> t1 >> t2

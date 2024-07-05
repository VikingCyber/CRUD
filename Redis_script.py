import csv
import statistics
import time
import pandas as pd
import json

from rejson import Client, Path
from os import getenv
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
host = getenv("HOST_REDIS")
port = getenv("PORT_REDIS")
user = getenv("USER_REDIS")
database = getenv("DATABASE_REDIS")


results_redis = {
    "insert_redis": {
        "iterations": 10,
        "total_execution_time": 181.37599,
        "mean_time": 20.2752,
        "variance_time": 2.58048
    },
    "select_redis": {
        "iterations": 100,
        "total_execution_time": 1441.42199,
        "mean_time": 14.41422,
        "variance_time": 8.99444,
        "iterations_2": 100,
        "total_execution_time_2": 1457.45299,
        "mean_time_2": 14.57453,
        "variance_time_2": 2.48977
    },
    "delete_redis": {
        "iterations": 100,
        "total_execution_time": 1426.48599,
        "mean_time": 14.26486,
        "variance_time": 6.02514,
    },
    "update_redis": {
        "iterations": 100,
        "total_execution_time": 1344.02599,
        "mean_time": 13.44026,
        "variance_time": 0.69427
    },
}

redis_client = Client(host=host, port=port, decode_responses=True)


def write_results_to_excel(results, filename='results_redis.xlsx'):
    # Создаем DataFrame с одиночными индексами для строк и столбцов
    df = pd.DataFrame(results)

    # Записываем DataFrame в Excel
    try:
        df.to_excel(filename, index=True, header=True)
        print(f"Results saved to {filename} successfully.")
    except Exception as e:
        print(f"Error saving results to Excel: {e}")


def measure_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.monotonic()
        result = None
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            print(f"Error executing {func.__name__}: {e}")
        end_time = time.monotonic()
        execution_time = end_time - start_time
        return result, execution_time
    return wrapper


def load_data_from_csv(file_path, limit=None):
    with open(file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        data = []
        for idx, row in enumerate(csv_reader):
            if limit and idx >= limit:
                break
            try:
                row['ingredients'] = json.loads(row['ingredients'])
                row['directions'] = json.loads(row['directions'])
                row['NER'] = json.loads(row['NER'])
                data.append(row)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON for row {idx}: {e}")
                continue
        return data


@measure_execution_time
def insert_data_to_redis(data):
    for idx, recipe in enumerate(data):
        key = f"recipe:{idx}"
        formatted_recipe = json.dumps(recipe, indent=2, ensure_ascii=False)
        redis_client.execute_command('JSON.SET', key, '.', formatted_recipe)
        print(f"Inserted {key}")


def clear_redis_data(pattern="recipe:*"):
    keys = redis_client.keys(pattern)
    if keys:
        redis_client.delete(*keys)
        print(f"Deleted {len(keys)} keys")


def perform_insert_table_operations(iterations: int, data: list) -> list[float]:
    execution_times = []
    for _ in range(iterations):
        clear_redis_data()
        _, exec_time = insert_data_to_redis(data)
        execution_times.append(exec_time)

    mean_insert_time = round(statistics.mean(execution_times), 5)
    variance_insert_time = round(statistics.variance(execution_times), 5)
    total_execution_time = round(sum(execution_times), 5)

    results_redis["insert_redis"]["iterations"] = iterations
    results_redis["insert_redis"]["mean_time"] = mean_insert_time
    results_redis["insert_redis"]["variance_time"] = variance_insert_time
    results_redis["insert_redis"]["total_execution_time"] = total_execution_time

    return execution_times


@measure_execution_time
def select_most_common_ner():
    # Создаем словарь для хранения частоты именованных сущностей
    ner_counts = {}

    # Получаем список всех ключей, соответствующих шаблону 'recipe:*'
    keys = redis_client.keys('recipe:*')

    # Проходим по каждому ключу
    for key in keys:
        # Получаем JSON из Redis по текущему ключу
        recipe_data = redis_client.execute_command('JSON.GET', key)

        # Проверяем, что полученные данные не пустые
        if recipe_data:
            # Получаем список NER из объекта recipe_data
            ner_list = recipe_data.get('NER', [])

            # Обновляем частоту каждого элемента NER
            for ner_entity in ner_list:
                if ner_entity in ner_counts:
                    ner_counts[ner_entity] += 1
                else:
                    ner_counts[ner_entity] = 1

    # Сортируем по частоте и выводим топ-50
    sorted_ner_counts = sorted(ner_counts.items(), key=lambda x: x[1])[-50:]
    # return sorted_ner_counts
    for ner_entity, count in enumerate(sorted_ner_counts, start=1):
        print(f"{ner_entity}: {count}")


def perform_select_table_operations_most_common(iterations: int) -> list[float]:
    execution_times = []
    for _ in range(iterations):
        _, exec_time = select_most_common_ner()
        execution_times.append(exec_time)

    mean_insert_time = round(statistics.mean(execution_times), 5)
    variance_insert_time = round(statistics.variance(execution_times), 5)
    total_execution_time = round(sum(execution_times), 5)

    results_redis["select_redis"]["iterations_2"] = iterations
    results_redis["select_redis"]["mean_time_2"] = mean_insert_time
    results_redis["select_redis"]["variance_time_2"] = variance_insert_time
    results_redis["select_redis"]["total_execution_time_2"] = total_execution_time

    return execution_times


@measure_execution_time
def select_recipe_chicken_parmesan():
    keys = redis_client.keys('recipe:*')
    for key in keys:
        recipe_data = redis_client.execute_command('JSON.GET', key)
        if recipe_data.get('title') == 'Baked Chicken Parmesan':
            directions = recipe_data.get('directions')
            print(directions)


def perform_select_table_operations_chicken(iterations: int) -> list[float]:
    execution_times = []
    for _ in range(iterations):
        _, exec_time = select_recipe_chicken_parmesan()
        execution_times.append(exec_time)

    mean_insert_time = round(statistics.mean(execution_times), 5)
    variance_insert_time = round(statistics.variance(execution_times), 5)
    total_execution_time = round(sum(execution_times), 5)

    results_redis["select_redis"]["iterations"] = iterations
    results_redis["select_redis"]["mean_time"] = mean_insert_time
    results_redis["select_redis"]["variance_time"] = variance_insert_time
    results_redis["select_redis"]["total_execution_time"] = total_execution_time

    return execution_times


@measure_execution_time
def delete_record_with_pie():
    keys = redis_client.keys('recipe:*')
    for key in keys:
        recipe_data = redis_client.execute_command('JSON.GET', key)
        if 'pie' in recipe_data.get('title').lower():
            redis_client.delete(key)
            print(f"Deleted {key}")


def perform_delete_table_operations_pie(iterations: int) -> list[float]:
    execution_times = []
    for _ in range(iterations):
        _, exec_time = delete_record_with_pie()
        execution_times.append(exec_time)
        insert_data_to_redis(data)

    mean_insert_time = round(statistics.mean(execution_times), 5)
    variance_insert_time = round(statistics.variance(execution_times), 5)
    total_execution_time = round(sum(execution_times), 5)

    results_redis["delete_redis"]["iterations"] = iterations
    results_redis["delete_redis"]["mean_time"] = mean_insert_time
    results_redis["delete_redis"]["variance_time"] = variance_insert_time
    results_redis["delete_redis"]["total_execution_time"] = total_execution_time

    return execution_times


@measure_execution_time
def update_ingredients_water_to_test():
    keys = redis_client.keys('recipe:*')
    for key in keys:
        recipe_data = redis_client.execute_command('JSON.GET', key)
        if recipe_data:
            ner_list = recipe_data.get('NER', [])
            if 'water' in ner_list:
                updated_ner = [ner.replace('water', 'TEST') for ner in ner_list]
                redis_client.execute_command('JSON.SET', key, '.', json.dumps(updated_ner))
                print(f'Updated {key}')


def perform_update_table_operations(iterations: int) -> list[float]:
    execution_times = []
    for _ in range(iterations):
        _, exec_time = update_ingredients_water_to_test()
        execution_times.append(exec_time)
        insert_data_to_redis(data)

    mean_insert_time = round(statistics.mean(execution_times), 5)
    variance_insert_time = round(statistics.variance(execution_times), 5)
    total_execution_time = round(sum(execution_times), 5)

    results_redis["update_redis"]["iterations"] = iterations
    results_redis["update_redis"]["mean_time"] = mean_insert_time
    results_redis["update_redis"]["variance_time"] = variance_insert_time
    results_redis["update_redis"]["total_execution_time"] = total_execution_time

    return execution_times


data = load_data_from_csv('dataset/full_dataset.csv', limit=10000)
perform_insert_table_operations(5, data)
perform_select_table_operations_most_common(100)
perform_select_table_operations_chicken(100)
perform_delete_table_operations_pie(100)
perform_update_table_operations(100)
write_results_to_excel(results_redis)
with open('redis_dict.json', 'w') as file:
    json.dump(results_redis, file, indent=4)

import time
import json
import statistics
import pandas as pd
from clickhouse_driver import Client
from os import getenv
from dotenv import load_dotenv, find_dotenv
from csv import DictReader


# find the .env file and load it
load_dotenv(find_dotenv())
host = getenv("HOST")
port = getenv("PORT")
user = getenv("USER")
database = getenv("DATABASE")

results = {
    "insert": {
        "iterations": 0,
        "total_execution_time": 0,
        "mean_time": 0,
        "variance_time": 0
    },
    "select": {
        "iterations": 0,
        "total_execution_time": 0,
        "mean_time": 0,
        "variance_time": 0,
        "iterations_2": 0,
        "total_execution_time_2": 0,
        "mean_time_2": 0,
        "variance_time_2": 0
    },
    "delete": {
        "iterations": 0,
        "total_execution_time": 0,
        "mean_time": 0,
        "variance_time": 0,
    },
    "update": {
        "iterations": 0,
        "total_execution_time": 0,
        "mean_time": 0,
        "variance_time": 0
    },
}

# Создаем клиент для подключения к ClickHouse
client = Client(host=host, port=port, user=user, database=database)
print(f"Connecting to ClickHouse at {host}:{port} with user {user}")

csv_file_path = 'dataset/full_dataset.csv'


def write_results_to_excel(results, filename='results.xlsx'):
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


@measure_execution_time
def drop_table_if_exists():
    drop_table_query = "DROP TABLE IF EXISTS recipes"
    try:
        client.execute(drop_table_query)
        print("Table 'recipes' dropped if it existed.")
    except Exception as e:
        print(f"Error dropping table: {e}")


def create_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS recipes
    (
        title String,
        ingredients Array(String),
        directions Array(String),
        link String,
        source LowCardinality(String),
        NER Array(String)
    ) ENGINE = MergeTree ORDER BY title;
    """
    try:
        client.execute(create_table_query)
        print("Table 'recipes' created or already exists.")
    except Exception as e:
        print(f"Error creating table: {e}")


def parse_array_string(value):
    if value.strip() == '':
        return []
    else:
        return value.strip()[1:-1].split(',')


def iter_csv(csv_file, limit=None):
    converters = {
        'ingredients': parse_array_string,
        'directions': parse_array_string,
        'NER': parse_array_string,
        'source': lambda x: x.strip()
    }

    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = DictReader(f)
        count = 0
        for line in reader:
            converted_line = {k: (converters[k](v) if k in converters else v) for k, v in line.items()}
            yield converted_line
            count += 1
            if limit and count >= limit:
                break


def truncate_table(table_name):
    truncate_query = f'TRUNCATE table {table_name}'
    try:
        client.execute(truncate_query)
        print(f'All records DELETED from {table_name}.')
    except Exception as e:
        print(f"Error deleting records: {e}")


@measure_execution_time
def insert_values(limit=None):
    insert_query = 'INSERT INTO recipes VALUES'
    try:
        client.execute(insert_query, iter_csv(csv_file_path, limit))
        print("Values inserted successfully")
    except Exception as e:
        print(f"Error inserting values: {e}")
        return


def perform_insert_table_operations(iterations: int, table_name: str, limit: int) -> list[float]:
    execution_times = []
    for _ in range(iterations):
        truncate_table(table_name)
        _, exec_time = insert_values(limit)
        execution_times.append(exec_time)

    mean_insert_time = round(statistics.mean(execution_times), 5)
    variance_insert_time = round(statistics.variance(execution_times), 5)
    total_execution_time = round(sum(execution_times), 5)

    results["insert"]["iterations"] = iterations
    results["insert"]["mean_time"] = mean_insert_time
    results["insert"]["variance_time"] = variance_insert_time
    results["insert"]["total_execution_time"] = total_execution_time

    return execution_times


@measure_execution_time
def select_most_common_ner():
    fetch_query = f"""
    SELECT
        arrayJoin(NER) AS k,
        count() AS c
    FROM recipes
    GROUP BY k
    ORDER BY c DESC
    LIMIT 50
    """
    try:
        client.execute(fetch_query)
        print("Query executed successfully.")
        # print(client.execute(fetch_query))
    except Exception as e:
        print(f"Error executing query: {e}")
        return


def perform_select_table_operations_most_common(iterations: int):
    execution_times = []
    for _ in range(iterations):
        _, exec_time = select_most_common_ner()
        execution_times.append(exec_time)

    mean_select_time = round(statistics.mean(execution_times), 5)
    variance_select_time = round(statistics.variance(execution_times), 5)
    total_execution_time = round(sum(execution_times), 5)

    results["select"]["iterations"] = iterations
    results["select"]["mean_time"] = mean_select_time
    results["select"]["variance_time"] = variance_select_time
    results["select"]["total_execution_time"] = total_execution_time

    return execution_times


@measure_execution_time
def select_recipes_chicken_parmesan():
    select_query = f"""
    SELECT
        arrayJoin(directions)
    FROM recipes
    WHERE title = 'Baked Chicken Parmesan';
    """
    try:
        client.execute(select_query)
        print('Query executed successfully.')
        # print(client.execute(select_query))
    except Exception as e:
        print(f'Error executing query: {e}')
        return


def perform_select_table_operations_chicken(iterations: int):
    execution_times = []
    for _ in range(iterations):
        _, exec_time = select_recipes_chicken_parmesan()
        execution_times.append(exec_time)

    mean_select_time = round(statistics.mean(execution_times), 5)
    variance_select_time = round(statistics.variance(execution_times), 5)
    total_execution_time = round(sum(execution_times), 5)

    results["select"]["iterations_2"] = iterations
    results["select"]["mean_time_2"] = mean_select_time
    results["select"]["variance_time_2"] = variance_select_time
    results["select"]["total_execution_time_2"] = total_execution_time

    return execution_times


@measure_execution_time
def delete_records_with_pie():
    delete_query = """
    ALTER TABLE recipes DELETE WHERE match(title, 'pie')
    """
    try:
        client.execute(delete_query)
        print("Records containing 'pie' in title deleted successfully.")
    except Exception as e:
        print(f'Error executing query: {e}')
        return


def create_backup_table():
    try:
        client.execute("DROP TABLE IF EXISTS recipes_backup")
        client.execute("CREATE TABLE recipes_backup AS recipes")
        client.execute("INSERT INTO recipes_backup SELECT * FROM recipes")
        print("Backup table created.")
    except Exception as e:
        print(f"Error creating backup table: {e}")


def restore_from_backup():
    try:
        client.execute("DROP TABLE IF EXISTS recipes")
        client.execute("CREATE TABLE recipes AS recipes_backup")
        client.execute("INSERT INTO recipes SELECT * FROM recipes_backup")
        print("Table restored from backup.")
    except Exception as e:
        print(f"Error creating backup table from backup: {e}")


def perform_delete_table_operations_pie(iterations: int):
    execution_times = []
    create_backup_table()
    for _ in range(iterations):
        _, exec_time = delete_records_with_pie()
        restore_from_backup()
        execution_times.append(exec_time)
    mean_select_time = round(statistics.mean(execution_times), 5)
    variance_select_time = round(statistics.variance(execution_times), 5)
    total_execution_time = round(sum(execution_times), 5)

    results["delete"]["iterations"] = iterations
    results["delete"]["mean_time"] = mean_select_time
    results["delete"]["variance_time"] = variance_select_time
    results["delete"]["total_execution_time"] = total_execution_time

    return execution_times


@measure_execution_time
def update_ingredients_water_to_test():
    update_query = """
    ALTER TABLE recipes 
    UPDATE NER = arrayMap(x -> replaceAll(x, ' "water"', ' "TEST"'), NER) 
    WHERE arrayExists(x -> x = ' "water"', NER);
    """
    try:
        client.execute(update_query)
        print('Ingredients updated successfully.')
    except Exception as e:
        print(f"Error updating ingredients: {e}")


def perform_update_table_operations(iterations: int):
    execution_times = []
    create_backup_table()
    for _ in range(iterations):
        _, exec_time = update_ingredients_water_to_test()
        restore_from_backup()
        execution_times.append(exec_time)
    mean_select_time = round(statistics.mean(execution_times), 5)
    variance_select_time = round(statistics.variance(execution_times), 5)
    total_execution_time = round(sum(execution_times), 5)

    results["update"]["iterations"] = iterations
    results["update"]["mean_time"] = mean_select_time
    results["update"]["variance_time"] = variance_select_time
    results["update"]["total_execution_time"] = total_execution_time

    return execution_times


create_table()
perform_insert_table_operations(10, 'recipes', 10000)
perform_select_table_operations_most_common(100)
perform_select_table_operations_chicken(100)
perform_delete_table_operations_pie(100)
perform_update_table_operations(100)
# write_results_to_excel(results, 'results.xlsx')
with open('clickhouse_dict.json', 'w') as file:
    json.dump(results, file, indent=4)


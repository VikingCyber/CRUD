import json

import matplotlib.pyplot as plt
import numpy as np

# Загрузка данных из JSON-файлов
with open('clickhouse_dict.json', 'r') as file, open('redis_dict.json', 'r') as file1:
    loaded_dict_clickhouse = json.load(file)
    loaded_dict_redis = json.load(file1)

merged_dict = loaded_dict_clickhouse | loaded_dict_redis


# Функция для построения столбчатых диаграмм
def plot_comparison_bar_charts(data, metric, title):
    operations = ['insert', 'select', 'update', 'delete']
    redis_operations = [op + '_redis' for op in operations]

    x = np.arange(len(operations))  # Положение столбцов
    width = 0.35  # Ширина столбца

    fig, ax = plt.subplots(figsize=(10, 6))

    values1 = [data[op].get(metric, 0) for op in operations]
    values2 = [data[op].get(metric, 0) for op in redis_operations]

    bars1 = ax.bar(x - width/2, values1, width, label='ClickHouse')
    bars2 = ax.bar(x + width/2, values2, width, label='Redis')

    ax.set_xlabel('CRUD Operations')
    ax.set_ylabel(metric.capitalize().replace('_', ' '))
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(operations)
    ax.legend()

    # Добавление подписей значений на столбцы
    for bars in [bars1, bars2]:
        for bar in bars:
            yval = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2, yval, round(yval, 2), va='bottom')

    fig.tight_layout()


# Построение диаграммы для каждой метрики
metrics = ['iterations', 'total_execution_time', 'mean_time', 'variance_time']
for metric in metrics:
    plot_comparison_bar_charts(merged_dict, metric, f'Comparison of {metric.capitalize().replace("_", " ")} (ClickHouse vs Redis)')

plt.show()




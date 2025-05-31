
# Инструкция по запуску ETL-процесса

## 1. Запуск всех сервисов

Сначала запускаем инфраструктуру:

```bash
docker-compose up -d
````

Это поднимет контейнеры с PostgreSQL, Spark, ClickHouse и т.д.

---

## 2. Загрузка данных в PostgreSQL (звёздная схема)

Выполняем ETL из таблицы `mock_data` в таблицы схемы `star_schema` в PostgreSQL:

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --jars /opt/spark/jars/postgresql-42.6.0.jar \
  /opt/spark_scripts/etl_postgres.py
```

---

## 3. Построение отчётов и загрузка в ClickHouse

Создаются агрегированные отчёты и заливаются в ClickHouse:

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --jars /opt/spark/jars/postgresql-42.6.0.jar,/opt/spark/jars/clickhouse-jdbc-0.4.6.jar \
  /opt/spark_scripts/etl_clickhouse.py
```

---

## 4. Посмотреть результат в ClickHouse

Можно подключиться из терминала:

```bash
docker exec -it clickhouse clickhouse-client
```

Примеры запросов:

```sql
SHOW TABLES;

SELECT * FROM report_products LIMIT 10;

SELECT * FROM report_customers ORDER BY total_spent DESC LIMIT 10;
```

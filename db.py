from datetime import datetime, timedelta
import psycopg2
import random
import string


# Соединение с БД
def create_connection(dbname, user, password, host='localhost', port='6432'):
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port)


# Создание таблицы
def create_table(cur):
    cur.execute("""
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(8),
            age INTEGER,
            created_at TIMESTAMP,
            balance NUMERIC(10, 2),
            is_active BOOLEAN
        )
    """)
    print('Таблица создана')


# Генерация случайного имени
def random_string(length=8):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


# Создание случайных данных
def generate_random_data():
    name = random_string(8).capitalize()
    age = random.randint(18, 45)
    created_at = datetime.now() - timedelta(days=random.randint(0, 365))
    balance = round(random.uniform(1000, 100000), 2)
    is_active = random.choice([True, False])
    return (name, age, created_at, balance, is_active)


# Вставка данных в таблицу
def insert_data(cur, data):
    cur.execute("""
        INSERT INTO test_table (name, age, created_at, balance, is_active)
        VALUES (%s, %s, %s, %s, %s)
    """, data)


# Генерация и вставка нескольких записей
def populate_table(cur, count):
    data = [generate_random_data() for _ in range(count)]
    list(map(lambda record: insert_data(cur, record), data))
    print('Таблица заполнена')


def main():
    with create_connection(
            dbname="test_db",
            user="postgres",
            password="postgres"
    ) as conn:
        with conn.cursor() as cur:
            create_table(cur)
            populate_table(cur, 20)
        conn.commit()


if __name__ == "__main__":
    main()

import json
import os
from psycopg2.extras import RealDictCursor
from ftplib import FTP

from db import create_connection


# Считывание данных из таблицы
def fetch_data(cur):
    cur.execute("SELECT * FROM test_table")
    return cur.fetchall()


# Сохранение данных в файл JSON
def save_to_json(data, filename='data.json'):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4, default=str)
        print(f'Файл {filename} успешно сохранен в директории {os.path.abspath(filename)}')


# Загрузка файла на FTP-сервер
def upload_to_ftp(filename, ftp_host, ftp_user, ftp_pass, ftp_dir='/'):
    try:
        with FTP(ftp_host) as ftp:
            ftp.login(user=ftp_user, passwd=ftp_pass)
            ftp.cwd(ftp_dir)  # Переход в нужную директорию на сервере, если требуется
            with open(filename, 'rb') as f:
                ftp.storbinary(f'STOR {filename}', f)
                print(f'Файл {filename} успешно загружен на FTP-сервер')
    except Exception as ex:
        print(f'Ошибка: {ex}')


def main():
    with create_connection(dbname="test_db", user="postgres", password="postgres") as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            conn.commit()
            data = fetch_data(cur)
            save_to_json(data)
            upload_to_ftp(
                filename='data.json',
                ftp_host="ftp_host",
                ftp_user="ftp_username",
                ftp_pass="ftp_password"
            )


if __name__ == "__main__":
    main()

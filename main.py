import psycopg2


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS clients(
                        client_id SERIAL PRIMARY KEY,
                        first_name VARCHAR(40) NOT NULL,
                        last_name VARCHAR(40) NOT NULL,
                        email VARCHAR(80) NOT NULL UNIQUE,
                        phones TEXT ARRAY UNIQUE
                    );
                    """)
        conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
                    INSERT INTO clients(first_name, last_name, email, phones)
                    VALUES (%s, %s, %s, %s) 
                    RETURNING *;
                    """, (first_name, last_name, email, phones))
        print(cur.fetchall())
        conn.commit()


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
                    UPDATE clients 
                    SET phones = ARRAY_APPEND(phones, %s)
                    WHERE client_id = %s 
                    RETURNING phones;
                    """, (phone, client_id))
        print(cur.fetchone())
        conn.commit()


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    arg_list = {'first_name': first_name, "last_name": last_name, 'email': email, "phones": phones}
    with conn.cursor() as cur:
        for key, arg in arg_list.items():
            if arg:
                cur.execute("""
                            UPDATE clients SET {} = %s 
                            WHERE client_id = %s 
                            RETURNING *;
                            """.format(key), (arg, client_id))
        print(cur.fetchall())
        conn.commit()


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
                    UPDATE clients 
                    SET phones = ARRAY_REMOVE(phones, %s) 
                    WHERE client_id = %s 
                    RETURNING phones;
                    """, (phone, client_id))
        print(cur.fetchone())
        conn.commit()


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute(""" DELETE FROM clients WHERE client_id = %s;""", (client_id, ))
        conn.commit()


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    arg_list = {'first_name': first_name, "last_name": last_name, 'email': email, "phones": phone}
    with conn.cursor() as cur:
        for key, arg in arg_list.items():
            if arg and key != 'phones':
                cur.execute("""
                            SELECT * FROM clients
                            WHERE {} = %s
                            """.format(key), (arg, ))
                print(cur.fetchall())
                break

            if arg and key == 'phones':
                cur.execute("""
                            SELECT * FROM clients
                            WHERE %s = ANY({})
                            """.format(key), (arg, ))
                print(cur.fetchall())
                break

        conn.commit()


with psycopg2.connect(database="clients_db", user="postgres", password="egor3107E", host="localhost") as conn:
    with conn.cursor() as cur:
        cur.execute("""DROP TABLE clients;""")

    create_db(conn)
    add_client(conn, 'William', 'Smith', 'william8smith@example.ru', ['(123) 123-4567'])
    add_client(conn, 'Sten', 'Lee', 'sten5lee@example.ru')
    add_client(conn, 'Jonathan', 'Grey', 'jonathan7grey@example.ru', ['(178) 178-7698'])
    add_phone(conn, 1, '(134) 134-4857')
    add_phone(conn, 2, '(167) 167-6893')
    add_phone(conn, 2, '(189) 189-9578')
    change_client(conn, 1, 'Henry')
    delete_phone(conn, 1, '(123) 123-4567')
    delete_client(conn, 1)
    find_client(conn, phone='(189) 189-9578')
    find_client(conn, phone='(178) 178-7698')
    find_client(conn, first_name='Sten')
    find_client(conn, last_name='Smith')
    find_client(conn, email='jonathan7grey@example.ru')

conn.close()

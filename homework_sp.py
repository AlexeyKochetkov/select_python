import psycopg2
from pprint import pprint

def create_db(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients(
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(20),
        last_name VARCHAR(40),
        email VARCHAR(50)
        );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS phones(
        phone VARCHAR(11) PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id)
        );
    """)

def delete_table(cur):
    cur.execute("""
        DROP TABLE phones;
        DROP TABLE clients;
        """)
    
def add_phone(cur, client_id, phone):
    cur.execute("""
        INSERT INTO phones(client_id, phone)
        VALUES (%s, %s);
        """, (client_id, phone))
    return client_id

def add_client(cur, first_name, last_name, email, phone=None):
    cur.execute("""
        INSERT INTO clients(first_name, last_name, email)
        VALUES (%s, %s, %s);
        """, (first_name, last_name, email))
    
    cur.execute("""
        SELECT id from clients
        ORDER BY id DESC
        LIMIT 1;
        """)
    id = cur.fetchone()[0]
    if phone is not None:
        add_phone(cur, id, phone)

def change_client(cur, id, first_name=None, last_name=None, email=None, phone=None):
    cur.execute("""
        SELECT * from clients c
                LEFT JOIN phones p ON c.id = p.client_id
        WHERE id = %s;
        """, (id, ))
    
    client = cur.fetchone()
    if first_name is None:
        first_name = client[1]
    if last_name is None:
        last_name = client[2]
    if email is None:
        email = client[3]
    if phone is None:
        phone = client[4]
    
    cur.execute("""
        UPDATE clients
        SET first_name = %s, last_name = %s, email =%s 
        WHERE id = %s;
        """, (first_name, last_name, email, id))
    
    cur.execute("""
        UPDATE phones
        SET phone = %s
        WHERE client_id = %s;
        """, (phone, id))
    
def delete_phone(cur, phone):
    cur.execute("""
        SELECT client_id 
                FROM phones
        WHERE phone = %s;
        """, (phone, ))
    
    client_id = cur.fetchone()[0]
    
    cur.execute("""
        DELETE FROM phones 
        WHERE phone = %s;
        """, (phone, ))
    
    cur.execute("""
        SELECT * 
                FROM clients
        WHERE id = %s;
        """, (client_id, ))
    pprint(cur.fetchall())

def delete_client(cur, id):
    cur.execute("""
        SELECT * from clients c
                LEFT JOIN phones p ON c.id = p.client_id
        WHERE id = %s;
        """, (id, ))
    
    client = cur.fetchall()

    cur.execute("""
        DELETE FROM phones
        WHERE client_id = %s
        """, (id, ))
    
    cur.execute("""
        DELETE FROM clients 
        WHERE id = %s
       """, (id, ))
    
    pprint(client)

def find_client(cur, first_name=None, last_name=None, email=None, phone=None):
    if first_name is not None:
            cur.execute("""
                SELECT * FROM clients c
                        LEFT JOIN phones p ON c.id = p.client_id
                WHERE c.first_name = %s;
                """, (first_name, ))
            
    if last_name is not None:
            cur.execute("""
                SELECT * FROM clients c
                        LEFT JOIN phones p ON c.id = p.client_id
                WHERE c.last_name = %s;
                """, (last_name, ))
            
    if email is not None:
            cur.execute("""
                SELECT * FROM clients c
                        LEFT JOIN phones p ON c.id = p.client_id
                WHERE c.email = %s;
                """, (email, ))
            
    if phone is not None:
            cur.execute("""
                SELECT * FROM clients c
                        LEFT JOIN phones p ON c.id = p.client_id
                WHERE p.phone = %s;
                """, (phone, ))

    pprint(cur.fetchall())
    

if __name__ == '__main__':
    with psycopg2.connect(database="clients_db", user="postgres", password="") as conn:
        with conn.cursor() as cur:
            # 1. Cоздаем структуру БД
            delete_table(cur)
            create_db(cur)
            print("Структура БД создана!")

            # 2. Добавляем клиентов
            add_client(cur, "Имя", "Фамилия", "add@test.ru", 70000000000)
            add_client(cur, "Имя2", "Фамилия2", "add@test.ru2", 70000000002)
            add_client(cur, "Имя3", "Фамилия3", "add@test.ru3")

            print("\nДобавлены клиенты:")
            cur.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, p.phone FROM clients c
                LEFT JOIN phones p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(cur.fetchall())

            # 3. Добавляем клиентам номера телефонов
            add_phone(cur, 2, 70000000003)
            add_phone(cur, 3, 70000000004)

            print("\nОбновлена информация по телефонам клиентов:")
            cur.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, p.phone FROM clients c
                LEFT JOIN phones p ON c.id = p.client_id
                WHERE p.client_id in (2,3)
                ORDER by c.id
                """)
            pprint(cur.fetchall())

             # 4. Изменим данные клиента
            change_client(cur, 3, "Имя33", "Фамилия33", "add@test.ru33", None)
            change_client(cur, 1, None, None, None, 70000000011)
            
            print("\nИзменена информация по клиентам:")
            cur.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, p.phone FROM clients c
                LEFT JOIN phones p ON c.id = p.client_id
                WHERE c.id in (1,3)
                ORDER by c.id
                """)
            pprint(cur.fetchall())
            
            # 5. Удаляем номер телефона клиента
            print("\nУдален номер телефона клиента:")
            delete_phone(cur, '70000000011')

            # 6. Удаляем клиента
            print("\nУдален клиент:")
            delete_client(cur, 2)

            # 7. Ищем клиента
            print("\nНайден клиент по имени:")
            find_client(cur, "Имя33", None, None, None)

            print("\nНайден клиент по фамилии:")
            find_client(cur, None, "Фамилия", None, None)

            print("\nНайден клиент по email:")
            find_client(cur, None, None, "add@test.ru", None)
            
            print("\nНайден клиент по телефону:")
            find_client(cur, None, None, None, "70000000004")                   
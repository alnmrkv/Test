import psycopg2
import pandas as pd
from sshtunnel import SSHTunnelForwarder


def initServer():
    try:
        server = SSHTunnelForwarder(("89.108.99.201", 22),
                                    ssh_username='testuser',
                                    ssh_password='TEST_PAROL',
                                    remote_bind_address=("localhost", 5432))
        server.start()
        print("Server start")
        params = {
            'database': 'postgres',
            'user': 'testuser',
            'password': 'TEST_PAROL',
            'host': 'localhost',
            'port': server.local_bind_port
        }
        conn = psycopg2.connect(**params)
        print("Connection start")
    except Exception as e:
        print(e)
    return server, conn

def get_data_to_csv(curs):
    curs.execute(
        """
        SELECT x.value, y.value
        FROM x_values x
        JOIN y_values y ON x.id = y.x_value_id
        ORDER BY x.id
        """
    )
    data = curs.fetchall()
    df = pd.DataFrame(data, columns=['x_value', 'y_value'])
    df.to_csv("data.csv", index=False)

def main():
    try:
        server, conn = initServer()
        curs = conn.cursor()
        # Create two tables
        curs.execute(
            '''CREATE TABLE IF NOT EXISTS x_values (id SERIAL PRIMARY KEY, value real)''')
        curs.execute(
            '''CREATE TABLE IF NOT EXISTS y_values (id SERIAL PRIMARY KEY, value real, x_value_id bigint REFERENCES x_values (id))''')

        # Fill two tables
        curs.execute(
            """
        INSERT INTO x_values (value)
        SELECT generate_series(1, 1000000)
        """
        )
        curs.execute(
            """
            INSERT INTO y_values (x_value_id, value)
            SELECT id AS x_value_id, (value * value) as value
            FROM x_values;
            """
        )
        conn.commit()
        get_data_to_csv(curs)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        server.close()
main()
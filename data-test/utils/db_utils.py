import psycopg2


def get_db_connection(config: dict):
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["database"],
        user=config["user"],
        password=config["password"],
    )


def fetch_customer_records(config: dict):
    conn = get_db_connection(config)
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {config['table']} ORDER BY customer_id")
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return [dict(zip(colnames, row)) for row in rows]


def count_records(config: dict):
    conn = get_db_connection(config)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {config['table']}")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count

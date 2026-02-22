from src.database import db_connection

with db_connection() as conn:
    cursor = conn.cursor()
    cursor.execute('SELECT version()')
    print("✅ Database connection successful!")
    print(cursor.fetchone()[0])

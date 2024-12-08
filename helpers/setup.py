import sqlite3


def initialize_database_tables (database_name: str):
    tables = [
        """
        CREATE TABLE IF NOT EXISTS persons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trial_name TEXT UNIQUE NOT NULL
            trial_stage INTEGER 
            curr_index INTEGER
        )
        """
    ]

    with sqlite3.connect(database_name) as conn:
        cursor = conn.cursor()
        for table in tables:
            cursor.execute(table)
        conn.commit()
        print("All tables created successfully.")




def initialize_trial_tables(conn: sqlite3.Connection):
    tables = [
    """
    CREATE TABLE IF NOT EXISTS embeddings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        person_id INTEGER NOT NULL,              
        embedding BLOB NOT NULL,                 
        FOREIGN KEY (person_id) REFERENCES persons (id)
            ON DELETE CASCADE                    
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS centers (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        embedding BLOB NOT NULL                  
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS results (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        person_id INTEGER NOT NULL,              
        vector BLOB NOT NULL,                    
        FOREIGN KEY (person_id) REFERENCES persons (id)
            ON DELETE CASCADE                    
    )
    """
    ]

    cursor = conn.cursor()
    for table in tables:
        cursor.execute(table)
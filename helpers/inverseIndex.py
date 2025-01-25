import sqlite3

class InverseIndex:
    def __init__(self, conn: sqlite3.Connection, table_name: str):
        """
        Initializes the InverseIndex class.

        Args:
            conn (sqlite3.Connection): SQLite database connection.
            table_name (str): Name of the inverse index table.
        """
        self.conn = conn
        self.table_name = table_name

    def add_result_vector(self, topic_ids: list[int], person_id: int, feature_count: int) -> None:
        """
        Adds a result vector for a user to the inverse index.

        Args:
            topic_ids (list[int]): List of topic IDs corresponding to the result vector.
            person_id (int): ID of the person associated with the result vector.
            feature_count (int): Total number of features (1s) in the result vector.
        """
        cursor = self.conn.cursor()
        query = f"""
        INSERT INTO {self.table_name} (topic_id, person_id, feature_count)
        VALUES (?, ?, ?)
        """
        for topic_id in topic_ids:
            cursor.execute(query, (topic_id, person_id, feature_count))
        self.conn.commit()

    def get_all_documents_by_topics(self, topic_ids: list[int], distinct: bool = True) -> list[int]:
        """
        Retrieves documents (person IDs) associated with multiple topic IDs.

        Args:
            topic_ids (list[int]): List of topic IDs to query.
            distinct (bool): Whether to retrieve distinct person IDs or allow duplicates.

        Returns:
            list[int]: List of person IDs matching any of the topic IDs.
        """
        cursor = self.conn.cursor()
        distinct_clause = "DISTINCT" if distinct else ""
        placeholders = ", ".join("?" for _ in topic_ids)
        query = f"""
        SELECT {distinct_clause} person_id
        FROM {self.table_name}
        WHERE topic_id IN ({placeholders})
        """
        cursor.execute(query, topic_ids)
        return [row[0] for row in cursor.fetchall()]

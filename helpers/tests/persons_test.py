from ..persons import Persons
from ..setup import initialize_database_tables
import sqlite3

def test_persons():
    with sqlite3.connect(':memory:') as conn:
        initialize_database_tables(conn)
        p = Persons(conn)
        
        descriptions = ["Hello World!", "Goodbye World."]

        assert(len(p.getDescriptionBatch(1, 99)) == 0)
        p.insertDescription(0, descriptions[0])
        p.insertDescription(1, descriptions[1])

        people = p.getDescriptionBatch(0, 2)
        assert(len(people) == 2)

        for i in range(2):
            id, desc = people[i]
            assert(id == i)
            assert(desc == descriptions[i])
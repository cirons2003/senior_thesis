from ..centers import Centers
from ..setup import initialize_centers_table
import sqlite3
import pytest 

def test_centers(): 
    centers = [[1,2,3], [4,5,6], [7,8,9]]
    table_name = 'testCenters'

    with sqlite3.connect(':memory:') as conn:
        initialize_centers_table(conn, table_name)
        c = Centers(conn, table_name, 3)
        assert(c.get_centers() == [])

        ## test validation 
        with pytest.raises(AssertionError): 
            c.update_centers(centers[0:1])
        with pytest.raises(AssertionError): 
            c.update_centers(centers[1:2])

        # test center update
        c.update_centers(centers)
        assert(c.get_centers() == centers)

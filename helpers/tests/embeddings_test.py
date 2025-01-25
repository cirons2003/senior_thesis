from ..embeddings import Embeddings
from ..setup import initialize_embeddings_table
import sqlite3

table_name = 'test_embeddings'
embedding_list = [[1, 2, 3], [3, 2, 1], [7, 1, 4]]

def test_embeddings():
    with sqlite3.connect(':memory:') as conn: 
        initialize_embeddings_table(conn, table_name)
        e = Embeddings(conn, table_name)

        for i in range(len(embedding_list)):
            e.insertEmbedding(1, embedding_list[i], i, 3)

        # Test standard batch retrieval
        for epoch in range(7):
            last_id, embeddings = e.getEmbeddingBatch(1, 99, epoch)
            assert(last_id == epoch % 3 + 1) # 1-indexed in sqlite
            assert(embeddings[0] == embedding_list[epoch % 3])

        other_emb = [9, 10, 11]
        e.insertEmbedding(2, other_emb, 0, 1)
        assert(len(e.getEmbeddingBatch(1, 2, 0)) == 2)

        # Test person batch retrieval (below)
        
        # Test for single person batch
        tups = e.getPersonEmbeddingsBatch(1, 1)
        assert(len(tups) == 1)

        pid, ecount, embs = tups[0]
        assert(pid == 1)
        assert(ecount == 3)
        
        for i in range(3): 
            assert(embs[i] == embedding_list[i])

        tups = e.getPersonEmbeddingsBatch(2, 1)
        assert(len(tups) == 1)

        pid, ecount, embs = tups[0]
        assert(pid == 2) 
        assert(ecount == 1)
        assert(embs[0] == other_emb)

        all_tups = e.getPersonEmbeddingsBatch(0, 99)
        assert(len(all_tups) == 2)
        assert(len(all_tups[0][2]) == 3) 
        assert(len(all_tups[1][2]) == 1)

        pid, ecount, embs = all_tups[0]
        assert(pid == 1)
        assert(ecount == 3)

        # Test for multi-person batch
        for i in range(3):  
            assert(embs[i] == embedding_list[i])

        pid, ecount, embs = all_tups[1]
        assert(pid == 2) 
        assert(ecount == 1)
        assert(embs[0] == other_emb)
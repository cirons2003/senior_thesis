from ..prototypes import ChunkingAgent
import random 

def chunk_description_batch(chunking_agent: ChunkingAgent, batch: list[tuple[int, str]], deterministic = False):
        """
        Takes in a batch of tuples (id, text)
        Returns a tuple with: 
        1) ids | a list of ids 
        2) sizes | a list of sizes
        3) embedding_ids | a list of ids
        4) chunks | a list of chunks
        Such that for each i, chunks[i] corresponds to tuple with id ids[i] 
        and sizes[i] is # of chunks from that tuple, and embedding_ids[i] is the chunks unique id in range [0, sizes[i])
        """

        ids = []
        sizes = []
        embedding_ids = []
        chunks = []
        for id, text in batch: 
            text_chunks = chunking_agent.chunk(text)

            if not deterministic: 
                # remove positional correlations
                random.shuffle(text_chunks)

            for i, text_chunk in enumerate(text_chunks):
                ids.append(id)
                sizes.append(len(text_chunks))
                embedding_ids.append(i)
                chunks.append(text_chunk)

        return (ids, sizes, embedding_ids, chunks)
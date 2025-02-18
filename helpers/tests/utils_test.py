from ..utils.chunkDescriptionBatch import chunk_description_batch

class TestChunker: 
    name = 'Carl'
    def chunk(self, text):
        return [text ]

def test_chunk_description_batch(): 
    test_input = [(1, "hello"), (2, "there")]
    expected_output = ([1, 2], [1, 1], [0, 0], ["hello", "there"])

    output = chunk_description_batch(TestChunker(), test_input, deterministic = True)

    assert output == expected_output
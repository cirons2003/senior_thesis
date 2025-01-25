import re

class SentenceChunkingAgent:

    def __init__(self):
        self.name = 'FL_Sentence'

    def chunk(self, raw_text: str) -> list[str]:
        """
        Split the input text into sentences based on punctuation.

        Args:
            raw_text (str): The text to chunk.

        Returns:
            List[str]: A list of sentences.
        """
        return re.split(r'(?<=[.!?]) +', raw_text)

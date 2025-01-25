import re
from typing import List

class ParagraphChunkingAgent:
    def __init__(self):
        self.name = 'FL_Paragraph'

    def chunk(self, raw_text: str) -> List[str]:
        """
        Split the input text into paragraphs based on newline characters.

        Args:
            raw_text (str): The text to chunk.

        Returns:
            List[str]: A list of paragraphs.
        """ 
        return re.split(r'\r?\n', raw_text)

from IPython.display import display, update_display, Markdown
import html

class UpdatePrinter:
    """
    Creates a reserved vertical write buffer and allows dynamic updates in Jupyter Notebook.
    Supports normal print() statements before and after the buffer.
    """
    def __init__(self, maxLineLength: int = 50):
        if maxLineLength < 0:
            raise ValueError(f"Max line length cannot be negative. Received: {maxLineLength}")

        self.bufferHeight = 0
        self.maxLineLength = maxLineLength
        self.buffer = []  # Stores the buffer lines
        self.ready = False
        self.display_id = None  # Unique display ID for updating buffer output

    def reload(self, bufferHeight: int):
        if bufferHeight < 0:
            raise ValueError(f"Buffer height cannot be negative. Received: {bufferHeight}")

        self.bufferHeight = bufferHeight
        self.buffer = ["..." for _ in range(bufferHeight)]  # Initialize buffer
        self.ready = True

        # Display the buffer for the first time
        self.display_id = display(Markdown(self.__formattedBuffer()), display_id=True).display_id

    def update_message_level(self, message: str, level: int = 0):
        """
        Write a message into Message Buffer at `level` lines below top.
        """
        if not self.ready:
            raise BufferError("No buffer established... reload UpdatePrinter and try again.")

        if len(message) > self.maxLineLength:
            message = message[:self.maxLineLength - 3] + "..."  # Truncate instead of error

        if level < 0 or level >= self.bufferHeight:
            raise ValueError(f"Level {level} is out of bounds. Buffer height: {self.bufferHeight}")

        self.buffer[level] = message  # Update the buffer
        self.__refresh_output()

    def release_levels(self, release_levels: int):
        """
        Clears and releases `release_levels` lowest levels.
        """
        if release_levels == 0 or self.ready == False:
            return

        if release_levels > self.bufferHeight:
            raise ValueError(f"Cannot release more levels than buffer height. BufferHeight: {self.bufferHeight}, received: {release_levels}")

        if release_levels < 0:
            raise ValueError(f"Release levels cannot be negative (received {release_levels})")

        self.buffer = self.buffer[:-release_levels]  # Remove last `release_levels` lines
        self.bufferHeight -= release_levels
        self.__refresh_output()

    def finish(self):
        """
        Unmounts printer and leaves current buffer displayed
        """
        if not self.ready:
            return
        self.__refresh_output()
        self.ready = False

    def __refresh_output(self):
        """Refreshes only the buffer display without clearing other Jupyter outputs."""
        update_display(Markdown(self.__formattedBuffer()), display_id=self.display_id)

    def __formattedBuffer(self):
        escaped_text = html.escape("\n".join(self.buffer))
        css = 'style="font-family: Courier; margin:2px; padding-left:40px; line-height:1;"'
        return f'<pre {css}>{escaped_text}</pre>'


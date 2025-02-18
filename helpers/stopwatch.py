import datetime

class Stopwatch:
    def __init__(self): 
        self.start_time = datetime.datetime.now()

    def measure(self): 
        delta = datetime.datetime.now() - self.start_time  
        return f"{delta.seconds // 60} min {delta.seconds % 60} sec"
    
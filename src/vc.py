import json


class VectorClock:
    def __init__(self, x):
        if isinstance(x, int):
            self.clock = [0 for _ in range(x)]
        elif isinstance(x, list):
            self.clock = list()
            for elem in x:
                self.clock.append(elem)
        elif isinstance(x, str):
            l = json.loads(x)
            self.clock = list()
            for elem in l:
                self.clock.append(elem)
        else:
            raise Exception("Bad VectorClock construction")

    def __lt__(self, that):   # happens before
        if len(self.clock) != len(that.clock):
            raise Exception("Comparing Vector Clocks of different sizes")
        smaller = False
        for i in range(len(self.clock)):
            if self.clock[i] < that.clock[i]:
                smaller = True
                break
            if that.clock[i] < self.clock[i]:
                return False
        return smaller

    def __gt__(self, other):   # happens after
        return other < self

    def __eq__(self, other):    # __eq__ means they are concurrent
        return (not (self < other)) and (not (other < self))

    def __str__(self):
        return json.dumps(self.clock)

    def add(self):
        self.clock.append(0)

    def max(self,other):
        if len(self.clock) != len(other.clock):
            raise Exception("Cannot max two clocks of different sizes")
        for i in range(len(self.clock)):
            self.clock[i] = max(other.clock[i], self.clock[i])

vc = VectorClock(0)

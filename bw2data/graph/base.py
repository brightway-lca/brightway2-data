class GraphBackend:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"GraphBackend({self.name})"

    def __str__(self):
        return self.name

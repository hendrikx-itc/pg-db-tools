from typing import List


class Modification:
    pass


class DropColumn(Modification):
    def __init__(self, table, column):
        self.table = table
        self.column = column


class AddColumn(Modification):
    def __init__(self, table, column):
        self.table = table
        self.column = column


class Diff:
    steps: List[Modification]

    def __init__(self, steps: List[Modification]=None):
        if steps is None:
            self.steps: List[Modification] = []
        else:
            self.steps: List[Modification] = steps

class CreateTypeError(Exception):
    def __init__(self, message):
        super().__init__(message)


class CreateTableError(Exception):
    def __init__(self, message):
        super().__init__(message)


# https://www.python.org/dev/peps/pep-0249/#error


class Error(StandardError):
    pass

class DatabaseError(Error):
    pass

class ProgrammingError(DatabaseError):
    pass

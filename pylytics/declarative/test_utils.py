from contextlib import contextmanager
import pickling


@contextmanager
def load():
    pass


# Use case:
# with unpickle(my_function)

# context lib doesn't really make sense ...
# -> it's just the same as passing something into a function.

# so we're going to store the output of a particular function in a file e.g.
# foo.pickled

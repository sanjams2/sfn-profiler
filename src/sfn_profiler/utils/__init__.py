import argparse
from typing import Optional, Any

def get_hostname():
    import socket
    return socket.gethostname()


def file_arg_action(*args, **kwargs):
    class FlattenAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            # Process each value with our custom type function
            result = []
            for value in values:
                if value and isinstance(value, str) and value.startswith("file://"):
                    with open(value[len("file://"):]) as f:
                        result.extend([line.rstrip() for line in f.readlines()])
                else:
                    result.append(value)
            # Set the attribute to our flattened list
            setattr(namespace, self.dest, result)
    return FlattenAction


def noop_context(c: Optional[Any]) -> Optional[Any]:
    if not c:
        return None
    class NoopContext:
        def __init__(self, c):
            self.c = c

        def __enter__(self):
            return c

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
    return NoopContext(c)
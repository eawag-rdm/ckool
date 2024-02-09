from functools import wraps


def log_exceptions(logger):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                e.add_note(f"Exception occurred in {func.__name__}")
                logger.exception(e)

        return wrapper

    return decorator

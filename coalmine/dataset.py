
class Dataset:
    pass


def register_builder(name):
    def _wrapper(Class):
        def _op(*args, **kwargs):
            return Class(*args, **kwargs)
        setattr(Dataset, name, _op)
        return Class
    return _wrapper


def register_operation(name):
    def _wrapper(Class):
        def _op(self, *args, **kwargs):
            return Class(self, *args, **kwargs)
        setattr(Dataset, name, _op)
        return Class
    return _wrapper

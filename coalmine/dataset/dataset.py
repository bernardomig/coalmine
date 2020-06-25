
class Dataset:
    pass


def register_op(name, type='method'):

    def wrapper(Class):
        """dsa"""

        if type == 'method':
            def _operation(self, *args, **kwargs):
                return Class(self, *args, **kwargs)
        elif type == 'staticmethod':
            @staticmethod
            def _operation(*args, **kwargs):
                return Class(*args, **kwargs)
        else:
            raise ValueError("type should be either 'rangemethod' or 'staticmethod', not {}"
                             .format(type))

        setattr(Dataset, name, _operation)
        getattr(Dataset, name).__doc__ = Class.__doc__
        getattr(Dataset, name).__name__ = name
        return Class

    return wrapper

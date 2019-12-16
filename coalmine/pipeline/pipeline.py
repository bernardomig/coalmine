
class Pipeline:
    pass


def register_pipeline_op(name, type='method'):
    def wrapper(Class):
        if type == 'method':
            def _operation(self, *args, **kwargs):
                return Class(self, *args, **kwargs)
        elif type == 'staticmethod':
            @staticmethod
            def _operation(*args, **kwargs):
                return Class(*args, **kwargs)
        else:
            raise ValueError("type should be either 'method' or 'staticmethod', not {}"
                             .format(type))

        setattr(Pipeline, name, _operation)
        return Class

    return wrapper

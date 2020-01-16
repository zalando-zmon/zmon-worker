import __future__
import ast
from inspect import isclass


class InvalidEvalExpression(Exception):
    pass


class ProtectedPartial(object):
    '''
    Provides functools.partial functionality with one additional feature: if keyword arguments contain '__protected'
    key with list of arguments as value, the appropriate values will not be overwritten when calling the partial. This
    way we can prevent user from overwriting internal zmon parameters in check command. The protected key uses double
    underscore to prevent overwriting it, we reject all commands containing double underscores.
    '''

    def __init__(self, func, *args, **kwargs):
        self.__func = func
        self.__partial_args = args
        self.__partial_kwargs = kwargs
        self.__protected = frozenset(kwargs.get('__protected', []))
        self.__partial_kwargs.pop('__protected', None)

    def __call__(self, *args, **kwargs):
        new_kwargs = self.__partial_kwargs.copy()
        new_kwargs.update((k, v) for (k, v) in kwargs.iteritems() if k not in self.__protected)
        return self.__func(*self.__partial_args + args, **new_kwargs)


def check_ast_node_is_safe(node, source):
    '''
    Check that the ast node does not contain any system attribute calls
    as well as exec call (not to construct the system attribute names with strings).

    eval() function calls should not be a problem, as it is hopefuly not exposed
    in the globals and __builtins__

    >>> node = ast.parse('def __call__(): return 1')
    >>> node == check_ast_node_is_safe(node, '<source>')
    True

    >>> node = ast.parse('instance._Instance__request')
    >>> check_ast_node_is_safe(node, '<source>')
    Traceback (most recent call last):
        ...
    InvalidEvalExpression: <source> should not try to access hidden attributes (for example '__class__')

    >>> check_ast_node_is_safe(ast.parse('def m(): return ().__class__'), '<hidden>')
    Traceback (most recent call last):
        ...
    InvalidEvalExpression: <hidden> should not try to access hidden attributes (for example '__class__')


    >>> check_ast_node_is_safe(ast.parse('def horror(g): exec "exploit = ().__" + "class" + "__" in g'), '<horror>')
    Traceback (most recent call last):
        ...
    InvalidEvalExpression: <horror> should not try to execute arbitrary code


    >>> check_ast_node_is_safe(ast.parse('def __exit__(): return 1'), '<hidden>')
    Traceback (most recent call last):
        ...
    InvalidEvalExpression: <hidden> should not try to define __exit__ method as it leaks hidden data
    '''

    for n in ast.walk(node):
        if isinstance(n, ast.Attribute):
            if '__' in n.attr:
                raise InvalidEvalExpression(
                    "{} should not try to access hidden attributes (for example '__class__')".format(source))
        elif isinstance(n, ast.Exec):
            raise InvalidEvalExpression('{} should not try to execute arbitrary code'.format(source))
        elif isinstance(n, ast.FunctionDef) and n.name == '__exit__':
            raise InvalidEvalExpression(
                '{} should not try to define __exit__ method as it leaks hidden data'.format(source))
    return node


def safe_eval(expr, eval_source='<string>', **kwargs):
    '''
    Safely execute expr.

    For now expr can be only one python expression, a function definition
    or a callable class definition.

    If the expression is returning a callable object (like lambda function
    or Try() object) it will be called and a result of the call will be returned.

    If a result of calling of the defined function or class are returning a callable object
    it will not be called.

    As access to the hidden attributes is protected by check_ast_node_is_safe() method
    we should not have any problem with vulnerabilites defined here:
    Link: http://nedbatchelder.com/blog/201206/eval_really_is_dangerous.html

    TODO: implement compile object cache

    >>> safe_eval('value > 0', value=1)
    True

    >>> safe_eval('def m(): return value', value=10)
    10

    >>> safe_eval('def m(param): return value', value=10)
    Traceback (most recent call last):
        ...
    TypeError: m() takes exactly 1 argument (0 given)

    >>> safe_eval('lambda: value', value=10)
    10

    >>> result = safe_eval('def m(): print value', value=10)
    Traceback (most recent call last):
        ...
    SyntaxError: invalid syntax

    >>> result = safe_eval('print value', value=10)
    Traceback (most recent call last):
        ...
    SyntaxError: invalid syntax

    >>> safe_eval('def m(): return lambda: value', value=10) #doctest: +ELLIPSIS
    <function <lambda> at ...>

    >>> safe_eval('error = value', value=10, eval_source='<alert-condition>')
    Traceback (most recent call last):
        ...
    InvalidEvalExpression: <alert-condition> can contain a python expression, a function call or a callable class definition

    >>> safe_eval('def m(): return value.__class__', value=10)
    Traceback (most recent call last):
        ...
    InvalidEvalExpression: <string> should not try to access hidden attributes (for example '__class__')

    >>> safe_eval("""
    ... class CallableClass(object):
    ...
    ...     def get_value(self):
    ...         return value
    ...
    ...     def __call__(self):
    ...         return self.get_value()
    ... """, value=10)
    10

    >>> safe_eval("""
    ... class NotCallableClass(object):
    ...
    ...     def get_value(self):
    ...         return value
    ...
    ...     def call(self): # this is not a callable class
    ...         return self.get_value()
    ... """, value=10)
    Traceback (most recent call last):
        ...
    InvalidEvalExpression: <string> should contain a callable class definition (missing __call__ method?)


    >>> safe_eval("""
    ... def firstfunc():
    ...     return value
    ...
    ... value > 0
    ...
    ... """, value=10)
    Traceback (most recent call last):
        ...
    InvalidEvalExpression: <string> should contain only one python expression, a function call or a callable class definition

    '''  # noqa

    g = {'__builtins__': {}, 'object': object, '__name__': __name__}
    # __builtins__ should be masked away to disable builtin functions
    # object is needed if the NewStyle class is being created
    # __name__ is needed to be able to compile a class
    g.update(kwargs)

    node = compile(expr, eval_source, 'exec', ast.PyCF_ONLY_AST | __future__.CO_FUTURE_PRINT_FUNCTION)
    node = check_ast_node_is_safe(node, eval_source)
    body = node.body
    if body and len(body) == 1:
        x = body[0]
        if isinstance(x, ast.FunctionDef) or isinstance(x, ast.ClassDef):
            cc = compile(node, eval_source, 'exec')  # can be nicely cached
            v = {}
            exec (cc, g, v)
            if len(v) == 1:
                c = v.itervalues().next()
                if isclass(c):
                    # we need a class instance and not the class itself
                    c = c()

                if callable(c):
                    return c()  # if a function will return another callable, we will not call it
                else:
                    raise InvalidEvalExpression(
                        '{} should contain a callable class definition (missing __call__ method?)'.format(eval_source))
            else:
                raise InvalidEvalExpression(
                    '{} should contain only one function or one callable class definition'.format(eval_source))
        elif isinstance(x, ast.Expr):
            cc = compile(expr, eval_source, 'eval', __future__.CO_FUTURE_PRINT_FUNCTION)  # can be nicely cached
            r = eval(cc, g)
            if callable(r):
                # Try() returns callable that should be executed
                return r()
            else:
                return r
        else:
            raise InvalidEvalExpression(
                '{} can contain a python expression, a function call or a callable class definition'.format(
                    eval_source))
    else:
        raise InvalidEvalExpression(
            '{} should contain only one python expression, a function call or a callable class definition'.format(
                eval_source))

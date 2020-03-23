# -*- coding: utf-8 -*-
#           _      _    _ _
#     _ __ (_)_ _ (_)__| | |__
#    | '  \| | ' \| / _` | '_ \
#    |_|_|_|_|_||_|_\__,_|_.__/
#    simple python object store
#
# Copyright 2009-2010, 2014-2020 Thomas Perl <thp.io>. All rights reserved.
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#

"""A simple SQLite3-based store for Python objects"""

__author__ = 'Thomas Perl <m@thp.io>'
__version__ = '2.0.3'
__url__ = 'http://thp.io/2010/minidb/'
__license__ = 'ISC'


__all__ = [
    # Main classes
    'Store', 'Model', 'JSON',

    # Exceptions
    'UnknownClass',

    # Utility functions
    'columns', 'func', 'literal',

    # Decorator for registering converters
    'converter_for',

    # Debugging utilities
    'pprint', 'pformat',
]


DEBUG_OBJECT_CACHE = False
CONVERTERS = {}


import sqlite3
import threading
import inspect
import functools
import types
import collections
import weakref
import sys
import json
import datetime
import logging


logger = logging.getLogger(__name__)


class UnknownClass(TypeError):
    ...


def converter_for(type_):
    def decorator(f):
        CONVERTERS[type_] = f
        return f

    return decorator


def _get_all_slots(class_, include_private=False):
    for clazz in reversed(inspect.getmro(class_)):
        if hasattr(clazz, '__minidb_slots__'):
            for name, type_ in clazz.__minidb_slots__.items():
                if include_private or not name.startswith('_'):
                    yield (name, type_)


def _set_attribute(o, slot, cls, value):
    if value is None and hasattr(o.__class__, '__minidb_defaults__'):
        value = getattr(o.__class__.__minidb_defaults__, slot, None)
        if isinstance(value, types.FunctionType):
            # Late-binding of default lambda (taking o as argument)
            value = value(o)
    if value is not None and cls not in CONVERTERS:
        value = cls(value)
    setattr(o, slot, value)


class RowProxy(object):
    def __init__(self, row, keys):
        self._row = row
        self._keys = keys

    def __getitem__(self, key):
        if isinstance(key, str):
            try:
                index = self._keys.index(key)
            except ValueError as ve:
                raise KeyError(key)

            return self._row[index]

        return self._row[key]

    def __getattr__(self, attr):
        if attr not in self._keys:
            raise AttributeError(attr)

        return self[attr]

    def __repr__(self):
        return repr(self._row)

    def keys(self):
        return self._keys


class Store(object):
    PRIMARY_KEY = ('id', int)
    MINIDB_ATTR = '_minidb'

    def __init__(self, filename=':memory:', debug=False, smartupdate=False):
        self.db = sqlite3.connect(filename, check_same_thread=False)
        self.debug = debug
        self.smartupdate = smartupdate
        self.registered = {}
        self.lock = threading.RLock()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is exc_value is traceback is None:
            self.commit()

        self.close()

    def _execute(self, sql, args=None):
        if args is None:
            if self.debug:
                logger.debug('%s', sql)
            return self.db.execute(sql)
        else:
            if self.debug:
                logger.debug('%s %r', sql, args)
            return self.db.execute(sql, args)

    def _schema(self, class_):
        if class_ not in self.registered.values():
            raise UnknownClass('{} was never registered'.format(class_))
        return (class_.__name__, list(_get_all_slots(class_)))

    def commit(self):
        with self.lock:
            self.db.commit()

    def close(self):
        with self.lock:
            self.db.isolation_level = None
            self._execute('VACUUM')
            self.db.close()

    def _ensure_schema(self, table, slots):
        with self.lock:
            cur = self._execute('PRAGMA table_info(%s)' % table)
            available = cur.fetchall()

            def column(name, type_, primary=True):
                if (name, type_) == self.PRIMARY_KEY and primary:
                    return 'INTEGER PRIMARY KEY'
                elif type_ in (int, bool):
                    return 'INTEGER'
                elif type_ in (float,):
                    return 'REAL'
                elif type_ in (bytes,):
                    return 'BLOB'
                else:
                    return 'TEXT'

            if available:
                available = [(row[1], row[2]) for row in available]

                modify_slots = [(name, type_) for name, type_ in slots if name in (name for name, _ in available) and
                                (name, column(name, type_, False)) not in available]
                for name, type_ in modify_slots:
                    raise TypeError('Column {} is {}, but expected {}'.format(name, next(dbtype for n, dbtype in
                                                                                         available if n == name),
                                                                              column(name, type_)))

                # TODO: What to do with extraneous columns?

                missing_slots = [(name, type_) for name, type_ in slots if name not in (n for n, _ in available)]
                for name, type_ in missing_slots:
                    self._execute('ALTER TABLE %s ADD COLUMN %s %s' % (table, name, column(name, type_)))
            else:
                self._execute('CREATE TABLE %s (%s)' % (table, ', '.join('{} {}'.format(name, column(name, type_))
                                                                         for name, type_ in slots)))

    def register(self, class_, upgrade=False):
        if not issubclass(class_, Model):
            raise TypeError('{} is not a subclass of minidb.Model'.format(class_.__name__))

        if class_ in self.registered.values():
            raise TypeError('{} is already registered'.format(class_.__name__))
        elif class_.__name__ in self.registered and not upgrade:
            raise TypeError('{} is already registered {}'.format(class_.__name__, self.registered[class_.__name__]))

        with self.lock:
            self.registered[class_.__name__] = class_
            table, slots = self._schema(class_)
            self._ensure_schema(table, slots)

        return class_

    def serialize(self, v, t):
        if v is None:
            return None
        elif t in CONVERTERS:
            return CONVERTERS[t](v, True)
        elif isinstance(v, bool):
            return int(v)
        elif isinstance(v, (int, float, bytes)):
            return v

        return str(v)

    def deserialize(self, v, t):
        if v is None:
            return None
        elif t in CONVERTERS:
            return CONVERTERS[t](v, False)
        elif isinstance(v, t):
            return v

        return t(v)

    def save_or_update(self, o):
        if o.id is None:
            o.id = self.save(o)
        else:
            self._update(o)

    def delete_by_pk(self, o):
        with self.lock:
            table, slots = self._schema(o.__class__)

            assert self.PRIMARY_KEY in slots
            pk_name, pk_type = self.PRIMARY_KEY
            pk = getattr(o, pk_name)
            assert pk is not None

            self._execute('DELETE FROM %s WHERE %s = ?' % (table, pk_name), [pk])
            setattr(o, pk_name, None)

    def _update(self, o):
        with self.lock:
            table, slots = self._schema(o.__class__)

            # Update requires a primary key
            assert self.PRIMARY_KEY in slots
            pk_name, pk_type = self.PRIMARY_KEY

            if self.smartupdate:
                existing = dict(next(self.query(o.__class__, where=lambda c:
                                                getattr(c, pk_name) == getattr(o, pk_name))))
            else:
                existing = {}

            values = [(name, type_, getattr(o, name, None))
                      for name, type_ in slots if (name, type_) != self.PRIMARY_KEY and
                      (name not in existing or getattr(o, name, None) != existing[name])]

            if self.smartupdate and self.debug:
                for name, type_, to_value in values:
                    logger.debug('%s %s', '{}(id={})'.format(table, o.id),
                                 '{}: {} -> {}'.format(name, existing[name], to_value))

            if not values:
                # No values have changed - nothing to update
                return

            def gen_keys():
                for name, type_, value in values:
                    if value is not None:
                        yield '{name}=?'.format(name=name)
                    else:
                        yield '{name}=NULL'.format(name=name)

            def gen_values():
                for name, type_, value in values:
                    if value is not None:
                        yield self.serialize(value, type_)

                yield getattr(o, pk_name)

            self._execute('UPDATE %s SET %s WHERE %s = ?' % (table, ', '.join(gen_keys()), pk_name), list(gen_values()))

    def save(self, o):
        with self.lock:
            table, slots = self._schema(o.__class__)

            # Save all values except for the primary key
            slots = [(name, type_) for name, type_ in slots if (name, type_) != self.PRIMARY_KEY]

            values = [self.serialize(getattr(o, name), type_) for name, type_ in slots]
            return self._execute('INSERT INTO %s (%s) VALUES (%s)' % (table, ', '.join(name for name, type_ in slots),
                                                                      ', '.join('?' * len(slots))), values).lastrowid

    def delete_where(self, class_, where):
        with self.lock:
            table, slots = self._schema(class_)

            if isinstance(where, types.FunctionType):
                # Late-binding of where
                where = where(class_.c)

            ssql, args = where.tosql()
            sql = 'DELETE FROM %s WHERE %s' % (table, ssql)
            return self._execute(sql, args).rowcount

    def query(self, class_, select=None, where=None, order_by=None, group_by=None, limit=None):
        with self.lock:
            table, slots = self._schema(class_)
            attr_to_type = dict(slots)

            sql = []
            args = []

            if select is None:
                select = literal('*')

            if isinstance(select, types.FunctionType):
                # Late-binding of columns
                select = select(class_.c)

            # Select can always be a sequence
            if not isinstance(select, Sequence):
                select = Sequence([select])

            # Look for RenameOperation operations in the SELECT sequence and
            # remember the column types, so we can decode values properly later
            for arg in select.args:
                if isinstance(arg, Operation):
                    if isinstance(arg.a, RenameOperation):
                        if isinstance(arg.a.column, Column):
                            attr_to_type[arg.a.name] = arg.a.column.type_

            ssql, sargs = select.tosql()
            sql.append('SELECT %s FROM %s' % (ssql, table))
            args.extend(sargs)

            if where is not None:
                if isinstance(where, types.FunctionType):
                    # Late-binding of columns
                    where = where(class_.c)
                wsql, wargs = where.tosql()
                sql.append('WHERE %s' % (wsql,))
                args.extend(wargs)

            if order_by is not None:
                if isinstance(order_by, types.FunctionType):
                    # Late-binding of columns
                    order_by = order_by(class_.c)

                osql, oargs = order_by.tosql()
                sql.append('ORDER BY %s' % (osql,))
                args.extend(oargs)

            if group_by is not None:
                if isinstance(group_by, types.FunctionType):
                    # Late-binding of columns
                    group_by = group_by(class_.c)

                gsql, gargs = group_by.tosql()
                sql.append('GROUP BY %s' % (gsql,))
                args.extend(gargs)

            if limit is not None:
                sql.append('LIMIT ?')
                args.append(limit)

            sql = ' '.join(sql)

            result = self._execute(sql, args)
            columns = [d[0] for d in result.description]

            def _decode(row, columns):
                for name, value in zip(columns, row):
                    type_ = attr_to_type.get(name, None)
                    yield (self.deserialize(value, type_) if type_ is not None else value)

            return (RowProxy(tuple(_decode(row, columns)), columns) for row in result)

    def load(self, class_, *args, **kwargs):
        with self.lock:
            query = kwargs.get('__query__', None)
            if '__query__' in kwargs:
                del kwargs['__query__']

            table, slots = self._schema(class_)
            sql = 'SELECT %s FROM %s' % (', '.join(name for name, type_ in slots), table)
            if query:
                if isinstance(query, types.FunctionType):
                    # Late-binding of query
                    query = query(class_.c)

                ssql, aargs = query.tosql()
                sql += ' WHERE %s' % ssql
                sql_args = aargs
            elif kwargs:
                sql += ' WHERE %s' % (' AND '.join('%s = ?' % k for k in kwargs))
                sql_args = list(kwargs.values())
            else:
                sql_args = []
            cur = self._execute(sql, sql_args)

            def apply(row):
                row = zip(slots, row)
                kwargs = {name: self.deserialize(v, type_) for (name, type_), v in row if v is not None}
                o = class_(*args, **kwargs)
                setattr(o, self.MINIDB_ATTR, self)
                return o

            return (x for x in (apply(row) for row in cur) if x is not None)

    def get(self, class_, *args, **kwargs):
        it = self.load(class_, *args, **kwargs)
        result = next(it, None)

        try:
            next(it)
        except StopIteration:
            return result

        raise ValueError('More than one row returned')


class Operation(object):
    def __init__(self, a, op=None, b=None, brackets=False):
        self.a = a
        self.op = op
        self.b = b
        self.brackets = brackets

    def _get_class(self, a):
        if isinstance(a, Column):
            return a.class_
        elif isinstance(a, RenameOperation):
            return self._get_class(a.column)
        elif isinstance(a, Function):
            return a.args[0].class_
        elif isinstance(a, Sequence):
            return a.args[0].class_

        raise ValueError('Cannot determine class for query')

    def query(self, db, where=None, order_by=None, group_by=None, limit=None):
        return self._get_class(self.a).query(db, self, where=where, order_by=order_by, group_by=group_by, limit=limit)

    def __floordiv__(self, other):
        if self.b is not None:
            raise ValueError('Cannot sequence columns')
        return Sequence([self, other])

    def argtosql(self, arg):
        if isinstance(arg, Operation):
            return arg.tosql(self.brackets)
        elif isinstance(arg, Column):
            return (arg.name, [])
        elif isinstance(arg, RenameOperation):
            columnname, args = arg.column.tosql()
            return ('%s AS %s' % (columnname, arg.name), args)
        elif isinstance(arg, Function):
            sqls = []
            argss = []
            for farg in arg.args:
                sql, args = self.argtosql(farg)
                sqls.append(sql)
                argss.extend(args)
            return ['%s(%s)' % (arg.name, ', '.join(sqls)), argss]
        elif isinstance(arg, Sequence):
            sqls = []
            argss = []
            for farg in arg.args:
                sql, args = self.argtosql(farg)
                sqls.append(sql)
                argss.extend(args)
            return ['%s' % ', '.join(sqls), argss]
        elif isinstance(arg, Literal):
            return [arg.name, []]
        if type(arg) in CONVERTERS:
            return ('?', [CONVERTERS[type(arg)](arg, True)])

        return ('?', [arg])

    def tosql(self, brackets=False):
        sql = []
        args = []

        ssql, aargs = self.argtosql(self.a)
        sql.append(ssql)
        args.extend(aargs)

        if self.op is not None:
            sql.append(self.op)

        if self.b is not None:
            ssql, aargs = self.argtosql(self.b)
            sql.append(ssql)
            args.extend(aargs)

        if brackets:
            sql.insert(0, '(')
            sql.append(')')

        return (' '.join(sql), args)

    def __and__(self, other):
        return Operation(self, 'AND', other, True)

    def __or__(self, other):
        return Operation(self, 'OR', other, True)

    def __repr__(self):
        if self.b is None:
            if self.op is None:
                return '{self.a!r}'.format(self=self)
            return '{self.a!r} {self.op}'.format(self=self)
        return '{self.a!r} {self.op} {self.b!r}'.format(self=self)


class Sequence(object):
    def __init__(self, args):
        self.args = args

    def __repr__(self):
        return ', '.join(repr(arg) for arg in self.args)

    def tosql(self):
        return Operation(self).tosql()

    def query(self, db, order_by=None, group_by=None, limit=None):
        return Operation(self).query(db, order_by=order_by, group_by=group_by, limit=limit)

    def __floordiv__(self, other):
        self.args.append(other)
        return self


def columns(*args):
    """columns(a, b, c) -> a // b // c

    Query multiple columns, like the // column sequence operator.
    """
    return Sequence(args)


class func(object):
    max = staticmethod(lambda *args: Function('max', *args))
    min = staticmethod(lambda *args: Function('min', *args))
    sum = staticmethod(lambda *args: Function('sum', *args))
    distinct = staticmethod(lambda *args: Function('distinct', *args))
    random = staticmethod(lambda: Function('random'))

    abs = staticmethod(lambda a: Function('abs', a))
    length = staticmethod(lambda a: Function('length', a))
    lower = staticmethod(lambda a: Function('lower', a))
    upper = staticmethod(lambda a: Function('upper', a))
    ltrim = staticmethod(lambda a: Function('ltrim', a))
    rtrim = staticmethod(lambda a: Function('rtrim', a))
    trim = staticmethod(lambda a: Function('trim', a))

    count = staticmethod(lambda a: Function('count', a))
    __call__ = lambda a, name: RenameOperation(a, name)


class OperatorMixin(object):
    __lt__ = lambda a, b: Operation(a, '<', b)
    __le__ = lambda a, b: Operation(a, '<=', b)
    __eq__ = lambda a, b: Operation(a, '=', b) if b is not None else Operation(a, 'IS NULL')
    __ne__ = lambda a, b: Operation(a, '!=', b) if b is not None else Operation(a, 'IS NOT NULL')
    __gt__ = lambda a, b: Operation(a, '>', b)
    __ge__ = lambda a, b: Operation(a, '>=', b)

    __call__ = lambda a, name: RenameOperation(a, name)
    tosql = lambda a: Operation(a).tosql()
    query = lambda a, db, where=None, order_by=None, group_by=None, limit=None: Operation(a).query(db, where=where,
                                                                                                   order_by=order_by,
                                                                                                   group_by=group_by,
                                                                                                   limit=limit)
    __floordiv__ = lambda a, b: Sequence([a, b])

    like = lambda a, b: Operation(a, 'LIKE', b)

    avg = property(lambda a: Function('avg', a))
    max = property(lambda a: Function('max', a))
    min = property(lambda a: Function('min', a))
    sum = property(lambda a: Function('sum', a))
    distinct = property(lambda a: Function('distinct', a))

    asc = property(lambda a: Operation(a, 'ASC'))
    desc = property(lambda a: Operation(a, 'DESC'))

    abs = property(lambda a: Function('abs', a))
    length = property(lambda a: Function('length', a))
    lower = property(lambda a: Function('lower', a))
    upper = property(lambda a: Function('upper', a))
    ltrim = property(lambda a: Function('ltrim', a))
    rtrim = property(lambda a: Function('rtrim', a))
    trim = property(lambda a: Function('trim', a))
    count = property(lambda a: Function('count', a))


class RenameOperation(OperatorMixin):
    def __init__(self, column, name):
        self.column = column
        self.name = name

    def __repr__(self):
        return '%r AS %s' % (self.column, self.name)


class Literal(OperatorMixin):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name


def literal(name):
    """Insert a literal as-is into a SQL query

    >>> func.count(literal('*'))
    count(*)
    """
    return Literal(name)


class Function(OperatorMixin):
    def __init__(self, name, *args):
        self.name = name
        self.args = args

    def __repr__(self):
        return '%s(%s)' % (self.name, ', '.join(repr(arg) for arg in self.args))


class Column(OperatorMixin):
    def __init__(self, class_, name, type_):
        self.class_ = class_
        self.name = name
        self.type_ = type_

    def __repr__(self):
        return '.'.join((self.class_.__name__, self.name))


class Columns(object):
    def __init__(self, name, slots):
        self._class = None
        self._name = name
        self._slots = slots

    def __repr__(self):
        return '<{} for {} ({})>'.format(self.__class__.__name__, self._name, ', '.join(self._slots))

    def __getattr__(self, name):
        d = {k: v for k, v in _get_all_slots(self._class, include_private=True)}
        if name not in d:
            raise AttributeError(name)

        return Column(self._class, name, d[name])


def model_init(self, *args, **kwargs):
    slots = list(_get_all_slots(self.__class__, include_private=True))
    unmatched_kwargs = set(kwargs.keys()).difference(set(key for key, type_ in slots))
    if unmatched_kwargs:
        raise KeyError('Invalid keyword argument(s): %r' % unmatched_kwargs)

    for key, type_ in slots:
        _set_attribute(self, key, type_, kwargs.get(key, None))

    # Call redirected constructor
    if '__minidb_init__' in self.__class__.__dict__:
        getattr(self, '__minidb_init__')(*args)


class MetaModel(type):
    @classmethod
    def __prepare__(metacls, name, bases):
        return collections.OrderedDict()

    def __new__(mcs, name, bases, d):
        # Redirect __init__() to __minidb_init__()
        if '__init__' in d:
            d['__minidb_init__'] = d['__init__']
        d['__init__'] = model_init

        # Caching of live objects
        d['__minidb_cache__'] = weakref.WeakValueDictionary()

        slots = collections.OrderedDict((k, v) for k, v in d.items()
                                        if k.lower() == k and
                                        not k.startswith('__') and
                                        not isinstance(v, types.FunctionType) and
                                        not isinstance(v, property) and
                                        not isinstance(v, staticmethod) and
                                        not isinstance(v, classmethod))

        keep = collections.OrderedDict((k, v) for k, v in d.items() if k not in slots)
        keep['__minidb_slots__'] = slots

        keep['__slots__'] = tuple(slots.keys())
        if not bases:
            # Add weakref slot to Model (for caching)
            keep['__slots__'] += ('__weakref__',)

        columns = Columns(name, slots)
        keep['c'] = columns

        result = type.__new__(mcs, name, bases, keep)
        columns._class = result
        return result


def pformat(result, color=False):
    def incolor(color_id, s):
        return '\033[9%dm%s\033[0m' % (color_id, s) if sys.stdout.isatty() and color else s

    inred, ingreen, inyellow, inblue = (functools.partial(incolor, x) for x in range(1, 5))

    rows = list(result)
    if not rows:
        return '(no rows)'

    def colorvalue(formatted, value):
        if value is None:
            return inred(formatted)
        if isinstance(value, bool):
            return ingreen(formatted)

        return formatted

    s = []
    keys = rows[0].keys()
    lengths = tuple(max(x) for x in zip(*[[len(str(column)) for column in row] for row in [keys] + rows]))
    s.append(' | '.join(inyellow('%-{}s'.format(length) % key) for key, length in zip(keys, lengths)))
    s.append('-+-'.join('-' * length for length in lengths))
    for row in rows:
        s.append(' | '.join(colorvalue('%-{}s'.format(length) % col, col) for col, length in zip(row, lengths)))
    s.append('({} row(s))'.format(len(rows)))
    return ('\n'.join(s))


def pprint(result, color=False):
    print(pformat(result, color))


class JSON(object):
    ...


@converter_for(JSON)
def convert_json(v, serialize):
    return json.dumps(v) if serialize else json.loads(v)


@converter_for(datetime.datetime)
def convert_datetime_datetime(v, serialize):
    """
    >>> convert_datetime_datetime(datetime.datetime(2014, 12, 13, 14, 15), True)
    '2014-12-13T14:15:00'
    >>> convert_datetime_datetime('2014-12-13T14:15:16', False)
    datetime.datetime(2014, 12, 13, 14, 15, 16)
    """
    if serialize:
        return v.isoformat()
    else:
        isoformat, microseconds = (v.rsplit('.', 1) if '.' in v else (v, 0))
        return (datetime.datetime.strptime(isoformat, '%Y-%m-%dT%H:%M:%S') +
                datetime.timedelta(microseconds=int(microseconds)))


@converter_for(datetime.date)
def convert_datetime_date(v, serialize):
    """
    >>> convert_datetime_date(datetime.date(2014, 12, 13), True)
    '2014-12-13'
    >>> convert_datetime_date('2014-12-13', False)
    datetime.date(2014, 12, 13)
    """
    if serialize:
        return v.isoformat()
    else:
        return datetime.datetime.strptime(v, '%Y-%m-%d').date()


@converter_for(datetime.time)
def convert_datetime_time(v, serialize):
    """
    >>> convert_datetime_time(datetime.time(14, 15, 16), True)
    '14:15:16'
    >>> convert_datetime_time('14:15:16', False)
    datetime.time(14, 15, 16)
    """
    if serialize:
        return v.isoformat()
    else:
        isoformat, microseconds = (v.rsplit('.', 1) if '.' in v else (v, 0))
        return (datetime.datetime.strptime(isoformat, '%H:%M:%S') +
                datetime.timedelta(microseconds=int(microseconds))).time()


class Model(metaclass=MetaModel):
    id = int
    _minidb = Store

    @classmethod
    def _finalize(cls, id):
        if DEBUG_OBJECT_CACHE:
            logger.debug('Finalizing {} id={}'.format(cls.__name__, id))

    def __repr__(self):
        def get_attrs():
            for key, type_ in _get_all_slots(self.__class__):
                yield key, getattr(self, key, None)

        attrs = ['{key}={value!r}'.format(key=key, value=value) for key, value in get_attrs()]
        return '<%(cls)s(%(attrs)s)>' % {
            'cls': self.__class__.__name__,
            'attrs': ', '.join(attrs),
        }

    @classmethod
    def __lookup_single(cls, o):
        if o is None:
            return None

        cache = cls.__minidb_cache__
        if o.id not in cache:
            if DEBUG_OBJECT_CACHE:
                logger.debug('Storing id={} in cache {}'.format(o.id, o))
                weakref.finalize(o, cls._finalize, o.id)
            cache[o.id] = o
        else:
            if DEBUG_OBJECT_CACHE:
                logger.debug('Getting id={} from cache'.format(o.id))
        return cache[o.id]

    @classmethod
    def __lookup_cache(cls, objects):
        for o in objects:
            yield cls.__lookup_single(o)

    @classmethod
    def load(cls, db, query=None, **kwargs):
        if query is not None:
            kwargs['__query__'] = query
        if '__minidb_init__' in cls.__dict__:
            @functools.wraps(cls.__minidb_init__)
            def init_wrapper(*args):
                return cls.__lookup_cache(db.load(cls, *args, **kwargs))
            return init_wrapper
        else:
            return cls.__lookup_cache(db.load(cls, **kwargs))

    @classmethod
    def get(cls, db, query=None, **kwargs):
        if query is not None:
            kwargs['__query__'] = query
        if '__minidb_init__' in cls.__dict__:
            @functools.wraps(cls.__minidb_init__)
            def init_wrapper(*args):
                return cls.__lookup_single(db.get(cls, *args, **kwargs))
            return init_wrapper
        else:
            return cls.__lookup_single(db.get(cls, **kwargs))

    def save(self, db=None):
        if getattr(self, Store.MINIDB_ATTR, None) is None:
            if db is None:
                raise ValueError('Needs a db object')
            setattr(self, Store.MINIDB_ATTR, db)

        getattr(self, Store.MINIDB_ATTR).save_or_update(self)

        if DEBUG_OBJECT_CACHE:
            logger.debug('Storing id={} in cache {}'.format(self.id, self))
            weakref.finalize(self, self.__class__._finalize, self.id)
        self.__class__.__minidb_cache__[self.id] = self

        return self

    def delete(self):
        if getattr(self, Store.MINIDB_ATTR) is None:
            raise ValueError('Needs a db object')
        elif self.id is None:
            raise KeyError('id is None (not stored in db?)')

        # drop from cache
        cache = self.__class__.__minidb_cache__
        if self.id in cache:
            if DEBUG_OBJECT_CACHE:
                logger.debug('Dropping id={} from cache {}'.format(self.id, self))
            del cache[self.id]

        getattr(self, Store.MINIDB_ATTR).delete_by_pk(self)

    @classmethod
    def delete_where(cls, db, query):
        return db.delete_where(cls, query)

    @classmethod
    def query(cls, db, select=None, where=None, order_by=None, group_by=None, limit=None):
        return db.query(cls, select=select, where=where, order_by=order_by, group_by=group_by, limit=limit)

    @classmethod
    def pquery(cls, db, select=None, where=None, order_by=None, group_by=None, limit=None, color=True):
        pprint(db.query(cls, select=select, where=where, order_by=order_by, group_by=group_by, limit=limit), color)

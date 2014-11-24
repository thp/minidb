# -*- coding: utf-8 -*-
#
# minidb - A simple SQLite3 store for Python objects
# (based on "ORM wie eine Kirchenmaus" by thp, 2009-11-29)
#
# Copyright 2009-2010 Thomas Perl <thp.io>. All rights reserved.
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

"""A simple SQLite3 store for Python objects"""

__author__ = 'Thomas Perl <m@thp.io>'
__version__ = '1.1'
__website__ = 'http://thp.io/2010/minidb/'
__license__ = 'ISC'

import sqlite3
import threading
import inspect
import functools
import types
import collections


def _get_all_slots(class_, include_private=False):
    for clazz in reversed(inspect.getmro(class_)):
        if hasattr(clazz, '__slots__'):
            for name, type_ in clazz.__slots__.items():
                if include_private or not name.startswith('_'):
                    yield (name, type_)

def _set_attribute(o, slot, cls, value):
    # Set a slot on the given object to value, doing a cast if
    # necessary. The value None is special-cased and never cast.
    if value is not None:
        value = cls(value)
    setattr(o, slot, value)


class Store(object):
    PRIMARY_KEY = ('id', int)
    MINIDB_ATTR = '_minidb'

    def __init__(self, filename=':memory:', autoregister=False, debug=False):
        """Create (or load) a new minidb storage

        Without arguments, this will create an in-memory
        database that will be deleted when closed. If you
        pass an argument, it should be the filename of the
        database file (which will be created if it does
        not yet exist).
        """
        self.db = sqlite3.connect(filename, check_same_thread=False)
        self.debug = debug
        self.autoregister = autoregister
        self.registered = []
        self.lock = threading.RLock()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if (exc_type, exc_value, traceback) is (None, None, None):
            self.commit()

        self.close()

    def _execute(self, sql, args=None):
        if args is None:
            if self.debug:
                print('    :', sql)
            return self.db.execute(sql)
        else:
            if self.debug:
                print('    :', sql, args)
            return self.db.execute(sql, args)

    def _schema(self, class_):
        if class_ not in self.registered:
            raise TypeError('{} was never registered'.format(class_))
        return (class_.__name__, list(sorted(_get_all_slots(class_))))

    def commit(self):
        """Commit changes into the database"""
        with self.lock:
            self.db.commit()

    def close(self):
        """Close the underlying database file"""
        with self.lock:
            self._execute('VACUUM')
            self.db.close()

    def _ensure_schema(self, table, slots, is_model):
        with self.lock:
            cur = self._execute('PRAGMA table_info(%s)' % table)
            available = cur.fetchall()

            def column(name, type_):
                if is_model and (name, type_) == self.PRIMARY_KEY:
                    return 'id INTEGER PRIMARY KEY'
                elif type_ in (int,):
                    return '{name} INTEGER'.format(name=name)
                elif type_ in (float,):
                    return '{name} REAL'.format(name=name)
                else:
                    return '{name} TEXT'.format(name=name)

            if available:
                available = [row[1] for row in available]
                missing_slots = ((name, type_) for name, type_ in slots if name not in available)
                for name, type_ in missing_slots:
                    self._execute('ALTER TABLE %s ADD COLUMN %s' % (table, column(name, type_)))
            else:
                self._execute('CREATE TABLE %s (%s)' % (table, ', '.join(column(name, type_)
                    for name, type_ in slots)))

    def register(self, class_):
        if class_ in self.registered:
            return class_

        with self.lock:
            self.registered.append(class_)
            table, slots = self._schema(class_)
            self._ensure_schema(table, slots, issubclass(class_, Model))

        return class_

    def convert(self, v):
        """Convert a value to its string representation"""
        if v is None:
            return None
        elif isinstance(v, str):
            return v
        elif isinstance(v, bytes):
            return v.decode('utf-8')
        else:
            return str(v)

    def update(self, o, **kwargs):
        """Update fields of an object and store the changes

        This will update named fields (specified by keyword
        arguments) inside the object and also store these
        changes in the database.
        """
        self.remove(o)
        for k, v in kwargs.items():
            setattr(o, k, v)
        self.save(o)

    def save_or_update(self, o):
        if o.id is None:
            o.id = self.save(o)
        else:
            self._update(o)

    def delete_by_pk(self, o):
        with self.lock:
            if self.autoregister:
                self.register(o.__class__)
            table, slots = self._schema(o.__class__)

            assert self.PRIMARY_KEY in slots
            pk_name, pk_type = self.PRIMARY_KEY
            pk = getattr(o, pk_name)
            assert pk is not None

            res = self._execute('DELETE FROM %s WHERE %s=?' % (table, pk_name), [pk])
            setattr(o, pk_name, None)

    def _update(self, o):
        with self.lock:
            if self.autoregister:
                self.register(o.__class__)
            table, slots = self._schema(o.__class__)

            # Update requires a primary key
            assert self.PRIMARY_KEY in slots
            pk_name, pk_type = self.PRIMARY_KEY

            values = [(name, type_, getattr(o, name, None))
                      for name, type_ in slots
                      if (name, type_) != self.PRIMARY_KEY]

            def gen_keys():
                for name, type_, value in values:
                    if value is not None:
                        yield '{name}=?'.format(name=name)
                    else:
                        yield '{name}=NULL'.format(name=name)

            def gen_values():
                for name, type_, value in values:
                    if value is not None:
                        yield self.convert(value)

                yield getattr(o, pk_name)

            res = self._execute('UPDATE %s SET %s WHERE %s=?' % (table,
                                  ', '.join(gen_keys()), pk_name),
                                  list(gen_values()))

    def save(self, o):
        """Save an object into the database

        Save a newly-created object into the database. The
        object will always be newly created, never updated.

        If you want to update an object inside the database,
        please use the "update" method instead.
        """
        if hasattr(o, '__iter__'):
            for child in o:
                self.save(child)
            return

        with self.lock:
            if self.autoregister:
                self.register(o.__class__)
            table, slots = self._schema(o.__class__)

            # If it's a Model subclass, we skip the primary key column
            skip_primary_key = isinstance(o, Model)

            # Save all values except for the primary key
            slots = [(name, type_) for name, type_ in slots
                     if not skip_primary_key or (name, type_) != self.PRIMARY_KEY]

            values = [self.convert(getattr(o, name)) for name, type_ in slots]
            return self._execute('INSERT INTO %s (%s) VALUES (%s)' % (table,
                                  ', '.join(name for name, type_ in slots),
                                  ', '.join('?'*len(slots))),
                                  values).lastrowid

    def delete(self, class_, **kwargs):
        """Delete objects from the database

        Delete objects of type "class_" with the criteria
        specified in "kwargs". Please note that all objects
        that match the criteria will be deleted.

        If you want to remove a specific object from the
        database, use "remove" instead.
        """
        with self.lock:
            if self.autoregister:
                self.register(class_)
            table, slots = self._schema(class_)
            sql = 'DELETE FROM %s' % (table,)
            if kwargs:
                sql += ' WHERE %s' % (' AND '.join('%s=?' % k for k in kwargs))
            return self._execute(sql, kwargs.values()).rowcount > 0

    def remove(self, o):
        """Delete objects by template object

        This will remove all objects from the database that
        compare to the given object (i.e. all attributes of
        "o" that are not None will match to the objects in
        the database).

        This method should be used to remove specific object
        only. For bulk deletion based on some criteria, the
        "delete" method might be better suited.
        """
        if hasattr(o, '__iter__'):
            for child in o:
                self.remove(child)
            return

        with self.lock:
            if self.autoregister:
                self.register(o.__class__)
            table, slots = self._schema(o.__class__)

            # Use "None" as wildcard selector in remove actions
            slots = [(name, type_) for name, type_ in slots
                     if getattr(o, name, None) is not None]

            values = [self.convert(getattr(o, name)) for name, type_ in slots]
            self._execute('DELETE FROM %s WHERE %s' % (table,
                ' AND '.join('%s=?'% name for (name, type_) in slots)), values)

    def delete_where(self, class_, where):
        with self.lock:
            if self.autoregister:
                self.register(class_)

            table, slots = self._schema(class_)

            ssql, args = where.tosql()
            sql = 'DELETE FROM %s WHERE %s' % (table, ssql)
            return self._execute(sql, args).rowcount

    def query(self, class_, select, where=None, order_by=None, limit=None, as_dict=False):
        with self.lock:
            if self.autoregister:
                self.register(class_)

            table, slots = self._schema(class_)

            sql = []
            args = []

            if isinstance(select, types.FunctionType):
                # Late-binding of columns
                select = select(class_.c)
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

            if limit is not None:
                sql.append('LIMIT ?')
                args.append(limit)

            sql = ' '.join(sql)

            result = self._execute(sql, args)
            if as_dict:
                columns = [d[0] for d in result.description]
                return (dict(zip(columns, row)) for row in result)

            return (row for row in result)

    def load(self, class_, *args, **kwargs):
        """Load objects of a given class

        Return a list of objects from the database that are of
        type "class_". By default, all objects are returned,
        but a simple pre-selection can be made using keyword
        arguments.
        """
        with self.lock:
            if self.autoregister:
                self.register(class_)

            query = kwargs.get('__query__', None)
            if '__query__' in kwargs:
                del kwargs['__query__']

            table, slots = self._schema(class_)
            sql = 'SELECT %s FROM %s' % (', '.join(name for name, type_ in slots), table)
            if query:
                ssql, aargs = query.tosql()
                sql += ' WHERE %s' % ssql
                sql_args = aargs
            elif kwargs:
                sql += ' WHERE %s' % (' AND '.join('%s=?' % k for k in kwargs))
                sql_args = list(kwargs.values())
            else:
                sql_args = []
            cur = self._execute(sql, sql_args)
            def apply(row):
                row = zip((name for name, type_ in slots), row)
                kwargs = {k: v for k, v in row if v is not None}
                if issubclass(class_, Model):
                    o = class_(*args, **kwargs)
                    setattr(o, self.MINIDB_ATTR, self)
                else:
                    o = class_.__new__(class_)
                    for (name, type_), value in zip(slots, row):
                        _set_attribute(o, name, type_, value)

                return o
            return (x for x in (apply(row) for row in cur) if x is not None)

    def get(self, class_, *args, **kwargs):
        """Load one object of a given class

        This is a convenience function that will load only a
        single object from the database, returning only that
        object or None when the object is not found.

        This method only makes sense when using keyword
        arguments to select the object (i.e. using a
        unique set of attributes to retrieve it).
        """
        return next(self.load(class_, *args, **kwargs), None)


class Operation(object):
    def __init__(self, a, op=None, b=None, brackets=False):
        self.a = a
        self.op = op
        self.b = b
        self.brackets = brackets

    def query(self, db, order_by=None, limit=None, as_dict=False):
        if isinstance(self.a, Column):
            class_ = self.a.class_
        elif isinstance(self.a, Function):
            class_ = self.a.args[0].class_
        elif isinstance(self.a, Sequence):
            class_ = self.a.args[0].class_
        else:
            raise ValueError('Cannot determine class for query')

        return class_.query(db, self, order_by=order_by, limit=limit, as_dict=as_dict)

    def __floordiv__(self, other):
        if self.b is not None:
            raise ValueError('Cannot sequence columns')
        return Sequence([self, other])

    def argtosql(self, arg):
        if isinstance(arg, Operation):
            return arg.tosql(self.brackets)
        elif isinstance(arg, Column):
            return (arg.name, [])
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
            return '{a!r} {op}'.format(**self.__dict__)
        return '{a!r} {op} {b!r}'.format(**self.__dict__)


def and_(*args):
    return reduce(lambda a, b: Operation(a, 'AND', b, True), args)


def or_(*args):
    return reduce(lambda a, b: Operation(a, 'OR', b, True), args)

class Sequence(object):
    def __init__(self, args):
        self.args = args

    def tosql(self):
        return Operation(self).tosql()

    def query(self, db, order_by=None, limit=None, as_dict=False):
        return Operation(self).query(db, order_by=order_by, limit=limit, as_dict=as_dict)

    def __floordiv__(self, other):
        self.args.append(other)
        return self

def columns(*args):
    return Sequence(args)

class func(object):
    max = staticmethod(lambda *args: Function('max', *args))
    min = staticmethod(lambda *args: Function('min', *args))
    random = staticmethod(lambda: Function('random'))

    abs = staticmethod(lambda a: Function('abs', a))
    length = staticmethod(lambda a: Function('length', a))
    lower = staticmethod(lambda a: Function('lower', a))
    upper = staticmethod(lambda a: Function('upper', a))
    ltrim = staticmethod(lambda a: Function('ltrim', a))
    rtrim = staticmethod(lambda a: Function('rtrim', a))
    trim = staticmethod(lambda a: Function('trim', a))

    count = staticmethod(lambda a: Function('count', a))

class OperatorMixin(object):
    __lt__ = lambda a, b: Operation(a, '<', b)
    __le__ = lambda a, b: Operation(a, '<=', b)
    __eq__ = lambda a, b: Operation(a, '=', b) if b is not None else Operation(a, 'IS NULL')
    __ne__ = lambda a, b: Operation(a, '!=', b) if b is not None else Operation(a, 'IS NOT NULL')
    __gt__ = lambda a, b: Operation(a, '>', b)
    __ge__ = lambda a, b: Operation(a, '>=', b)

    __call__ = lambda a, name: Operation(a, 'AS %s' % name)
    tosql = lambda a: Operation(a).tosql()
    query = lambda a, db, order_by=None, limit=None, as_dict=False: Operation(a).query(db,
            order_by=order_by, limit=limit, as_dict=as_dict)
    __floordiv__ = lambda a, b: Sequence([a, b])

    like = lambda a, b: Operation(a, 'LIKE', b)

    avg = property(lambda a: Function('avg', a))
    max = property(lambda a: Function('max', a))
    min = property(lambda a: Function('min', a))

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

class Literal(OperatorMixin):
    def __init__(self, name):
        self.name = name

def literal(name):
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

    def __getattr__(self, name):
        d = {k: v for k, v in _get_all_slots(self._class, include_private=True)}
        if name in d:
            return Column(self._class, name, d[name])

def model_init(self, *args, **kwargs):
    for key, type_ in _get_all_slots(self.__class__, include_private=True):
        _set_attribute(self, key, type_, kwargs.get(key, None))

    # Call redirected constructor
    if '__minidb_init__' in self.__class__.__dict__:
        getattr(self, '__minidb_init__')(*args)


class ClassAttributesAsSlotsMeta(type):
    """Metaclass that turns class attributes into __slots__, and renames __init__ to __minidb_init__"""

    @classmethod
    def __prepare__(metacls, name, bases):
        return collections.OrderedDict()

    def __new__(mcs, name, bases, d):
        if bases != (object,):
            # Redirect __init__() to __minidb_init__(), but not for
            # Model, which subclasses directly from object
            if '__init__' in d:
                d['__minidb_init__'] = d['__init__']
                del d['__init__']
                d['__init__'] = model_init

        slots = collections.OrderedDict((k, v) for k, v in d.items()
                 if k.lower() == k and
                 not k.startswith('__') and
                 not isinstance(v, types.FunctionType) and
                 not isinstance(v, property) and
                 not isinstance(v, staticmethod) and
                 not isinstance(v, classmethod))

        keep = collections.OrderedDict((k, v) for k, v in d.items() if k not in slots)
        keep['__slots__'] = slots

        columns = Columns(name, slots)
        keep['c'] = columns

        result = type.__new__(mcs, name, bases, keep)
        columns._class = result
        return result


class Model(metaclass=ClassAttributesAsSlotsMeta):
    id = int
    _minidb = Store

    def __init__(self):
        pass

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
    def load(cls, db, query=None, **kwargs):
        if query is not None:
            kwargs['__query__'] = query
        if '__minidb_init__' in cls.__dict__:
            @functools.wraps(cls.__minidb_init__)
            def init_wrapper(*args):
                return db.load(cls, *args, **kwargs)
            return init_wrapper
        else:
            return db.load(cls, **kwargs)

    @classmethod
    def get(cls, db, query=None, **kwargs):
        if query is not None:
            kwargs['__query__'] = query
        if '__minidb_init__' in cls.__dict__:
            @functools.wraps(cls.__minidb_init__)
            def init_wrapper(*args):
                return db.get(cls, *args, **kwargs)
            return init_wrapper
        else:
            return db.get(cls, **kwargs)

    def save(self, db=None):
        if getattr(self, Store.MINIDB_ATTR, None) is None:
            if db is None:
                raise ValueError('Needs a db object')
            setattr(self, Store.MINIDB_ATTR, db)

        getattr(self, Store.MINIDB_ATTR).save_or_update(self)

    def delete(self):
        if getattr(self, Store.MINIDB_ATTR) is None:
            raise ValueError('Needs a db object')

        getattr(self, Store.MINIDB_ATTR).delete_by_pk(self)

    @classmethod
    def delete_where(cls, db, query):
        return db.delete_where(cls, query)

    @classmethod
    def query(cls, db, select, where=None, order_by=None, limit=None, as_dict=False):
        return db.query(cls, select, where=where, order_by=order_by, limit=limit, as_dict=as_dict)
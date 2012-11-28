#!/usr/bin/python
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

# For Python 2.5, we need to request the "with" statement
from __future__ import with_statement

__author__ = 'Thomas Perl <m@thp.io>'
__version__ = '1.1'
__website__ = 'http://thp.io/2010/minidb/'
__license__ = 'ISC'

try:
    import sqlite3.dbapi2 as sqlite
except ImportError:
    try:
        from pysqlite2 import dbapi2 as sqlite
    except ImportError:
        raise Exception('Please install SQLite3 support.')


import threading

class Store(object):
    def __init__(self, filename=':memory:'):
        """Create (or load) a new minidb storage

        Without arguments, this will create an in-memory
        database that will be deleted when closed. If you
        pass an argument, it should be the filename of the
        database file (which will be created if it does
        not yet exist).
        """
        self.db = sqlite.connect(filename, check_same_thread=False)
        self.lock = threading.RLock()

    def _schema(self, class_):
        return class_.__name__, list(sorted(class_.__slots__))

    def _set(self, o, slot, value):
        # Set a slot on the given object to value, doing a cast if
        # necessary. The value None is special-cased and never cast.
        cls = o.__class__.__slots__[slot]
        if value is not None:
            if isinstance(value, unicode):
                value = value.decode('utf-8')
            value = cls(value)
        setattr(o, slot, value)

    def commit(self):
        """Commit changes into the database"""
        with self.lock:
            self.db.commit()

    def close(self):
        """Close the underlying database file"""
        with self.lock:
            self.db.execute('VACUUM')
            self.db.close()

    def _register(self, class_):
        with self.lock:
            table, slots = self._schema(class_)
            cur = self.db.execute('PRAGMA table_info(%s)' % table)
            available = cur.fetchall()

            if available:
                available = [row[1] for row in available]
                missing_slots = (s for s in slots if s not in available)
                for slot in missing_slots:
                    self.db.execute('ALTER TABLE %s ADD COLUMN %s TEXT' % (table,
                        slot))
            else:
                self.db.execute('CREATE TABLE %s (%s)' % (table,
                        ', '.join('%s TEXT'%s for s in slots)))

    def convert(self, v):
        """Convert a value to its string representation"""
        if isinstance(v, unicode):
            return v
        elif isinstance(v, str):
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
            self._register(o.__class__)
            table, slots = self._schema(o.__class__)

            # Only save values that have values set (non-None values)
            slots = [s for s in slots if getattr(o, s, None) is not None]

            values = [self.convert(getattr(o, slot)) for slot in slots]
            self.db.execute('INSERT INTO %s (%s) VALUES (%s)' % (table,
                ', '.join(slots), ', '.join('?'*len(slots))), values)

    def delete(self, class_, **kwargs):
        """Delete objects from the database

        Delete objects of type "class_" with the criteria
        specified in "kwargs". Please note that all objects
        that match the criteria will be deleted.

        If you want to remove a specific object from the
        database, use "remove" instead.
        """
        with self.lock:
            self._register(class_)
            table, slots = self._schema(class_)
            sql = 'DELETE FROM %s' % (table,)
            if kwargs:
                sql += ' WHERE %s' % (' AND '.join('%s=?' % k for k in kwargs))
            try:
                self.db.execute(sql, kwargs.values())
                return True
            except Exception, e:
                return False

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
            self._register(o.__class__)
            table, slots = self._schema(o.__class__)

            # Use "None" as wildcard selector in remove actions
            slots = [s for s in slots if getattr(o, s, None) is not None]

            values = [self.convert(getattr(o, slot)) for slot in slots]
            self.db.execute('DELETE FROM %s WHERE %s' % (table,
                ' AND '.join('%s=?'%s for s in slots)), values)

    def load(self, class_, **kwargs):
        """Load objects of a given class

        Return a list of objects from the database that are of
        type "class_". By default, all objects are returned,
        but a simple pre-selection can be made using keyword
        arguments.
        """
        with self.lock:
            self._register(class_)
            table, slots = self._schema(class_)
            sql = 'SELECT %s FROM %s' % (', '.join(slots), table)
            if kwargs:
                sql += ' WHERE %s' % (' AND '.join('%s=?' % k for k in kwargs))
            try:
                cur = self.db.execute(sql, kwargs.values())
            except Exception, e:
                raise
            def apply(row):
                o = class_.__new__(class_)
                for attr, value in zip(slots, row):
                    try:
                        self._set(o, attr, value)
                    except ValueError, ve:
                        return None
                return o
            return filter(lambda x: x is not None, [apply(row) for row in cur])

    def get(self, class_, **kwargs):
        """Load one object of a given class

        This is a convenience function that will load only a
        single object from the database, returning only that
        object or None when the object is not found.

        This method only makes sense when using keyword
        arguments to select the object (i.e. using a
        unique set of attributes to retrieve it).
        """
        result = self.load(class_, **kwargs)
        if result:
            return result[0]
        else:
            return None

if __name__ == '__main__':
    class Person(object):
        __slots__ = {'username': str, 'id': int}

        def __init__(self, username, id):
            self.username = username
            self.id = id

        def __repr__(self):
            return '<Person "%s" (%d)>' % (self.username, self.id)

    m = Store()
    m.save(Person('User %d' % x, x*20) for x in range(50))

    p = m.get(Person, id=200)
    print p
    m.remove(p)
    p = m.get(Person, id=200)

    # Remove some persons again (deletion by value!)
    m.remove(Person('User %d' % x, x*20) for x in range(40))

    class Person(object):
        __slots__ = {'username': str, 'id': int, 'mail': str}

        def __init__(self, username, id, mail):
            self.username = username
            self.id = id
            self.mail = mail

        def __repr__(self):
            return '<Person "%s" (%s)>' % (self.username, self.mail)

    # A schema update takes place here
    m.save(Person('User %d' % x, x*20, 'user@home.com') for x in range(50))
    print m.load(Person)


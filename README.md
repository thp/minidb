minidb: simple python object store
==================================

Store Python objects in SQLite 3. Concise, pythonic API. Fun to use.


Tutorial
--------

Let's start by importing the minidb module in Python 3:

```
>>> import minidb
```

To create a store in memory, we simply instantiate a minidb.Store, optionally
telling it to output SQL statements as debug output:

```
>>> db = minidb.Store(debug=True)
```

If you want to persist into a file, simply pass in a filename as the first
parameter when creating the minidb.Store:

```
>>> db = minidb.Store('filename.db', debug=True)
```

Note that for persisting data into the file, you actually need to call
db.close() to flush the changes to disk, and optionally db.commit() if you
want to save the changes to disk without closing the database.

To actually store objects, we need to subclass from minidb.Model (which takes
care of all the behind-the-scenes magic for making your class persistable, and
adds methods for working with the database):

```
>>> class Person(minidb.Model):
...     name = str
...     email = str
...     age = int
```

Every subclass of minidb.Model will also have a "id" attribute that is None if
an instance is not stored in the database, or an automatically assigned value
if it is in the database. This uniquely identifies an object in the database.

Now it's time to register our minidb.Model subclass with the store:

```
>>> db.register(Person)
```

This will check if the table exists, and create the necessary structure (this
output appears only when debug=True is passed to minidb.Store's constructor):

```
: PRAGMA table_info(Person)
: CREATE TABLE Person (id INTEGER PRIMARY KEY,
                       name TEXT,
                       email TEXT,
                       age INTEGER)
```

Now you can create instances of your minidb.Model subclass, optionally passing
keyword arguments that will be used to initialize the fields:

```
>>> p = Person(name='Hello World', email='minidb@example.com', age=99)
>>> p
<Person(id=None, name='Hello World', email='minidb@example.com', age=99)>
```

To store this object in the database, use .save() on the instance with the
store as sole argument:

```
>>> p.save(db)
```

In debug mode, we will see how it stores the object in the database:

```
: INSERT INTO Person (name, email, age) VALUES (?, ?, ?)
  ['Hello World', 'minidb@example.com', '99']
```

Also, it will now have its "id" attribute assigned:

```
>>> p
<Person(id=1, name='Hello World', email='minidb@example.com', age=99)>
```

The instance will remember the last minidb.Store object it was saved into or
the minidb.Store object from which it was loaded, so you can leave it out the
next time you want to save the object:

```
>>> p.name = 'Hello Again'
>>> p.save()
```

Again, the store will figure out what needs to be done:

```
: UPDATE Person SET name=?, email=?, age=? WHERE id=?
  ['Hello Again', 'minidb@example.com', '99', 1]
```

Now, let's insert some more data, just for fun:

```
>>> for i in range(10):
...     Person(name='Hello', email='x@example.org', age=10+i*3).save(db)
```

Now that we have some objects in the database, let's query all elements, and
also let's output if any of those loaded objects is the same object as p:

```
>>> for person in Person.load(db):
...     print(person, person is p)
```

The SQL query that is executed by Person.load() is:

```
: SELECT id, name, email, age FROM Person
  []
```

The output of the load looks like this:

```
<Person(id=1, name='Hello Again', email='minidb@example.com', age=99)> True
<Person(id=2, name='Hello', email='x@example.org', age=10)> False
<Person(id=3, name='Hello', email='x@example.org', age=13)> False
<Person(id=4, name='Hello', email='x@example.org', age=16)> False
<Person(id=5, name='Hello', email='x@example.org', age=19)> False
<Person(id=6, name='Hello', email='x@example.org', age=22)> False
<Person(id=7, name='Hello', email='x@example.org', age=25)> False
<Person(id=8, name='Hello', email='x@example.org', age=28)> False
<Person(id=9, name='Hello', email='x@example.org', age=31)> False
<Person(id=10, name='Hello', email='x@example.org', age=34)> False
<Person(id=11, name='Hello', email='x@example.org', age=37)> False
```

Note that the first object retrieved is actually the object p (there's no new
object created, it's the same). minidb caches objects as long as you have a
reference to them around, and will be able to retrieve those objects instead.
This makes sure that all objects stay in sync, let's try modifying an object
returned by Person.get(), a function that retrieves exactly one object:

```
>>> print(p.name)
Hello Again
>>> Person.get(db, id=1).name = 'Hello'
>>> print(p.name)
Hello
```

Now, let's try some more fancy queries. The minidb.Model subclass has a class
attribute called "c" that can be used to reference to the columns/attributes:

```
>>> Person.c
<Columns for Person (name, email, age)>
```

For example, we can query all objects for which age is between 16 and 50

```
>>> Person.load(db, (Person.c.age >= 16) & (Person.c.age <= 50))
```

This will run the following SQL query:

```
: SELECT id, name, email, age FROM Person WHERE ( age >= ? ) AND ( age <= ? )
  [16, 50]
```

Instead of querying for full objects, you can also query for columns, for
example, we can find out the minimum and maximum age value in the table:

```
>>> next(Person.query(db, Person.c.age.min // Person.c.age.max))
(10, 99)
```

The corresponding query looks like this:

```
: SELECT min(age), max(age) FROM Person
[]
```

Note that column1 // column2 is syntactic sugar for the more verbose syntax of
minidb.columns(column1, column2). The .query() method returns a generator of
rows, you can get a single row via the Python built-in next(). Each row can be
accessed in different ways:

 1. As tuple (this is also the default representation when printing a row)
 2. As dictionary
 3. As object with attributes

For example, as a dictionary:

```
>>> dict(next(Person.query(db, Person.c.age.min)))
{'min(age)': 10}
```

If you want to have nicer names, you can give your result columns names:

```
>>> dict(next(Person.query(db, Person.c.age.min('minimum_age'))))
{'minimum_age': 10}
```

The generated SQL query for renaming looks like this:

```
: SELECT min(age) AS minimum_age FROM Person
  []
```

And of course, you can access the column using attribute access:

```
>>> next(Person.query(db, Person.c.age.min('minimum_age'))).minimum_age
10
```

There is also support for SQL's ORDER BY, GROUP_BY and LIMIT, as optional
keyword arguments to .query():

```
>>> list(Person.query(db, Person.c.name // Person.c.age,
...                   order_by=Person.c.age.desc, limit=5))
```

To save typing, you can do:

```
>>> Person.c.name.query(db)

>>> (Person.c.name // Person.c.email).query(db)

>>> (Person.c.name // Person.c.age).query(db, order_by=lamdba c: c.age.desc)

>>> Person.query(db, lambda c: c.name // c.email)
```

See [`example.py`](example.py) for more examples.

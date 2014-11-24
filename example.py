import minidb


class Person(minidb.Model):
    # Constants have to be all-uppercase
    THIS_IS_A_CONSTANT = 123

    # Database columns have to be lowercase
    username = str
    mail = str
    foo = int

    # Not persisted (runtime-only) class attibutes start with underscore
    _not_persisted = float
    _foo = object

    # Custom, non-constant class attributes with dunder
    __custom_class_attribute__ = []

    # This is the custom constructor that will be called by minidb.
    # The attributes from the db will already be set when this is called.
    def __init__(self, foo):
        print('here we go now:', foo, self)
        self._not_persisted = 42.23
        self._foo = foo

    @classmethod
    def cm_foo(cls):
        print('called classmethod', cls)

    @staticmethod
    def sm_bar():
        print('called static method')

    def send_email(self):
        print('Would send e-mail to {self.username} at {self.mail}'.format(self=self))
        print('and we have _foo as', self._foo)

    @property
    def a_property(self):
        return self.username.upper()

    @property
    def read_write_property(self):
        return 'old value' + str(self.THIS_IS_A_CONSTANT)

    @read_write_property.setter
    def read_write_property(self, new):
        print('new value:', new)


class AdvancedPerson(Person):
    advanced_x = float
    advanced_y = float



class WithoutConstructor(minidb.Model):
    name = str
    age = int
    height = float


Person.__custom_class_attribute__.append(333)
print(Person.__custom_class_attribute__)
Person.cm_foo()
Person.sm_bar()

class FooObject(object):
    pass

with minidb.Store(autoregister=False, debug=True) as db:
    db.register(Person)
    db.register(WithoutConstructor)
    db.register(AdvancedPerson)

    AdvancedPerson(username='advanced', mail='a@example.net').save(db)

    for aperson in AdvancedPerson.load(db):
        print(aperson)

    for i in range(5):
        w = WithoutConstructor(name='x', age=10+3*i)
        w.height = w.age * 3.33
        w.save(db)
        print(w)
        w2 = WithoutConstructor()
        w2.name = 'xx'
        w2.age = 100 + w.age
        w2.height = w2.age * 23.33
        w2.save(db)
        print(w2)

    for i in range(3):
        p = Person(FooObject(), username='foo'*i)
        print(p)
        p.save(db)
        print(p)
        p.username *= 3
        p.save()
        pp = Person(FooObject())
        pp.username = 'bar'*i
        print(pp)
        pp.save(db)
        print(pp)

    print('loader is:', Person.load(db))

    print('query')
    for person in Person.load(db)(FooObject()):
    #for person in db.load(Person, FooObject()):
        print(person)
        if person.username == '':
            print('delete')
            person.delete()
            print('id after delete:', person.id)
            continue
        person.mail = person.username + '@example.com'
        person.save()
        print(person)

    print('query without')
    for w in WithoutConstructor.load(db):
        print(w)

    print('get without')
    w = WithoutConstructor.get(db, age=13)
    print('got:', w)

    print('requery')
    print({p.id: p for p in Person.load(db)(FooObject())})
    person = Person.get(db, id=3)(FooObject())
    #person = db.get(Person, FooObject(), id=2)
    print(person)
    person.send_email()
    print('a_property:', person.a_property)
    print('rw property:', person.read_write_property)
    person.read_write_property = 'hello'
    print('get not persisted:', person._not_persisted)
    person._not_persisted = 47.11
    print(person._not_persisted)
    person.save()

    print('RowProxy')
    for row in Person.query(db, Person.c.username // Person.c.foo):
        print('Repr:', row)
        print('Attribute access:', row.username, row.foo)
        print('Key access:', row['username'], row['foo'])
        print('Index access:', row[0], row[1])
        print('As dict:', dict(row))

    print('select with query builder')
    print('columns:', Person.c)
    query = (Person.c.id < 1000) & Person.c.username.like('%foo%') & (Person.c.username != None)
    #Person.load(db, Person.id < 1000 & Person.username.like('%foo%'))
    print('query:', query)
    print({p.id: p for p in Person.load(db, query)(FooObject())})
    print('deleting all persons with a short username')
    print(Person.delete_where(db, Person.c.username.length <= 3))

    print('what is left')
    for p in Person.load(db)(FooObject()):
        uu = next(Person.query(db, minidb.columns(Person.c.username.upper('up'),
                                                  Person.c.username.lower('down'),
                                                  Person.c.foo('foox'),
                                                  Person.c.foo),
                                                  where=(Person.c.id == p.id),
                                                  order_by=minidb.columns(Person.c.id.desc,
                                                                          Person.c.username.length.asc),
                                                  limit=1))
        print(p.id, p.username, p.mail, uu)

    print('='*30)
    print('queries')
    print('='*30)

    highest_id = next(Person.query(db, Person.c.id.max('max'))).max
    print('highest id:', highest_id)

    average_age = next(WithoutConstructor.query(db, WithoutConstructor.c.age.avg('average'))).average
    print('average age:', average_age)

    all_ages = list(WithoutConstructor.c.age.query(db, order_by=WithoutConstructor.c.age.desc))
    print('all ages:', all_ages)

    average_age = next(WithoutConstructor.c.age.avg('average').query(db, limit=1)).average
    print('average age (direct query):', average_age)

    print('multi-column query:')
    for row in WithoutConstructor.query(db, minidb.columns(WithoutConstructor.c.age,
                                                           WithoutConstructor.c.height),
                                                           order_by=WithoutConstructor.c.age.desc,
                                                           limit=50):
        print('got:', dict(row))

    print('multi-column query (direct)')
    print([dict(x) for x in minidb.columns(WithoutConstructor.c.age,
                              WithoutConstructor.c.height).query(db,
                              order_by=WithoutConstructor.c.height.desc)])

    print('order by multiple with then')
    print(list(WithoutConstructor.c.age.query(db, order_by=WithoutConstructor.c.height.asc // WithoutConstructor.c.age.desc)))

    print('order by shortcut with late-binding column lambda as dictionary')
    print(list(WithoutConstructor.c.age.query(db, order_by=lambda c: c.height.asc // c.age.desc)))

    print('multiple columns with // and as tuple')
    for age, height in (WithoutConstructor.c.age // WithoutConstructor.c.height).query(db):
        print(age, height)

    print('simple query for age')
    for (age,) in WithoutConstructor.c.age.query(db):
        print(age)

    print('late-binding column lambda')
    for name, age, height, random in WithoutConstructor.query(db, lambda c: c.name // c.age // c.height // minidb.func.random(),
            order_by=lambda c: c.height.desc // minidb.func.random().asc):
        print('got:', name, age, height, random)

    print(minidb.func.max(1, Person.c.username, 3, minidb.func.random()).tosql())
    print(minidb.func.max(Person.c.username.lower, person.c.foo.lower, 6)('maximal').tosql())

    print('...')
    print(Person.load(db, Person.c.username.like('%'))(FooObject()))


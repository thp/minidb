import minidb

class Person(object):
    __slots__ = {'username': str, 'id': int}

    def __init__(self, username, id):
        self.username = username
        self.id = id

    def __repr__(self):
        return '<Person "%s" (%d)>' % (self.username, self.id)

m = minidb.Store(debug=True)
m.save(Person('User %d' % x, x*20) for x in range(50))

p = m.get(Person, id=200)
print m.load(Person)
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


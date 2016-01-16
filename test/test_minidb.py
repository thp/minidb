import minidb

from nose.tools import *

import datetime


class FieldTest(minidb.Model):
    CONSTANT = 123

    # Persisted
    column1 = str
    column2 = int
    column3 = float
    column4 = bool

    # Not persisted per-instance attribute
    _private1 = object
    _private2 = str
    _private3 = int
    _private4 = object

    # Class attributes
    __class_attribute1__ = 'Hello'
    __class_attribute2__ = ['World']

    def __init__(self, constructor_arg):
        self._private1 = constructor_arg
        self._private2 = 'private'
        self._private3 = self.CONSTANT
        self._private4 = None

    @classmethod
    def a_classmethod(cls):
        return 'classmethod'

    @staticmethod
    def a_staticmethod():
        return 'staticmethod'

    def a_membermethod(self):
        return self._private1

    @property
    def a_read_only_property(self):
        return self._private2.upper()

    @property
    def a_read_write_property(self):
        return self._private3

    @a_read_write_property.setter
    def read_write_property(self, new_value):
        self._private3 = new_value


class FieldConversion(minidb.Model):
    integer = int
    floating = float
    boolean = bool
    string = str
    jsoninteger = minidb.JSON
    jsonfloating = minidb.JSON
    jsonboolean = minidb.JSON
    jsonstring = minidb.JSON
    jsonlist = minidb.JSON
    jsondict = minidb.JSON
    jsonnone = minidb.JSON

    @classmethod
    def create(cls):
        return cls(integer=1, floating=1.1, boolean=True, string='test',
                   jsonlist=[1, 2], jsondict={'a': 1}, jsonnone=None,
                   jsoninteger=1, jsonfloating=1.1, jsonboolean=True,
                   jsonstring='test')


def test_instantiate_fieldtest_from_code():
    field_test = FieldTest(999)
    assert field_test.id is None
    assert field_test.column1 is None
    assert field_test.column2 is None
    assert field_test.column3 is None
    assert field_test.column4 is None
    assert field_test._private1 == 999
    assert field_test._private2 is not None
    assert field_test._private3 is not None
    assert field_test._private4 is None


def test_saving_object_stores_id():
    with minidb.Store(debug=True) as db:
        db.register(FieldTest)
        field_test = FieldTest(998)
        assert field_test.id is None
        field_test.save(db)
        assert field_test.id is not None


def test_loading_object_returns_cached_object():
    with minidb.Store(debug=True) as db:
        db.register(FieldTest)
        field_test = FieldTest(9999)
        field_test._private1 = 4711
        assert field_test.id is None
        field_test.save(db)
        assert field_test.id is not None
        field_test_loaded = FieldTest.get(db, id=field_test.id)(9999)
        assert field_test_loaded._private1 == 4711
        assert field_test_loaded is field_test


def test_loading_object_returns_new_object_after_reference_drop():
    with minidb.Store(debug=True) as db:
        db.register(FieldTest)
        field_test = FieldTest(9999)
        field_test._private1 = 4711
        assert field_test.id is None
        field_test.save(db)
        assert field_test.id is not None
        field_test_id = field_test.id
        del field_test

        field_test_loaded = FieldTest.get(db, id=field_test_id)(9999)
        assert field_test_loaded._private1 == 9999


def test_loading_objects():
    with minidb.Store(debug=True) as db:
        db.register(FieldTest)
        for i in range(100):
            FieldTest(i).save(db)

        assert next(FieldTest.c.id.count('count').query(db)).count == 100

        for field_test in FieldTest.load(db)(997):
            assert field_test.id is not None
            assert field_test._private1 == 997


@raises(minidb.UnknownClass)
def test_saving_without_registration_fails():
    with minidb.Store(debug=True) as db:
        FieldTest(9).save(db)


@raises(TypeError)
def test_registering_non_subclass_of_model_fails():
    # This cannot be registered, as it's not a subclass of minidb.Model
    class Something(object):
        column = str

    with minidb.Store(debug=True) as db:
        db.register(Something)
        db.register(Something)


@raises(KeyError)
def test_invalid_keyword_arguments_fails():
    with minidb.Store(debug=True) as db:
        db.register(FieldTest)
        FieldTest(9, this_is_not_an_attribute=123).save(db)


@raises(AttributeError)
def test_invalid_column_raises_attribute_error():
    class HasOnlyColumnX(minidb.Model):
        x = int

    with minidb.Store(debug=True) as db:
        db.register(HasOnlyColumnX)
        HasOnlyColumnX.c.y


def test_json_serialization():
    class WithJsonField(minidb.Model):
        foo = str
        bar = minidb.JSON

    with minidb.Store(debug=True) as db:
        db.register(WithJsonField)
        d = {'a': 1, 'b': [1, 2, 3], 'c': [True, 4.0, {'d': 'e'}]}
        WithJsonField(bar=d).save(db)
        assert WithJsonField.get(db, id=1).bar == d


def test_json_field_query():
    class WithJsonField(minidb.Model):
        bar = minidb.JSON

    with minidb.Store(debug=True) as db:
        db.register(WithJsonField)
        d = {'a': [1, True, 3.9]}
        WithJsonField(bar=d).save(db)
        eq_(next(WithJsonField.c.bar.query(db)).bar, d)


def test_json_field_renamed_query():
    class WithJsonField(minidb.Model):
        bar = minidb.JSON

    with minidb.Store(debug=True) as db:
        db.register(WithJsonField)
        d = {'a': [1, True, 3.9]}
        WithJsonField(bar=d).save(db)
        eq_(next(WithJsonField.c.bar('renamed').query(db)).renamed, d)


def test_field_conversion_get_object():
    with minidb.Store(debug=True) as db:
        db.register(FieldConversion)
        FieldConversion.create().save(db)
        result = FieldConversion.get(db, id=1)
        assert isinstance(result.integer, int)
        assert isinstance(result.floating, float)
        assert isinstance(result.boolean, bool)
        assert isinstance(result.string, str)
        assert isinstance(result.jsoninteger, int)
        assert isinstance(result.jsonfloating, float)
        assert isinstance(result.jsonboolean, bool)
        assert isinstance(result.jsonstring, str)
        assert isinstance(result.jsonlist, list)
        assert isinstance(result.jsondict, dict)
        assert result.jsonnone is None


def test_field_conversion_query_select_star():
    with minidb.Store(debug=True) as db:
        db.register(FieldConversion)
        FieldConversion.create().save(db)
        result = next(FieldConversion.query(db, minidb.literal('*')))
        assert isinstance(result.integer, int)
        assert isinstance(result.floating, float)
        assert isinstance(result.boolean, bool)
        assert isinstance(result.string, str)
        assert isinstance(result.jsoninteger, int)
        assert isinstance(result.jsonfloating, float)
        assert isinstance(result.jsonboolean, bool)
        assert isinstance(result.jsonstring, str)
        assert isinstance(result.jsonlist, list)
        assert isinstance(result.jsondict, dict)
        assert result.jsonnone is None


def test_storing_and_retrieving_booleans():
    class BooleanModel(minidb.Model):
        value = bool

    with minidb.Store(debug=True) as db:
        db.register(BooleanModel)
        true_id = BooleanModel(value=True).save(db).id
        false_id = BooleanModel(value=False).save(db).id
        assert BooleanModel.get(db, id=true_id).value is True
        assert BooleanModel.get(db, BooleanModel.c.id == true_id).value is True
        assert BooleanModel.get(db, lambda c: c.id == true_id).value is True
        assert BooleanModel.get(db, id=false_id).value is False
        assert next(BooleanModel.c.value.query(db, where=lambda c: c.id == true_id)).value is True
        assert next(BooleanModel.c.value.query(db, where=lambda c: c.id == false_id)).value is False


def test_storing_and_retrieving_floats():
    class FloatModel(minidb.Model):
        value = float

    with minidb.Store(debug=True) as db:
        db.register(FloatModel)
        float_id = FloatModel(value=3.1415).save(db).id
        get_value = FloatModel.get(db, id=float_id).value
        assert type(get_value) == float
        assert get_value == 3.1415
        query_value = next(FloatModel.c.value.query(db, where=lambda c: c.id == float_id)).value
        assert type(query_value) == float
        assert query_value == 3.1415


def test_storing_and_retrieving_bytes():
    # http://probablyprogramming.com/2009/03/15/the-tiniest-gif-ever
    BLOB = (b'GIF89a\x01\x00\x01\x00\x80\x01\x00\xff\xff\xff\x00\x00\x00' +
            b'!\xf9\x04\x01\n\x00\x01\x00,\x00\x00\x00\x00\x01\x00\x01' +
            b'\x00\x00\x02\x02L\x01\x00;')

    class BytesModel(minidb.Model):
        value = bytes

    with minidb.Store(debug=True) as db:
        db.register(BytesModel)
        bytes_id = BytesModel(value=BLOB).save(db).id
        get_value = BytesModel.get(db, id=bytes_id).value
        assert type(get_value) == bytes
        assert get_value == BLOB
        query_value = next(BytesModel.c.value.query(db, where=lambda c: c.id == bytes_id)).value
        assert type(query_value) == bytes
        assert query_value == BLOB


@raises(ValueError)
def test_get_with_multiple_value_raises_exception():
    class Mod(minidb.Model):
        mod = str

    with minidb.Store(debug=True) as db:
        db.register(Mod)
        Mod(mod='foo').save(db)
        Mod(mod='foo').save(db)
        Mod.get(db, mod='foo')


def test_get_with_no_value_returns_none():
    class Mod(minidb.Model):
        mod = str

    with minidb.Store(debug=True) as db:
        db.register(Mod)
        assert Mod.get(db, mod='foo') is None


def test_delete_where():
    class DeleteWhere(minidb.Model):
        v = int

    with minidb.Store(debug=True) as db:
        db.register(DeleteWhere)

        for i in range(10):
            DeleteWhere(v=i).save(db)

        assert DeleteWhere.delete_where(db, lambda c: c.v < 2) == len({0, 1})
        assert DeleteWhere.delete_where(db, DeleteWhere.c.v > 5) == len({6, 7, 8, 9})
        assert {2, 3, 4, 5} == {v for (v,) in DeleteWhere.c.v.query(db)}


@raises(AttributeError)
def test_invalid_rowproxy_access_by_attribute():
    class Foo(minidb.Model):
        bar = str

    with minidb.Store(debug=True) as db:
        db.register(Foo)
        Foo(bar='baz').save(db)
        next(Foo.query(db, Foo.c.bar)).baz


@raises(KeyError)
def test_invalid_rowproxy_access_by_key():
    class Foo(minidb.Model):
        bar = str

    with minidb.Store(debug=True) as db:
        db.register(Foo)
        Foo(bar='baz').save(db)
        next(Foo.query(db, Foo.c.bar))['baz']


@raises(TypeError)
def test_use_schema_without_registration_raises_typeerror():
    with minidb.Store(debug=True) as db:
        class Foo(minidb.Model):
            bar = str
        Foo.query(db)


@raises(TypeError)
def test_use_schema_with_nonidentity_class_raises_typeerror():
    with minidb.Store(debug=True) as db:
        class Foo(minidb.Model):
            bar = str
        db.register(Foo)

        class Foo(minidb.Model):
            bar = str

        Foo.query(db)


@raises(TypeError)
def test_upgrade_schema_without_upgrade_raises_typeerror():
    with minidb.Store(debug=True) as db:
        class Foo(minidb.Model):
            bar = str

        db.register(Foo)

        class Foo(minidb.Model):
            bar = str
            baz = int

        db.register(Foo)


@raises(TypeError)
def test_reregistering_class_raises_typeerror():
    class Foo(minidb.Model):
        bar = int

    with minidb.Store(debug=True) as db:
        db.register(Foo)
        db.register(Foo)


def test_upgrade_schema_with_upgrade_succeeds():
    with minidb.Store(debug=True) as db:
        class Foo(minidb.Model):
            bar = str

        db.register(Foo)

        class Foo(minidb.Model):
            bar = str
            baz = int

        db.register(Foo, upgrade=True)


@raises(TypeError)
def test_upgrade_schema_with_different_type_raises_typeerror():
    with minidb.Store(debug=True) as db:
        class Foo(minidb.Model):
            bar = str

        db.register(Foo)

        class Foo(minidb.Model):
            bar = int

        db.register(Foo, upgrade=True)


def test_update_object():
    class Foo(minidb.Model):
        bar = str

    with minidb.Store(debug=True) as db:
        db.register(Foo)
        a = Foo(bar='a').save(db)
        b = Foo(bar='b').save(db)

        a.bar = 'c'
        a.save()

        b.bar = 'd'
        b.save()

        assert {'c', 'd'} == {bar for (bar,) in Foo.c.bar.query(db)}


def test_delete_object():
    class Foo(minidb.Model):
        bar = int

    with minidb.Store(debug=True) as db:
        db.register(Foo)
        for i in range(3):
            Foo(bar=i).save(db)

        Foo.get(db, bar=2).delete()

        assert {0, 1} == {bar for (bar,) in Foo.c.bar.query(db)}


def test_distinct():
    class Foo(minidb.Model):
        bar = str
        baz = int

    with minidb.Store(debug=True) as db:
        db.register(Foo)

        for i in range(2):
            Foo(bar='hi', baz=i).save(db)

        Foo(bar='ho', baz=7).save(db)

        expected = {('hi',), ('ho',)}

        # minidb.func.distinct(COLUMN)(NAME)
        result = {tuple(x) for x in Foo.query(db, lambda c: minidb.func.distinct(c.bar)('foo'))}
        eq_(result, expected)

        # COLUMN.distinct(NAME)
        result = {tuple(x) for x in Foo.query(db, Foo.c.bar.distinct('foo'))}
        eq_(result, expected)



def test_group_by_with_sum():
    class Foo(minidb.Model):
        bar = str
        baz = int

    with minidb.Store(debug=True) as db:
        db.register(Foo)

        for i in range(5):
            Foo(bar='hi', baz=i).save(db)

        for i in range(6):
            Foo(bar='ho', baz=i).save(db)

        expected = {('hi', sum(range(5))), ('ho', sum(range(6)))}

        # minidb.func.sum(COLUMN)(NAME)
        result = {tuple(x) for x in Foo.query(db, lambda c: c.bar //
                  minidb.func.sum(c.baz)('sum'), group_by=lambda c: c.bar)}
        eq_(result, expected)

        # COLUMN.sum(NAME)
        result = {tuple(x) for x in Foo.query(db, lambda c: c.bar //
                  c.baz.sum('sum'), group_by=lambda c: c.bar)}
        eq_(result, expected)


@raises(ValueError)
def test_save_without_db_raises_valueerror():
    class Foo(minidb.Model):
        bar = int

    Foo(bar=99).save()


@raises(ValueError)
def test_delete_without_db_raises_valueerror():
    class Foo(minidb.Model):
        bar = int

    Foo(bar=99).delete()


@raises(KeyError)
def test_double_delete_without_id_raises_valueerror():
    class Foo(minidb.Model):
        bar = str

    with minidb.Store(debug=True) as db:
        db.register(Foo)
        a = Foo(bar='hello')
        a.save(db)
        assert a.id is not None
        a.delete()
        assert a.id is None
        a.delete()


def test_default_values_are_set_if_none():
    class Foo(minidb.Model):
        name = str

        class __minidb_defaults__:
            name = 'Bob'

    with minidb.Store(debug=True) as db:
        f = Foo()
        eq_(f.name, 'Bob')

        f = Foo(name='John')
        eq_(f.name, 'John')


def test_default_values_with_callable():
    class Foo(minidb.Model):
        name = str
        email = str

        # Defaults are applied in order of slots of the Model
        # subclass, so if e.g. email depends on name to be
        # set, make sure email appears *after* name in the model
        class __minidb_defaults__:
            name = lambda o: 'Bob'
            email = lambda o: o.name + '@example.com'

    with minidb.Store(debug=True) as db:
        f = Foo()
        eq_(f.name, 'Bob')
        eq_(f.email, 'Bob@example.com')

        f = Foo(name='John')
        eq_(f.name, 'John')
        eq_(f.email, 'John@example.com')

        f = Foo(name='Joe', email='joe@example.net')
        eq_(f.name, 'Joe')
        eq_(f.email, 'joe@example.net')


def test_storing_and_retrieving_datetime():
    DT_NOW = datetime.datetime.now()
    D_TODAY = datetime.date.today()
    T_NOW = datetime.datetime.now().time()

    class DateTimeModel(minidb.Model):
        dt = datetime.datetime
        da = datetime.date
        tm = datetime.time

    with minidb.Store(debug=True) as db:
        db.register(DateTimeModel)
        datetime_id = DateTimeModel(dt=DT_NOW, da=D_TODAY, tm=T_NOW).save(db).id
        get_value = DateTimeModel.get(db, id=datetime_id)
        assert type(get_value.dt) == datetime.datetime
        assert get_value.dt == DT_NOW
        assert type(get_value.da) == datetime.date
        assert get_value.da == D_TODAY
        assert type(get_value.tm) == datetime.time
        assert get_value.tm == T_NOW
        query_value = next(DateTimeModel.query(db, lambda c: c.dt // c.da // c.tm,
                                               where=lambda c: c.id == datetime_id))
        assert type(query_value.dt) == datetime.datetime
        assert query_value.dt == DT_NOW
        assert type(query_value.da) == datetime.date
        assert query_value.da == D_TODAY
        assert type(query_value.tm) == datetime.time
        assert query_value.tm == T_NOW


def test_query_with_datetime():
    DT_NOW = datetime.datetime.now()

    class DateTimeModel(minidb.Model):
        dt = datetime.datetime

    with minidb.Store(debug=True) as db:
        db.register(DateTimeModel)
        datetime_id = DateTimeModel(dt=DT_NOW).save(db).id
        assert DateTimeModel.get(db, lambda c: c.dt == DT_NOW).id == datetime_id


def test_custom_converter():
    class Point(object):
        def __init__(self, x, y):
            self.x = x
            self.y = y

    @minidb.converter_for(Point)
    def convert_point(v, serialize):
        if serialize:
            return ','.join(str(x) for x in (v.x, v.y))
        else:
            return Point(*(float(x) for x in v.split(',')))

    class Player(minidb.Model):
        name = str
        position = Point

    with minidb.Store(debug=True) as db:
        db.register(Player)
        p = Point(1.12, 5.99)
        player_id = Player(name='Foo', position=p).save(db).id
        get_value = Player.get(db, id=player_id)
        assert type(get_value.position) == Point
        assert (get_value.position.x, get_value.position.y) == (p.x, p.y)
        query_value = next(Player.query(db, lambda c: c.position,
                                        where=lambda c: c.id == player_id))
        assert type(query_value.position) == Point
        assert (query_value.position.x, query_value.position.y) == (p.x, p.y)

import minidb
from nose.tools import *


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
    with minidb.Store(autoregister=True, debug=True) as db:
        field_test = FieldTest(998)
        assert field_test.id is None
        field_test.save(db)
        assert field_test.id is not None


def test_loading_objects():
    with minidb.Store(autoregister=True, debug=True) as db:
        for i in range(100):
            FieldTest(i).save(db)

        assert next(FieldTest.c.id.count.query(db)) == (100,)

        for field_test in FieldTest.load(db)(997):
            assert field_test.id is not None
            assert field_test._private1 == 997


@raises(minidb.UnknownClass)
def test_saving_without_registration_fails():
    with minidb.Store(autoregister=False, debug=True) as db:
        FieldTest(9).save(db)


from sqlalchemy.exc import IntegrityError, NoSuchTableError
import mvc_exceptions as mvc_exc
import dataset
from sqlalchemy.exc import NoSuchTableError

DB_name = 'test'

class UnsupportedDatabaseEngine(Exception):
    pass

def connect_to_db(db_name=None, db_engine='sqlite'):
    engines = set('postgres')
    if db_name is None:
        db_string = 'sqlite:///:memory:'
        print('New connection to in-memory SQLite DB...')
    else:
        if db_engine == 'sqlite':
            db_string = 'sqlite:///{}.db'.format(DB_name)
            print('New connection to SQLite DB...')
        elif db_engine == 'postgres':
            db_string = \
                'postgresql://test:test@localhost:5432/{}'.format(DB_name)
            print('New connection to PostgreSQL DB...')
        else:
            raise UnsupportedDatabaseEngine(
                'No database engine with this name. '
                'Choose one of the following: {}'.format(engines))

    return dataset.connect(db_string)

def create_table(conn, table_name):
    try:
        conn.load_table(table_name)
    except NoSuchTableError as e:
        print('Table {} does not exist. It will be created now'.format(e))
        conn.get_table(table_name, primary_id='name', primary_type='String')
        print('Created table {} on database {}'.format(table_name, DB_name))


def insert_one(conn, name, price, quantity, table_name):
    table = conn.load_table(table_name)
    try:
        table.insert(dict(name=name, price=price, quantity=quantity))
    except IntegrityError as e:
        raise mvc_exc.ItemAlreadyStored(
            '"{}" already stored in table "{}".\nOriginal Exception raised: {}'
            .format(name, table.table.name, e))


def insert_many(conn, items, table_name):
    # TODO: check what happens if 1+ records can be inserted but 1 fails
    table = conn.load_table(table_name)
    try:
        for x in items:
            table.insert(
                dict( name=x['name'], price=x['price'], quantity=x['quantity']))
    except IntegrityError as e:
        print('At least one in {} was already stored in table "{}".\nOriginal '
              'Exception raised: {}'
              .format([x['name'] for x in items], table.table.name, e))

def select_one(conn, name, table_name):
    table = conn.load_table(table_name)
    row = table.find_one(name=name)
    if row is not None:
        return dict(row)
    else:
        raise mvc_exc.ItemNotStored(
            'Can\'t read "{}" because it\'s not stored in table "{}"'.format(name, table.table.name))

def select_all(conn, table_name):
    table = conn.load_table(table_name)
    rows = table.all()
    return list(map(lambda x: dict(x), rows))

def update_one(conn, name, price, quantity, table_name):
    table = conn.load_table(table_name)
    row = table.find_one(name=name)
    if row is not None:
        item = {'name': name, 'price': price, 'quantity': quantity}
        table.update(item, keys=['name'])
    else:
        raise mvc_exc.ItemNotStored(
            'Can\'t update "{}" because it\'s not stored in table "{}"'.format(name, table.table.name))

def delete_one(conn, item_name, table_name):
    table = conn.load_table(table_name)
    row = table.find_one(name=item_name)
    if row is not None:
        table.delete(name=item_name)
    else:
        raise mvc_exc.ItemNotStored(
            'Can\'t delete "{}" because it\'s not stored in table "{}"'.format(item_name, table.table.name))
# -*- coding: utf-8 -*-
# @author Azusazya

import asyncio, logging
import aiomysql

def log(sql,args=()):
    logging.info('SQL: %s' % sql)

async def create_pool(loop,**kw):
    logging.info('Create a database connection pool...')
    global __pool
    __pool=await aiomysql.create_pool(
        host=kw.get('host','localhost'),
        port=kw.get('port',3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset','utf8mb4'),
        autocommit=kw.get('autocommit',True),
        maxsize=kw.get('maxsize',10),
        minsize=kw.get('minsize',1),
        loop=loop
    )

async def select(sql,args,size=None):
    log(sql,args)
    global __pool
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?','%s'),args or ())
            if size:
                rs=await cur.fetchmany(size)
            else:
                rs=await cur.fetchall()
        logging.info('rows returned: %s' % len(rs))
        return rs

async def execute(sql,args,autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?','%s'),args)
                affected=cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected

def create_args_string(num):
    return ', '.join(['?']*num)

class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name=name
        self.column_type=column_type
        self.primary_key=primary_key
        self.default=default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__,self.column_type,self.name)

class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name,ddl,primary_key,default)

class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name,'boolean',False,default)

class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


class ModelMetaclass(type):

    def __new__(mcs, name, bases, attrs):
        if name == 'Model':
            return type.__new__(mcs, name, bases, attrs)
        table_name=attrs.get('__table__',None) or name
        logging.info('Found model: %s (table: %s)' % (name, table_name))
        mappings=dict()
        fields=[]
        primary_key=None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  Found mapping: %s ==> %s' % (k, v))
                mappings[k]=v
                if v.primary_key:
                    if primary_key:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primary_key=k
                else:
                    fields.append(k)
        if not primary_key:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)

        attrs['__mappings__']=mappings
        attrs['__table__']=table_name
        attrs['__primary_key__']=primary_key
        attrs['__fields__']=fields

        attrs['__select__']='SELECT %s, %s FROM %s' % (primary_key, ', '.join(fields), table_name)
        attrs['__insert__']='INSERT INTO %s (%s, %s) VALUE (%s)' % (table_name, ', '.join(fields), primary_key, create_args_string(len(fields) + 1))
        attrs['__update__']='UPDATE %s SET %s WHERE %s = ?' % (table_name, ', '.join(map(lambda f: '%s = ?' % (mappings.get(f).name or f), fields)), primary_key)
        attrs['__delete__']='DELETE FROM %s WHERE %s = ?' % (table_name, primary_key)

        return type.__new__(mcs, name, bases, attrs)

class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super().__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key]=value

    def get_value(self, key):
        return getattr(self, key, None)

    def get_value_or_default(self, key):
        value=getattr(self, key, None)
        if value is None:
            field=self.__mappings__[key]
            if field.default is not None:
                value=field.default() if callable(field.dafault) else field.default
                logging.debug('Using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod
    async def find_all(cls, where=None, args=None, **kw):
        sql=[cls.__select__]
        if where:
            sql.append('WHERE')
            sql.append(where)
        if args is None:
            args=[]
        order_by=kw.get('order_by', None)
        if order_by:
            sql.append('ORDER BY')
            sql.append(order_by)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('LIMIT')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                # 在 args 末尾一次性添加 limit 中的所有值
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rows=await select(' '.join(sql), args)
        return [cls(**r) for r in rows]

    @classmethod
    async def find_number(cls, select_field, where=None, args=None):
        """
        Find number by SELECT and WHERE.

        :param select_field:
        :param where:
        :param args:
        :return:
        """
        sql = ['SELECT %s _num_ FROM %s' % (select_field, cls.__table__)]
        if where:
            sql.append('WHERE')
            sql.append(where)
        rows = await select(' '.join(sql), args, 1)
        if len(rows) == 0:
            return None
        return rows[0]['_num_']

    @classmethod
    async def find(cls, primary_key):
        """
        Find object by primary key

        :param primary_key:
        :return:
        """
        rows = await select('%s WHERE %s = ?' % (cls.__select__, cls.__primary_key__), [primary_key], 1)
        if len(rows) == 0:
            return None
        return cls(**rows[0])

    async def save(self):
        args = list(map(self.get_value_or_default, self.__fields__))
        args.append(self.get_value_or_default(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('Failed to insert record: affected rows: %s' % rows)
        
    async def update(self):
        args = list(map(self.get_value, self.__fields__))
        args.append(self.get_value(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('Failed to update by primary key: affected rows: %s' % rows)
        
    async def remove(self):
        args = [self.get_value(self.__primary_key__)]
        print('agrs: %s' % args)
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning('Failed to remove by primary key: affected rows %s' % rows)
    
            


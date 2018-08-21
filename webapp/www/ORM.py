import logging
import  aiomysql,asyncio
from orm import Model,StringField,IntegerField
class User(Model):
    __table__='users'
    id=IntegerField(primary_key=True)
    name=StringField()
async def create_pool(loop,**kw):
    logging.info('create database connection pool...')
    global __pool
    __pool=await  aiomysql.create_pool(
        host=kw.get('host','localhost'),
        port=kw.get('port',3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset','utf-8'),
        autocommit=kw.get('autocommit',True),
        maxsize=kw.get('maxsize',10),
        minsize=kw.get('minsize',1),
        loop=loop
    )
async def select(sql,args,size=None):
    log(sql,args)
    global __pool
    async with __pool.get() as conn:
        async  with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?','%s'),args or())
            if size:
                rs=await cur.fetchmany(size)
            else:
                rs=await  cur.fetchall()
        logging.info('rows returned: %s'%len(rs))
        return rs
async def execute(sql,args,auticommit):
    log(sql)
    async with __pool.get() as conn:
        if not auticommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?','%s'),args)
                affected=cur.rowcount
            if not auticommit:
                await conn.commit()
        except BasedException as e:
            if not auticommit:
                await conn.rollback()
            raise
        return affected

#coding=utf-8
from __future__ import division
from pyhs import Manager,exceptions
import sys,getopt,math,random,string,timeit,MySQLdb,multiprocessing

n=0

HOST='10.15.1.17'
USER='dongkai'
PASS='hello123'
DB='mydb'
PORT=3357

class MySQL:
    def __init__(self,host=HOST,user=USER,password=PASS,database=DB,port=PORT):
        self.host=host
        self.user=user
        self.password=password
        self.database=database
        self.port=port

        self.conn = MySQLdb.connect(host=self.host, user=self.user, passwd=self.password, db=self.database,port=port,charset="utf8")
        self.conn.autocommit(1)
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
    
    def escape(self,content):
        return MySQLdb.escape_string(content.encode('utf-8')).decode('utf-8')

    def the_all(self,query):
        self.cursor.execute(query.encode('utf-8'))
        return self.cursor.fetchall()

    def the_one(self,query):
        self.cursor.execute(query.encode('utf-8'))
        return self.cursor.fetchone()

    def execute(self,query):
        self.cursor.execute(query.encode('utf-8'))
        return True


def read(db):
    '''
    随机生成一个数字，查找uid匹配的记录
    '''

    uid=int(random.uniform(1,80000000))
    #print 'select uid=%d' % uid
    sql="select * from mytbl where uid=%d" % uid
    db.the_all(sql)

def insert(db):
    name=''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(10))
    #print 'insert name=%s' % name
    sql="insert into mytbl(name) values('%s')" % name
    db.execute(sql)

def delete(db):
    uid=int(random.uniform(1,80000000))
    #print 'delete uid=%d' % uid
    sql="delete from mytbl where uid=%d" % uid
    db.execute(sql)

def update(db):
    uid=int(random.uniform(1,80000000))
    name=''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(10))
    name=name + '_updated'
    #print 'update uid=%d,name=%s' % (uid,name)
    sql="update mytbl set name='%s' where uid=%d" % (name,uid)
    db.execute(sql)

def write(db):
    func=random.choice([insert , delete , update])
    func(db)

def read_thread(i,que):
    #print 'read %d start' % i
    db=MySQL()
    
    t1=timeit.Timer(lambda:read(db))
    time_t1=t1.timeit(n)

    que.put(time_t1)
    #print 'read %d end' % i

def write_thread(i,que):
    #print 'write %d start' % i
    db=MySQL()
    
    t1=timeit.Timer(lambda:write(db))
    time_t1=t1.timeit(n)
    
    que.put(time_t1)
    #print 'write %d end' % i

 
if __name__ == '__main__':
    opts, args = getopt.getopt(sys.argv[1:], "c:r:w:n:")
    c=0
    r=0
    w=0

    for op, value in opts:
        if op == '-c':
            c=int(value)
        elif op == '-r':
            r=int(value)
        elif op == '-w':
            w=int(value)
        elif op == '-n':
            n=int(value)

    r_cnt=int( math.ceil( (r*c)/(r+w) ) )
    w_cnt=int( math.ceil( (w*c)/(r+w) ) )
    
    r_jobs=list()
    w_jobs=list()
    
    rq=multiprocessing.Queue(r_cnt)
    wq=multiprocessing.Queue(w_cnt)

    if r_cnt:
        for i in range(r_cnt):
            p=multiprocessing.Process(target=read_thread, args=(i,rq))
            r_jobs.append(p)
            p.start()

    if w_cnt:
        for i in range(w_cnt):
            p=multiprocessing.Process(target=write_thread, args=(i,wq))
            w_jobs.append(p)
            p.start()
    
    rs=list()
    ws=list()
    if r_jobs:
        for i in r_jobs:
            i.join()
    if w_jobs:
        for i in w_jobs:
            i.join()
    
    #线程数*每个线程request次数
    r_requests=r_cnt * n
    w_requests=w_cnt * n
    
    rs=list()
    ws=list()
    while not rq.empty():
        rs.append(rq.get())
    while not wq.empty():
        ws.append(wq.get())

    rs_total=max(rs)
    ws_total=max(ws)
    
    print 'total %f read requests,spent %fs' % (r_requests,rs_total)
    print 'read request per second:%f' % (  float(r_requests) /float(rs_total) )
    print 'total %f write requests,spent %fs' % (w_requests,ws_total)
    print 'write request per second:%f' % ( float(w_requests) /float(ws_total) )

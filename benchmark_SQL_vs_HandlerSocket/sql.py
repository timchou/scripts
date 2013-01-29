#coding=utf-8
from __future__ import division
from pyhs import Manager,exceptions
import sys,getopt,math,random,string,timeit,MySQLdb,multiprocessing,datetime

'''
运行方法:python hs.py -c 100 -r 8 -w 2 -n 100000
其中c是总共起多少个并发，r和w是读写的比例，n是每个并发循环执行的次数
'''

n=0

HOST='10.15.1.22'
USER='dongkai'
PASS='hello123'
DB='mydb'
PORT=3306

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


def read_thread(i,que):
    #print 'read %d start' % i
    db=MySQL()
    
    start=datetime.datetime.now()
    for i in range(n):
        sql="select * from mytbl where uid=%d" % 153 
        db.the_all(sql)
    end=datetime.datetime.now()

    c=end - start
    que.put(c.seconds*1000000 + c.microseconds)

def write_thread(i,que):
    db=MySQL()

    start=datetime.datetime.now()
    for i in range(n):
        sql="update mytbl set name='%s' where uid=%d" % ('__updated',9999)
        db.execute(sql)
    end=datetime.datetime.now()

    c=end - start
    que.put(c.seconds*1000000 + c.microseconds)
    
 
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
    
    if rs:
        rs_total=max(rs)
        print 'total %f read requests,spent %fs' % (r_requests,rs_total/1000000)
        print 'read request per second:%f' % (  float(r_requests*1000000) /float(rs_total) )
    if ws:
        ws_total=max(ws)
        print 'total %f write requests,spent %fs' % (w_requests,ws_total/1000000)
        print 'write request per second:%f' % ( float(w_requests*1000000) /float(ws_total) )


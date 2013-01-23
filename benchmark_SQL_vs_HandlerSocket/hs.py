#coding=utf-8
from __future__ import division
from pyhs import Manager,exceptions
import sys,getopt,math,random,string,timeit,MySQLdb,multiprocessing

n=0

def read(hs):
    '''
    随机生成一个数字，查找uid匹配的记录
    '''

    uid=int(random.uniform(1,80000000))
    #print 'select uid=%d' % uid
    rows=hs.find('mydb', 'mytbl', '=',['uid','name'],[str(uid),])

def insert(hs):
    name=''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(10))
    hs.insert('mydb', 'mytbl', [('name', name),])

def delete(hs):
    uid=int(random.uniform(1,80000000))
    hs.delete('mydb', 'mytbl', '=' , ['uid','name'], [str(uid),])

def update(hs):
    uid=int(random.uniform(1,80000000))
    name=''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(10))
    name=name + '_updated'
    hs.update('mydb', 'mytbl', '=' , ['name',], [str(uid),] ,[name,])

def write(hs):
    func=random.choice([insert , delete , update])
    func(hs)

def read_thread(i,que):
    #print 'read %d start' % i
    hs = Manager(read_servers=[('inet','10.15.1.17',9998,None)],)
    
    t1=timeit.Timer(lambda:read(hs))
    time_t1=t1.timeit(n)
    
    que.put(time_t1)
    #print 'read %d end' % i
    #return time_t1

def write_thread(i,que):
    #print 'write %d start' % i
    hs = Manager(write_servers=[('inet','10.15.1.17',9999,None)],)
    
    t1=timeit.Timer(lambda:write(hs))
    time_t1=t1.timeit(n)

    que.put(time_t1)
    #print 'write %d end' % i
    #return time_t1

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

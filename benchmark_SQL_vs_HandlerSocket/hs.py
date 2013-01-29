#coding=utf-8
from __future__ import division
from pyhs import Manager,exceptions
import sys,getopt,math,random,string,timeit,multiprocessing,datetime

n=0


def read_thread(i,que):
    #print 'read %d start' % i
    hs = Manager(read_servers=[('inet','10.15.1.22',9998,None)],)
    
    start=datetime.datetime.now()
    for j in range(n):
        row=hs.find('mydb', 'mytbl', '=',['uid','name'],['153',],'PRIMARY','1')
    end=datetime.datetime.now()
    
    c=end - start 
    que.put(c.seconds*1000000 + c.microseconds)
    #print 'read %d end' % i
    #return time_t1

def write_thread(i,que):
    #print 'write %d start' % i
    hs = Manager(write_servers=[('inet','10.15.1.22',9999,None)],)
    
    start=datetime.datetime.now()
    for j in range(n):
        hs.update('mydb', 'mytbl', '=' , ['name',], ['9999',] ,['__updated',])
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
        print 'read'
        for i in range(r_cnt):
            p=multiprocessing.Process(target=read_thread, args=(i,rq))
            r_jobs.append(p)
            p.start()

    if w_cnt:
        print 'write'
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

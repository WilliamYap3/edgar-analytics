import csv
import os
import datetime
import collections

# create variables
timewatcher=[]
timedict=collections.defaultdict(list)
ipdict={}

# create output file
outfile="output/sessionization.txt"
w=csv.writer(open(os.path.join(os.pardir, outfile), "w"))
inactivityfile = 'input/inactivity_period.txt'
fi=open(os.path.join(os.pardir, inactivityfile),'r').read()
mynum=int(fi)
timeout = datetime.timedelta(seconds=mynum)

# read input file
def readfile(filename):
    with open(filename) as f:
        for entry in csv.DictReader(f):
            yield entry
infile = 'input/log.csv'
iterrow = iter(readfile(os.path.join(os.pardir, infile)))

# separate ip addresses based on their times
for row in iterrow:
    ipname=row['ip']
    mytime=row['date']+" "+row['time']
    rqst_time=datetime.datetime.strptime(mytime, "%Y-%m-%d %H:%M:%S")
    timedict[rqst_time].append(ipname)
    if rqst_time not in timewatcher:
        timewatcher.append(rqst_time)
    if ipname not in ipdict:
        ipdict[ipname]=[rqst_time, mytime]
    docdict={k:collections.Counter(v) for k,v in timedict.items()}
    if (rqst_time - timewatcher[0]) > timeout: 
        tk0=timewatcher[0]
        tkearly = timedict[tk0]
        try:
            for n in range(mynum+1)[1:]:
                tklate = timedict[timewatcher[n]]
                tklate+=tklate
        except:
            tklate=timedict[timewatcher[0]]           
        earlyips=set(tkearly)
        lateips =set(tklate)
        diff = earlyips.difference(lateips)
        for ip in diff:
            iptime = (rqst_time-ipdict[ip][0]).total_seconds()
            start = ipdict[ip][1]
            t = ipdict[ip][0]
            end = rqst_time
            v=0
            while t < end:
                v+=docdict[t][ip]
                t+= datetime.timedelta(seconds=1)
            w.writerow([ip, start, mytime, iptime, v])
        del timewatcher[0] 
    else:
        pass

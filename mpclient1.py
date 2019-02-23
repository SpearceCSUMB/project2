# mpclient1.py
import socket, random

USERID=''
PASSWORD=''
hosts=[]
ports=[]
DEBUG = 1

def readConfig():
    global USERID, PASSWORD, hosts, ports, DEBUG 
    f = open("config.txt")
    for line in f:
       tokens=line.split()
       if tokens[0]=='userid':
           USERID = tokens[1]
       elif tokens[0]=='password':
           PASSWORD = tokens[1]
       elif tokens[0]=='worker':
           hosts.append(tokens[1])
           ports.append(int(tokens[2]))
       elif tokens[0]=='debug':
           DEBUG = int(tokens[1])
       else:
           print("configuration file error", line)
    f.close()

class Coordinator:
    def __init__(self):
       self.sockets = []     
       # connect to all workers
       for hostname, port in zip(hosts, ports):
           sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
           sock.connect((hostname, port))
           self.sockets.append(sock)
           if DEBUG >=2:
                print("Connected to", hostname, port)
       
    
    def close(self):
        for sock in self.sockets:
           sock.close();
        self.sockets = []
        if DEBUG >= 2:
             print("All worker connections closed")
    
    def sendToAll(self, stmt):
        for sock,port in zip(self.sockets,ports):
           self.send(sock, stmt)
           if DEBUG >= 1:
               print("to port=", port, "msg=", stmt)
               
        for sock,port in zip(self.sockets,ports):
           status_msg = self.recv(sock)
           if DEBUG >= 1:
               print("from port=",port,"msg=",status_msg)
               
           # check if message received is a set of rows
           # if yes, then print one row per line
           status_msg = str(status_msg).strip()
           if status_msg.startswith("("):
               index = 0
               while index < len(status_msg) and status_msg[index]=='(':
                   index_next = status_msg.find(')',index)
                   print(status_msg[index+1:index_next])
                   index = index_next+1
   
    def recv(self,sock):
        print("In recv")
        buffer = b''
        while True:
            chunk = sock.recv(2048)
            if len(chunk)==0:
                print('connection error', sock)
                return -1
            buffer = buffer+chunk
            if buffer[-1]==0:
                return buffer[0:-1].decode('utf-8')
            
        
    def send(self, sock, msg):
        buffer = bytes(msg,'utf-8')+b'\x00'
        buflen = len(buffer)
        start = 0
        while start < buflen: 
            sentbytes = sock.send(buffer[start:])
            if sentbytes==0:
                print("connection error", sock)
                return -1
            start=start+sentbytes
        if DEBUG >= 1:
             print("send msg=",msg)
        return 0
    
    def loadTable(self, tableName, filename):
        # read file of data values which must be comma separated and in the same column order as the schema columns
        # first column is the key (which must be integer) and is hashed to distributed the data across
        # worker nodes
        f = open(filename, 'r')
        print("Opened file name: " + filename)
        for line in f:
            line = line.strip()
            sql='insert into '+tableName+' values('+line+')'
            intkey= line[0: line.index(',')]
            index = hash((int(intkey)))%len(ports)
            # index is which server to get the data
            self.send(self.sockets[index], sql)     
            rc = self.recv(self.sockets[index])
            if DEBUG >= 1:
                 print("sent",sql,"received",rc)
        f.close()
          
    
    def getRowByKey(self, sql, key):
        index = hash(key)%len(ports)
        self.send(self.sockets[index], sql)
        rc = self.recv(self.sockets[index])
        print("getRowByKey data=", rc)

    def testQueries(self):
        sqlt1 =  "select * from stud where studid = 85"
        self.sendToAll(sqlt1)
        sqlt2 =  "select * from stud where studid = 107"
        self.sendToAll(sqlt2)
        sqlt3 =  "select * from student where major = 'Biology'"
        self.sendToAll(sqlt3)
        sqlt4 =  "select * from student where gpa >= 3.2"
        self.sendToAll(sqlt4)
 
#  main 

readConfig()

# create test data and write to emp.data file
# f = open("emp.data", "w")
# for empid in range(1,100):
    # dept = random.randint(100, 105);
    # salary = random.randint(90000, 250000)
    # name = 'Joe Employee'+str(empid)
    # line = str(empid)+', "'+name+'", '+str(dept)+', '+str(salary)+'\n'
    # f.write(line)
# f.close()

#create test data and write to stud.data file
# f = open("stud.data", "w")
# list = ['Biology','Business','CS','Statistics']
# for studid in range(1, 101):
    # major = random.choice(list)
    # name = 'Student ' + str(studid)
    # gpa = round(random.uniform(2.50, 4.01), 2)
    # line = str(studid) + ', "' + name + '", ' + major + ', ' + str(gpa) + '\n'
    # f.write(line)
# f.close()


c = Coordinator()
print("Sending to all 1")
c.sendToAll("drop table if exists stud")
print("Sending to all 2")
c.sendToAll("create table stud (studid int primary key, name char(20), major char(20), gpa double)")
print("Loading table")
c.loadTable("stud", "stud.data")


# example of find by key
for studid in range(1,5):
    c.getRowByKey("select * from stud where studid="+str(studid), studid)
print("Made it past the for loop")
    
# example of find by non key
print("Sending to all with a non key")
c.sendToAll("reduce select * from stud where major='Biology' or gpa > 3.00 or name like '%7'")


# example of map-shuffle-reduce

# crate temp table
c.sendToAll("drop table if exists tempstud ")
c.sendToAll("create table tempstud (major char(20), gpa double)")

# map phase
# map phase executes the select but holds the result data at the worker
# map should be followed by shuffle
c.sendToAll("map select major, gpa from stud ")

# shuffle phase
# the result from prior map phase is distributed to servers
# and inserted into temp table.
# use {} as place holder for data values as shown below.
c.sendToAll("shuffle insert into tempstud  values {}") 

# reduce phase
# execute the select and return result to client.
print("Reduce result set")
c.sendToAll("reduce select major, avg(gpa), count(*) from tempstud  group by major order by major")

# clean up - delete temp table
c.sendToAll("drop table if exists tempstud ")


# run the test queries
c.testQueries()


c.close()

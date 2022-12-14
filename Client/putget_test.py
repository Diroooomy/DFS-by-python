from Client import Client
import time
import pyhdfs
import signal
# DFS 测试
c = Client("tcp://192.168.126.80:4242")
# c.rm("/test/data.b")
# # c.mkdir("/test")
time_start = time.time()
# c.putfile("centos.iso","/test/centos.iso")
c.getfile("/test/centos.iso")
print(time.time() - time_start)

# HDFS 测试
client = pyhdfs.HdfsClient(hosts="192.168.126.80,9000",user_name="helen")
print(client.get_home_directory())
time_start = time.time()
# client.copy_from_local("./centos.iso","/user/hadoop/centos")
client.copy_to_local("/user/hadoop/centos", "D:/Code/DFS/output/centos")
print(time.time() - time_start)

# Define signal handler function
# def myHandler(signum, frame):
#     print('I received: ', signum)

# # register signal.SIGTSTP's handler
# signal.signal(signal.SIGTERM, myHandler)
# signal.pause()
# print('End of Signal Demo')
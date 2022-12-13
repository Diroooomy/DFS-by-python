from Client import Client
import time
import pyhdfs
import signal
# DFS 测试
# c = Client("tcp://192.168.126.80:4242")
# # c.rm("/test/data.b")
# # # c.mkdir("/test")
# time_start = time.time()
# c.putfile("data.b","/test/data.b")
# print(time.time() - time_start)

# HDFS 测试
# client = pyhdfs.HdfsClient(hosts="192.168.126.80,9000",user_name="helen")
# print(client.get_home_directory())
# time_start = time.time()
# client.copy_from_local("./data.b","/user/hadoop/data.n")
# print(time.time() - time_start)

# Define signal handler function
def myHandler(signum, frame):
    print('I received: ', signum)

# register signal.SIGTSTP's handler
signal.signal(signal.SIGTERM, myHandler)
signal.pause()
print('End of Signal Demo')
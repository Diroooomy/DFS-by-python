# coding=utf-8
from ftplib import FTP
import zerorpc
import os
import paramiko
import shutil

def mkSubFile(srcName,des, cnt, buf):
    [des_filename, extname] = os.path.splitext(srcName)
    filename = des + des_filename + str(cnt) + extname
    print('正在生成子文件: %s' % filename)
    with open(filename, 'wb') as fout:
        fout.write(buf)

def splitBySize(filename,des, size):
    with open(filename, 'rb') as fin:
        buf = fin.read(size)
        cnt = 0
        while len(buf) > 0:
            mkSubFile(filename,des, cnt + 1, buf)
            cnt += 1
            buf = fin.read(size)
    return cnt

def merge(dir , name , size):
    if os.path.exists(name):
        os.remove(name)
    files = os.listdir(dir)
    print(files)
    target = name
    for file in files:
        file = dir + file
        with open(file, 'rb') as fin:
            with open(target ,'ab') as fout:
                buf = fin.read(1024)
                while len(buf) > 0:
                    fout.write(buf)
                    buf = fin.read(size)

class Client:
    def __init__(self, host):
        self.c = zerorpc.Client()
        self.c.connect(host)


    def mkdir(self, path):
        return self.c.mkdir(path)

    def putfile(self, localpath, remotepath):
        if not os.path.exists("./split"):
            os.makedirs("./split")
        # 获取允许的分块大小
        blocksize = self.c.getBlockSize()
        # blocksize = 1024
        # 将文件分块 得到块数
        blocknum = splitBySize(localpath, "./split/", blocksize)
        # 拿到各块存储位置
        location = self.c.getLocation(remotepath, blocknum)
        print(location)
        if isinstance(location, str):
            return location
        [filename, extname] = os.path.splitext(localpath.split('/')[-1])
        # 向目的地址传送文件块
        path = "./split"
        files = os.listdir(path)
        for i in range(blocknum):
            for j in location[i]:
                try:
                    t = paramiko.Transport((j[0], 22)) # 实例化连接对象
                    t.connect(username='helen',password='142578') # 建立连接
                    sftp = paramiko.SFTPClient.from_transport(t) # 使用链接建立sftp对象
                    storefilename = filename + j[1] + extname
                    sftp.put(path +'/'+ files[i], "/var/data/"+storefilename)
                    t.close() # 关闭连接
                except:
                    newloc = self.c.changeIP(filename, i + 1, j)
                    print("changeed ip: " + newloc[0] + "\n")
                    t = paramiko.Transport((newloc[0], 22)) # 实例化连接对象
                    t.connect(username='helen',password='142578') # 建立连接
                    sftp = paramiko.SFTPClient.from_transport(t) # 使用链接建立sftp对象
                    filename = str(newloc[1]) + extname
                    sftp.put(path +'/'+ files[i], "/var/data/"+filename)
                    t.close() # 关闭连接
        shutil.rmtree("./split")  

    def getfile(self, remotepath, localpath="./fileFromDatanode/"):
        # 拿到文件存储位置信息
        if not os.path.exists(localpath):
            os.makedirs(localpath)
        location = self.c.get(remotepath)
        print(location)
        # 获取允许的分块大小
        blocksize = self.c.getBlockSize()

        [filename, extname] = os.path.splitext(remotepath.split('/')[-1])

        for loc in location:
            name = loc[0] + "_" + filename + str(loc[1]) + extname
            print("name: "+ name)
            remotefilename = filename + str(loc[1]) + extname
            print(remotefilename)
            t = paramiko.Transport((loc[0], 22)) # 实例化连接对象
            t.connect(username='helen',password='142578') # 建立连接
            sftp = paramiko.SFTPClient.from_transport(t) # 使用链接建立sftp对象
            sftp.get("/var/data/"+remotefilename, localpath+loc[1])
        merge(localpath, "./merge/"+filename+extname , blocksize)
        print("合并的文件请见：./merge/"+filename+extname)
        
        shutil.rmtree(localpath)  

    def cat(self, path):
        self.getfile(path)
        [filename, extname] = os.path.splitext(path.split('/')[-1])
        file_object = open("./fileFromDatanode/" + filename + extname, 'r', encoding='utf-8')
        for string in file_object:
            print(string)
        file_object.close()

    def ls(self, path):
        return self.c.ls(path)

    def mv(self, src, des):
        return self.c.mv(src, des)

    def cp(self, src, des):
        pass

    def rm(self, path , flag=None):
        if flag == 'r':
            return self.c.rmr(path)
        else:
            return self.c.rm(path)


if __name__ == '__main__':
    client = Client("tcp://192.168.126.80:4242")
    # client.putfile("./x.txt","/test/x.txt")
    client.getfile("/test/x.txt")
    client.mkdir("/dir1")
    # client.mkdir("/dir1/dir2")
    # client.mkdir("/dir1/dir3")
    # print(client.ls("/"))
    # print(client.ls("/dir1"))
    # print(client.rm("/dir1"))
    # print(client.ls("/"))
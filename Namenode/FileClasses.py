# coding=utf-8
import queue
import random
from ftplib import FTP
import time

replica_num = 2
ips = ["192.168.126.81","192.168.126.82","192.168.126.83"]
flag = [True, True, True]
blocksize = 1024 * 1024 * 128 


# 文件基础类 记录一些文件基础信息，如文件名
class INODE:
	# 文件名称
	filename = ""

	#构造函数
	def __init__(self):
		self.filename = ""

class BLOCKNODE:
	def __init__(self):
		# 块存储位置
		self.location = []
		# 块的大小
		self.size = 0
		# 链接下一个块 这里是否需要后面再看
		self.next = None

# 文件节点类
class FILENODE(INODE):
	def __init__(self):
		INODE.__init__(self)
		# 当前文件有多少个块
		self.blocks = 0
		# 块链表头
		self.head = None
		# 文件各个块的位置
		self.locations = []

		# TODO: 备份怎么处理呢？
		# 先默认一个备份
		self.replica_num = 2
		# 备份块链表头，只设置一个备份则数组里就一个元素
		# self.copies = []

	# 构造块链 带头结点的链表
	def buildBlockList(self):
		blockhead = BLOCKNODE()
		p = blockhead
		for i in range(self.blocks):
			block = BLOCKNODE()
			block.next = None
			# 块大小固定为 1024
			block.size = 1024
			# TODO: 存储位置如何获取？
			# 随机挑选datanode的ip

			id = random.sample(range(0, len(ips)), self.replica_num)
			for j in range(len(id)):
				time_str = str(time.time())
				time_str.replace('.', '')
				time_str += str(i) + str(j)
				loc = [ips[id[j]], time_str]
				block.location.append(loc)
			self.locations.append(block.location)
			p.next = block
			p = block
		self.head = blockhead


	def changeBlockLocation(self , blocknum , i):
		'''
		@jie
		:param blocknum: 第几个块
		:param i: 第几个副本
		:return:
			更改后的ip + index
		'''
		p = self.head.next
		cnt = 1
		while cnt < blocknum:
			p = p.next
			cnt += 1

		id = ips.index(p.location[i][0])
		flag[id] = False

		ok = False
		while ok is False:
			id = random.randint(0, len(ips))
			if flag[id] is True:
				time_str = str(time.time())
				time_str.replace('.', '')
				p.location[i] = [ips[id], time_str]
				self.locations[blocknum - 1][i] = [ips[id], time_str]
				break
			else:
				continue
		return [ips[id], time_str]

	def blockLocations(self):
		'''
		@jie
		用于获取文件 得到文件各块存储位置 随机选择一个副本
		:return:
			location []  每一个块的位置
		'''
		x = self.head.next
		location = []
		while x is not None:
			t = random.randint(0, self.replica_num-1)  # 产生随机数 0 或者 1
			location.append(x.location[t])
			x = x.next
		return location


# 目录节点类
class DIRECTORYNODE(INODE):
	def __init__(self):
		INODE.__init__(self)
		# 本目录里目录数
		self.directoryNum = 0
		# 本目录里文件数
		self.fileNum = 0
		# 数组 存储子目录指针
		self.childDirectories = []
		#数组 存储子文件指针
		self.childFiles = []

	def addDirectory(self, dir):
		self.directoryNum += 1
		self.childDirectories.append(dir)

	def addFile(self, file):
		self.fileNum += 1
		self.childFiles.append(file)



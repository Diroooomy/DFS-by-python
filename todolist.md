1.json配置文件 client连接时从服务器加载,包括datanode存储位置，账户密码 namenode启动时加载，包括datanode节点信息
2.log文件完善
3.后台运行时中断处理
4.上传文件时文件名重复时设置返回信息无法上传，remotepath中文件夹不存在时返回适当信息
5.获取文件时先检查remote路径是否存在，如不存在返回适当信息
6.多线程上传文件，提升效率
7.备份丢失检测及恢复
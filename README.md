# Tiny-Redis

参考教程： https://build-your-own.org/redis/

[03](./03/) clien server 小测试 🍿

[04](./04/) 自定义协议的c / s demo  🍿

[06](./06/) 用poll来实现c / s 

- 理解STATE_REQ 和 STATE_RES

- 对于STATE_REQ 本程序一次可以读取多个请求，而对于STATE_RES 每次只回复一个请求

- 注意理解 STATE_REQ 和 STATE_RES 的状态转换

- 读取数据之后尝试进行写入，写入时循环进行，直到写入完全，之后回到 STATE_REQ状态

- 对于读 / 写 缓冲区满、读写出错、读写到EOF要进行特别处理

[07](./07/) 用c++内置map实现 get set del

- 这次实验并不难，prase cmd环节简单粗暴，debug时一定要细心

[08](./08/) 自定义hashtable代替map

[09](./09/) 序列化操作
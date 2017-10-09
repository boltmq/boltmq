## smartgonamesrv

`smartgonamesrv` is ...

Read the [docs](http://git.oschina.net/cloudzone/smartgo)



## 信号类型

一个平台的信号定义或许有些不同。下面列出了POSIX中定义的信号。
Linux使用34-64信号用作实时系统中。
命令 man 7 signal 提供了官方的信号介绍。
在POSIX.1-1990标准中定义的信号列表, 更多资料，参考 [信号类型](http://www.cnblogs.com/jkkkk/p/6180016.html)

----

| 信号	       | 值            | 值   | 说明   |
| :----------- |:-------------| :-----|:-----|
| SIGHUP        | 1          | Term   | 终端控制进程结束(终端连接断开)   |
| SIGINT        | 2          | Term   | 用户发送INTR字符(Ctrl+C)触发  |
| SIGQUIT        | 3          | Core   | 用户发送QUIT字符(Ctrl+/)触发   |
| SIGILL        | 4          | Core   | 非法指令(程序错误、试图执行数据段、栈溢出等)  |
| SIGABRT        | 6          | Core   | 调用abort函数触发   |
| SIGFPE        | 8          | Core   | 算术运行错误(浮点运算错误、除数为零等) |
| SIGKILL        | 9          | Term   | 无条件结束程序(不能被捕获、阻塞或忽略)   |
| SIGSEGV        | 11          | Core   | 无效内存引用(试图访问不属于自己的内存空间、对只读内存空间进行写操作)   |
| SIGPIPE        | 13          | Term   | 消息管道损坏(FIFO/Socket通信时，管道未打开而进行写操作)  |
| SIGALRM        | 14          | Term   | 时钟定时信号  |
| SIGTERM        | 15          | Term   | 结束程序(可以被捕获、阻塞或忽略) |

---
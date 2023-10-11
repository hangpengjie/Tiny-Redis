#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>

static void msg(const char *msg){
    fprintf(stderr,"%s\n",msg);
}
// 输出错误信息，终止程序
static void die(const char *msg){
    int err = errno;
    fprintf(stderr,"[%d] %s\n", err, msg);
    abort();
}

static void dosomething(int connfd){
    char rbuf[64] = {};
    ssize_t n = read(connfd, rbuf, sizeof(rbuf) - 1);
    if(n < 0){
        msg("read() error");
        return;
    }
    printf("client says: %s\n", rbuf);
    char wbuf[] = "world";
    write(connfd, wbuf, strlen(wbuf));
}

int main(){
    int fd = socket(AF_INET,SOCK_STREAM, 0);
    // 创建文件描述符失败
    if(fd < 0){
        die("socket()");
    }

    // 最多一个server程序
    int val = 1;
    setsockopt(fd, SOL_SOCKET,SO_REUSEADDR, &val, sizeof(val));

    // 绑定socket
    // 0.0.0.0:1234
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(0);

    int rv = bind(fd, (const sockaddr*) &addr, sizeof(addr));
    // note: 这里不为 0 即失败
    if(rv){
        die("bind()");
    }

    // 监听窗口
    rv = listen(fd,SOMAXCONN);
    // note: 这里不为 0 即失败
    if(rv){
        die("listen()");
    }   

    while (true){
        struct sockaddr_in client_addr = {};
        socklen_t socketlen = sizeof(client_addr);
        // 这里必须强制转换
        int connfd = accept(fd, (struct sockaddr*)&client_addr,&socketlen);
        // something error
        if(connfd < 0){
            continue;
        }
        dosomething(connfd);
        close(connfd);
    }
    return 0;   
}
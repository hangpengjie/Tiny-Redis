#include <assert.h>
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


// 一次性从 fd 把 n 个字节的数据全都读入到 buf
static int32_t read_full(int fd, char* buf, size_t n){
    while(n > 0){
        ssize_t rv = read(fd, buf, n);
        if(rv <= 0){
            return -1; // read() 错误 or EOF
        }
        assert((size_t) rv <= n);
        n -= (size_t) rv;
        buf += rv;
    }
    return 0;   // success
}

// 一次性将 buf 中的 n 个字节的数据全都写入到 fd
static int32_t write_full(int fd, const char* buf, size_t n){
    while(n > 0){
        ssize_t rv = write(fd, buf, n);
        if(rv <= 0){
            return -1; // write() 错误
        }
        assert((size_t) rv <= n);
        n -= (size_t) rv;
        buf += rv;
    }
    return 0;   // success
}

const size_t k_max_msg = 4096;

static int32_t one_request(int fd){
    // len data '\0'
    char rbuf[4 + k_max_msg + 1];
    errno = 0;
    int err = read_full(fd, rbuf, 4);
    // err == -1 
    if(err){
        if(errno == 0){
            msg("EOF");
        }else{
            msg("read() error");
        }
        return err;
    }
    uint32_t len = 0;
    // 假设小端存储
    memcpy(&len, rbuf, 4);
    if(len > k_max_msg){
        msg("too long");
        return -1;
    }

    // reques body
    err = read_full(fd, &rbuf[4],len);
    if(err){
        msg("read() error");
        return err;
    }

    // do something
    rbuf[4 + len] = '\0';
    printf("client says: %s\n", &rbuf[4]);

    // reply
    const char reply[] = "world";
    // note: len data '\0'
    char wbuf[4 + sizeof(reply)];
    len = (uint32_t) strlen(reply);
    memcpy(wbuf, &len, 4);
    memcpy(&wbuf[4], reply, len);
    return write_full(fd,wbuf,4 + len);
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
        // 一次只处理一个client
        while(true){
            int err = one_request(connfd);
            if(err){
                break;
            }
        }
        close(connfd);
    }
    return 0;   
}
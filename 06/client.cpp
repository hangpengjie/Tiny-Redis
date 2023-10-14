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

const int32_t k_max_msg = 4096;
static int32_t send_req(int fd, const char *text){
    uint32_t len = strlen(text);
    if(len > k_max_msg){
        msg("too long can't send");
        return -1;
    }
    // 这里其实不需要额外的1
    char wbuf[4 + k_max_msg + 1];
    memcpy(wbuf, &len, 4);
    memcpy(&wbuf[4], text, len);

    if(int32_t err = write_full(fd,wbuf, 4 + len)){
        return err;
    }
    return 0;
}
static int32_t read_res(int fd){
    uint32_t len = 0;
    char rbuf[4 + k_max_msg + 1];
    errno = 0;
    int err = read_full(fd, rbuf, 4);
    if(err){
        if(errno == 0){
            msg("EOF");
        }else{
            msg("read() error");
        }
        return err;
    }
    memcpy(&len, rbuf, 4);
    if(len > k_max_msg){
        msg("too long");
        return -1;
    }
    err = read_full(fd, &rbuf[4], len);
    if(err){
        msg("read() error");
        return err;
    }

    // do something
    rbuf[4 + len] = '\0';
    printf("server says: %s\n",&rbuf[4]);
    return 0;
}



int main(){
    int fd = socket(AF_INET,SOCK_STREAM,0);
    if(fd < 0){
        die("socket()");
    }
    // 127.0.0.1:1234
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);
    
    int rv = connect(fd,(const struct sockaddr*) & addr,sizeof(addr));
    if(rv){
        die("connect()");
    }

    const char *query_list[3] = {"helo1","hello2","hello3"};
    for(size_t i = 0; i < 3; ++i){
        int err = send_req(fd, query_list[i]);
        if(err){
            goto L_DONE;
        }
    }
    for(size_t i = 0; i < 3; ++i){
        int err = read_res(fd);
        if(err){
            goto L_DONE;
        }
    }

L_DONE:
    close(fd);
    return 0;

}
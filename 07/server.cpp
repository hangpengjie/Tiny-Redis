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
#include <poll.h>
#include <fcntl.h>
#include <vector>
#include <string>
#include <map>


const size_t k_max_msg = 4096;

enum{
    STATE_REQ = 0,  // 读取请求,其实也是初始状态，接收客户端数据
    STATE_RES = 1,  // 发送数据，表示wbuf存在数据需要将其写给客户端
    STATE_END = 2,  // 将删除此连接  
};

struct Conn{
    int fd = -1;
    uint32_t state = 0; // STATE_REQ or STATE_RES
    // for reading
    size_t rbuf_size = 0;
    uint8_t rbuf[4 + k_max_msg];
    // for writing
    size_t wbuf_size = 0;
    size_t wbuf_sent = 0;
    uint8_t wbuf[4 + k_max_msg];
};


static void msg(const char *msg){
    fprintf(stderr,"%s\n",msg);
}
// 输出错误信息，终止程序
static void die(const char *msg){
    int err = errno;
    fprintf(stderr,"[%d] %s\n", err, msg);
    abort();
}

static void fd_set_nb(int fd){
    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0);
    if (errno) {
        die("fcntl error");
        return;
    }

    flags |= O_NONBLOCK;

    errno = 0;
    (void)fcntl(fd, F_SETFL, flags);
    if (errno) {
        die("fcntl error");
    }
}

static void conn_put(std::vector<Conn*> &fd2conn, struct Conn *conn){
    if(fd2conn.size() <= (size_t)conn->fd){
        fd2conn.resize(conn->fd + 1);
    }
    fd2conn[conn->fd] = conn;
}
static int accept_new_conn(std::vector<Conn*> &fd2conn, int fd){
    struct sockaddr_in client_addr = {};
    socklen_t socklen = sizeof(client_addr);
    int connfd = accept(fd, (struct sockaddr*)&client_addr, &socklen);
    if(connfd < 0){
        msg("accept() error");
        return -1;  // error
    }
    // set nonblocking
    fd_set_nb(connfd);
    struct Conn *conn = (struct Conn*)malloc(sizeof(struct Conn));
    if(!conn){
        close(connfd);
        return -1;
    }
    conn->fd = connfd;
    conn->state = STATE_REQ;
    conn->rbuf_size = 0;
    conn->wbuf_sent = 0;
    conn->wbuf_size = 0;
    conn_put(fd2conn, conn);
    return 0;
}
static void state_req(struct Conn *conn);
static void state_res(struct Conn *conn);
// 处理IO
static void connection_io(struct Conn *conn){
    // 可以读取数据了 进行读取操作
    if(conn->state == STATE_REQ){
        state_req(conn);
    }else if(conn->state == STATE_RES){
        state_res(conn);
    }else{
        assert(0);  // not expected
    }
}
static bool try_fill_buffer(struct Conn *conn);
static bool try_flush_buffer(struct Conn *conn);
static bool try_one_request(struct Conn *conn);

static void state_req(struct Conn *conn){
    while(try_fill_buffer(conn)) {}
}
static void state_res(struct Conn *conn){
    while(try_flush_buffer(conn)){}
}
static bool try_fill_buffer(struct Conn *conn){
    assert(conn->rbuf_size < sizeof(conn->rbuf));
    ssize_t rv = 0;
    do{
        size_t cap = sizeof(conn->rbuf) - conn->rbuf_size;
        rv = read(conn->fd, &(conn->rbuf[conn->rbuf_size]), cap);
    } while(rv < 0 && errno == EINTR);
    if(rv < 0 && errno == EAGAIN){
        return false;
    }
    if(rv < 0){
        msg("read() error");
        conn->state = STATE_END;
        return false;
    }
    if(rv == 0){
        // 这个判断有点意思 当conn->rbuf_size > 0时结合后面try_one_request函数理解
        // conn->rbuf_size > 0表示读取数据不完整还应该进行读取，但是 rv == 0 表示读到了EOF，这就有问题了！！
        // 发生了未知EOF
        if(conn->rbuf_size > 0){
            msg("unexpected EOF");
        }else{
            msg("EOF");
        }
        conn->state = STATE_END;
        return false;
    }
    conn->rbuf_size += (size_t) rv;
    assert(conn->rbuf_size <= sizeof(conn->rbuf));
    // Try to process requests one by one.
    // Why is there a loop? Please read the explanation of "pipelining".
    while (try_one_request(conn)) {}
    return (conn->state == STATE_REQ);
}

static bool try_flush_buffer(struct Conn *conn){
    ssize_t rv = 0;
    do {
        size_t remain = conn->wbuf_size - conn->wbuf_sent;
        rv = write(conn->fd, &conn->wbuf[conn->wbuf_sent], remain);
    } while (rv < 0 && errno == EINTR);
    if (rv < 0 && errno == EAGAIN) {
        // got EAGAIN, stop.
        return false;
    }
    if (rv < 0) {
        msg("write() error");
        conn->state = STATE_END;
        return false;
    }
    conn->wbuf_sent += (size_t)rv;
    assert(conn->wbuf_sent <= conn->wbuf_size);
    if (conn->wbuf_sent == conn->wbuf_size) {
        // response was fully sent, change state back
        conn->state = STATE_REQ;
        conn->wbuf_sent = 0;
        conn->wbuf_size = 0;
        return false;
    }
    // still got some data in wbuf, could try to write again
    return true;
}
static int32_t do_request(
    const uint8_t * req, uint32_t reqlen,
    uint32_t *rescode, uint8_t *res, uint32_t *reslen);

static bool try_one_request(struct Conn *conn){
    if(conn->rbuf_size < 4){
        // not enough data in the buffers 
        // 下一轮再进行
        return false;
    }
    uint32_t len = 0;
    memcpy(&len, &(conn->rbuf[0]), 4);
    if(len > k_max_msg){
        msg("too long");
        conn->state = STATE_END;
        return false;
    }
    if(4 + len > conn->rbuf_size){
        // not enough data
        return false;
    }

    // got one request, generate the response
    u_int32_t rescode = 0;
    uint32_t wlen = 0;
    // 如果命令执行成功 提前将数据写入到wbuf中
    int32_t err = do_request(
        &conn->rbuf[4], len,
        &rescode, &conn->wbuf[4+4],&wlen
    );
    if(err){
        conn->state = STATE_END;
        return false;
    }
    wlen += 4;  // 将rescode的长度也算上
    memcpy(&conn->wbuf[0], &wlen, 4);   // 返回字符串长度
    memcpy(&conn->wbuf[4], &rescode, 4);// 返回的状态码
    conn->wbuf_size = 4 + wlen;

    // remove the request from the buffer.
    // note: frequent memmove is inefficient.
    // note: need better handling for production code.
    size_t remain = conn->rbuf_size - 4 - len;
    if (remain) {
        memmove(conn->rbuf, &conn->rbuf[4 + len], remain);
    }
    conn->rbuf_size = remain;

    // change state
    conn->state = STATE_RES;
    state_res(conn);

    // continue the outer loop if the request was fully processed
    return (conn->state == STATE_REQ);
}

static int32_t parse_req(const uint8_t* req, uint32_t reqlen, std::vector<std::string> &cmd);

static bool cmd_is(const std::string &word, const char *cmd) {
    return 0 == strcasecmp(word.c_str(), cmd);
}

enum{
    RES_OK = 0,
    RES_ERR = 1,
    RES_NX = 2, // 不存在
};

// just is a placeholder
// key -> val
static std::map<std::string, std::string> g_map;
static int32_t do_get(const std::vector<std::string> &cmd, uint8_t *res, uint32_t *reslen){
    // get key
    if(!g_map.count(cmd[1])){
        return RES_NX;
    }
    std::string &val = g_map[cmd[1]];
    assert(val.size() <= k_max_msg);
    memcpy(res, val.data(), val.size());
    *reslen = val.size();
    return RES_OK;
}

static int32_t do_set(const std::vector<std::string> &cmd, uint8_t *res, uint32_t *reslen){
    assert(cmd[2].size() <= k_max_msg); // 虽然冗余 但是或许还是有用的
    g_map[cmd[1]] = cmd[2];
    return RES_OK;
}

static int32_t do_del(const std::vector<std::string> &cmd, uint8_t *res, uint32_t *reslen){
    g_map.erase(cmd[1]);
    return RES_OK;
}

static int32_t do_request(
    const uint8_t * req, uint32_t reqlen,
    uint32_t *rescode, uint8_t *res, uint32_t *reslen)
{
    std::vector<std::string> cmd;   // store the cmd
    if(0 != parse_req(req, reqlen, cmd)){
        msg("bad cmd");
        return -1;
    }
    if(cmd.size() == 2 && cmd_is(cmd[0], "get")){
        *rescode = do_get(cmd,res,reslen);
    }else if(cmd.size() == 3 && cmd_is(cmd[0], "set")){
        *rescode = do_set(cmd,res,reslen);
    }else if(cmd.size() == 2 && cmd_is(cmd[0], "del")){
        *rescode = do_del(cmd, res, reslen);
    }else{
        //  cmd is not reconginzed
        *rescode = RES_ERR;
        const char *msg = "Unknown cmd";
        strcpy((char *)res, msg);
        *reslen = strlen(msg);
        return 0;
    }
    return 0;
}

static int32_t parse_req(const uint8_t* data, uint32_t len, std::vector<std::string> &out){
    if(len < 4){
        return -1;
    }
    uint32_t n = 0;
    memcpy(&n, &data[0],4);
    if(n > k_max_msg){
        return -1;
    }
    size_t pos = 4;
    while(n--){
        if(pos + 4 > len){
            return -1;
        } 
        uint32_t sz = 0;
        memcpy(&sz, &data[pos], 4);
        if(pos + 4 + sz > len){
            return -1;
        }
        out.push_back(std::string((char *)&data[pos+4], sz));
        pos += 4 + sz;
        
    }
    if(pos != len){
        return -1; // trailing garbage
    }
    return 0;
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

    // a map for all client connection, key is fd
    std::vector<Conn*> fd2conn;

    // 设置listen fd 为 nonblocking 模式
    fd_set_nb(fd);

    // the event loop
    std::vector<struct pollfd> poll_args;

    while (true){
        // prepare the arguements of poll()
        poll_args.clear();
        // 为了方便将listen fd 放到首位
        struct pollfd pfd = {fd, POLLIN, 0};
        poll_args.push_back(pfd);

        // connections fd
        for(Conn*  conn : fd2conn){
            if(!conn) continue;
            struct pollfd pfd = {};
            pfd.fd = conn->fd;
            pfd.events = conn->state == STATE_REQ ? POLLIN : POLLOUT;
            pfd.events |= POLLERR;
            poll_args.push_back(pfd);
        };
        
        // poll the activate fd
        // the timeout argument doesn't matter here
        int rv = poll(poll_args.data(),(nfds_t)poll_args.size(),1000);
        if(rv < 0){
            die("poll");
        }

        // process the connections
        for(size_t i = 1; i < poll_args.size(); ++i){
            if(poll_args[i].revents){
                Conn *conn = fd2conn[poll_args[i].fd];
                connection_io(conn);
                if(conn->state == STATE_END){
                    // client 关闭连接,或者这个连接出现了错误
                    // 关闭此conn
                    fd2conn[conn->fd] = NULL;
                    (void)close(conn->fd);
                    free(conn);
                }
            }
        }

        // 如果listen fd出现了请求，尝试去建立一个新连接
        if(poll_args[0].revents){
            (void) accept_new_conn(fd2conn, fd);
        }
    }
    return 0;   
}
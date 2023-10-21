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
#include <math.h>
#include "hashtable.h"
#include "zset.h"
// 侵入式 通过HNode指针找到Entry指针
#define container_of(ptr, type, member) ({                  \
    const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
    (type *)( (char *)__mptr - offsetof(type, member) );})

const size_t k_max_msg = 4096;
// 返回状态
enum{
    STATE_REQ = 0,  // 读取请求,其实也是初始状态，接收客户端数据
    STATE_RES = 1,  // 发送数据，表示wbuf存在数据需要将其写给客户端
    STATE_END = 2,  // 将删除此连接  
};

// 序列化
enum{
    SER_NIL = 0, // like null
    SER_ERR = 1, // an error code and msg
    SER_STR = 2, // a string
    SER_INT = 3, // a int 64
    SER_DBL = 4,
    SER_ARR = 5, // array
};
// 错误类型
enum {
    ERR_UNKNOWN = 1,
    ERR_2BIG = 2,
    ERR_TYPE = 3,
    ERR_ARG = 4,
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



static int32_t parse_req(const uint8_t* req, uint32_t reqlen, std::vector<std::string> &cmd);

static bool cmd_is(const std::string &word, const char *cmd) {
    return 0 == strcasecmp(word.c_str(), cmd);
}

enum{
    RES_OK = 0,
    RES_ERR = 1,
    RES_NX = 2, // 不存在
};

enum{
    T_STR = 0,
    T_ZSET = 1,
};

struct Entry{
    struct HNode node;
    std::string key;
    std::string val;
    uint32_t type = 0;
    ZSet *zset = NULL;
};

// cmp function
static bool entry_eq(HNode *lhs, HNode *rhs) {
    struct Entry *le = container_of(lhs, struct Entry, node);
    struct Entry *re = container_of(rhs, struct Entry, node);
    return lhs->hcode == rhs->hcode && le->key == re->key;
}
// hash function
static uint64_t str_hash(const uint8_t *data, size_t len) {
    uint32_t h = 0x811C9DC5;
    for (size_t i = 0; i < len; i++) {
        h = (h + data[i]) * 0x01000193;
    }
    return h;
}

static struct
{
    HMap db;
} g_data;

static void out_nil(std::string &out){
    out.push_back(SER_NIL);
}

static void out_str(std::string &out, const char *s, size_t size) {
    out.push_back(SER_STR);
    uint32_t len = (uint32_t)size;
    out.append((char *)&len, 4);
    out.append(s, len);
}

static void out_str(std::string &out, const std::string &val){
    out.push_back(SER_STR);
    uint32_t len = val.size();
    out.append((char *)&len, 4);
    out.append(val);
}
static void out_int(std::string &out, int64_t val){
    out.push_back(SER_INT);
    out.append((char *)&val, 8);
}
static void out_dbl(std::string &out, double val) {
    out.push_back(SER_DBL);
    out.append((char *)&val, 8);
}

static void out_err(std::string &out, int32_t code, const std::string &msg){
    out.push_back(SER_ERR);
    out.append((char *)&code, 4);
    uint32_t len = msg.size();
    out.append((char *)&len,4);
    out.append(msg);
}
static void out_arr(std::string &out, uint32_t n){
    out.push_back(SER_ARR);
    out.append((char *)&n, 4);
}

static void out_update_arr(std::string &out, uint32_t n) {
    assert(out[0] == SER_ARR);
    memcpy(&out[1], &n, 4);
}


// key -> val
static void do_get( std::vector<std::string> &cmd, std::string &out){
    // get key
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);

    if(!node){
        return out_nil(out);
    }
    std::string &val = container_of(node, Entry, node)->val;
    return out_str(out, val);
}

static void do_set(std::vector<std::string> &cmd, std::string &out){
    assert(cmd[2].size() <= k_max_msg); // 虽然冗余 但是或许还是有用的
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if(node){
        container_of(node, Entry, node)->val.swap(cmd[2]);
    }else{
        Entry *ent = new Entry();
        ent->key.swap(key.key);
        ent->val.swap(cmd[2]);
        ent->node.hcode = key.node.hcode;
        hm_insert(&g_data.db, &ent->node);
    }
    return out_nil(out);
}

static void do_del( std::vector<std::string> &cmd, std::string &out){
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *node = hm_pop(&g_data.db, &key.node, &entry_eq);
    out_int(out, node ? 1 : 0);
    if(node){
        delete container_of(node, Entry, node);
    }
    return;
}

static void h_scan(HTab *tab, void (*f)(HNode *, void *), void *arg) {
    if (tab->size == 0) {
        return;
    }
    for (size_t i = 0; i < tab->mask + 1; ++i) {
        HNode *node = tab->tab[i];
        while (node) {
            f(node, arg);
            node = node->next;
        }
    }
}

static void cb_scan(HNode *node, void *arg) {
    std::string &out = *(std::string *)arg;
    out_str(out, container_of(node, Entry, node)->key);
}

static void do_keys(std::vector<std::string> &cmd, std::string &out) {
    (void)cmd;
    out_arr(out, (uint32_t)hm_size(&g_data.db));
    h_scan(&g_data.db.ht1, &cb_scan, &out);
    h_scan(&g_data.db.ht2, &cb_scan, &out);
}


static bool str2dbl(const std::string &s, double &out) {
    char *endp = NULL;
    out = strtod(s.c_str(), &endp);
    return endp == s.c_str() + s.size() && !isnan(out);
}

static bool str2int(const std::string &s, int64_t &out) {
    char *endp = NULL;
    out = strtoll(s.c_str(), &endp, 10);
    return endp == s.c_str() + s.size();
}

// zadd zset score name
static void do_zadd(std::vector<std::string> &cmd, std::string &out) {
    double score = 0;
    if (!str2dbl(cmd[2], score)) {
        return out_err(out, ERR_ARG, "expect fp number");
    }

    // look up or create the zset
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);

    Entry *ent = NULL;
    if (!hnode) {
        ent = new Entry();
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        ent->type = T_ZSET;
        ent->zset = new ZSet();
        hm_insert(&g_data.db, &ent->node);
    } else {
        ent = container_of(hnode, Entry, node);
        if (ent->type != T_ZSET) {
            return out_err(out, ERR_TYPE, "expect zset");
        }
    }

    // add or update the tuple
    const std::string &name = cmd[3];
    bool added = zset_add(ent->zset, name.data(), name.size(), score);
    return out_int(out, (int64_t)added);
}

static bool expect_zset(std::string &out, std::string &s, Entry **ent) {
    Entry key;
    key.key.swap(s);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!hnode) {
        out_nil(out);
        return false;
    }

    *ent = container_of(hnode, Entry, node);
    if ((*ent)->type != T_ZSET) {
        out_err(out, ERR_TYPE, "expect zset");
        return false;
    }
    return true;
}

// zrem zset name
static void do_zrem(std::vector<std::string> &cmd, std::string &out) {
    Entry *ent = NULL;
    if (!expect_zset(out, cmd[1], &ent)) {
        return;
    }

    const std::string &name = cmd[2];
    ZNode *znode = zset_pop(ent->zset, name.data(), name.size());
    if (znode) {
        znode_del(znode);
    }
    return out_int(out, znode ? 1 : 0);
}

// zscore zset name
static void do_zscore(std::vector<std::string> &cmd, std::string &out) {
    Entry *ent = NULL;
    if (!expect_zset(out, cmd[1], &ent)) {
        return;
    }

    const std::string &name = cmd[2];
    ZNode *znode = zset_lookup(ent->zset, name.data(), name.size());
    return znode ? out_dbl(out, znode->score) : out_nil(out);
}

// zquery zset score name offset limit
static void do_zquery(std::vector<std::string> &cmd, std::string &out) {
    // parse args
    double score = 0;
    if (!str2dbl(cmd[2], score)) {
        return out_err(out, ERR_ARG, "expect fp number");
    }
    const std::string &name = cmd[3];
    int64_t offset = 0;
    int64_t limit = 0;
    if (!str2int(cmd[4], offset)) {
        return out_err(out, ERR_ARG, "expect int");
    }
    if (!str2int(cmd[5], limit)) {
        return out_err(out, ERR_ARG, "expect int");
    }

    // get the zset
    Entry *ent = NULL;
    if (!expect_zset(out, cmd[1], &ent)) {
        if (out[0] == SER_NIL) {
            out.clear();
            out_arr(out, 0);
        }
        return;
    }

    // look up the tuple
    if (limit <= 0) {
        return out_arr(out, 0);
    }
    ZNode *znode = zset_query(
        ent->zset, score, name.data(), name.size(), offset
    );

    // output
    out_arr(out, 0);    // the array length will be updated later
    uint32_t n = 0;
    while (znode && (int64_t)n < limit) {
        out_str(out, znode->name, znode->len);
        out_dbl(out, znode->score);
        znode = container_of(avl_offset(&znode->tree, +1), ZNode, tree);
        n += 2;
    }
    return out_update_arr(out, n);
}


static void do_request(std::vector<std::string> &cmd, std::string &out){
    if(cmd.size() == 1 && cmd_is(cmd[0], "keys")){
        do_keys(cmd,out);
    }else if(cmd.size() == 2 && cmd_is(cmd[0], "get")){
        do_get(cmd,out);
    }else if(cmd.size() == 3 && cmd_is(cmd[0], "set")){
        do_set(cmd,out);
    }else if(cmd.size() == 2 && cmd_is(cmd[0], "del")){
        do_del(cmd,out);
    }else if(cmd.size() == 4 && cmd_is(cmd[0], "zadd")){
        do_zadd(cmd,out);
    }else if (cmd.size() == 3 && cmd_is(cmd[0], "zrem")) {
        do_zrem(cmd, out);
    } else if (cmd.size() == 3 && cmd_is(cmd[0], "zscore")) {
        do_zscore(cmd, out);
    } else if (cmd.size() == 6 && cmd_is(cmd[0], "zquery")) {
        do_zquery(cmd, out);
    } else{
        out_err(out, ERR_UNKNOWN, "Unknown cmd");
    }
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
    // parse the request
    std::vector<std::string> cmd;
    if(0 != parse_req(&conn->rbuf[4], len, cmd)){
        msg("bad req");
        conn->state = STATE_END;
        return false;
    }
    // got one request generate one response
    std::string out;
    do_request(cmd,out);
    if(4 + out.size() > k_max_msg){
        out.clear();
        out_err(out, ERR_2BIG, "response is too big");
    }
    uint32_t wlen = out.size();  // 将rescode的长度也算上
    memcpy(&conn->wbuf[0], &wlen, 4);   // 返回字符串长度
    memcpy(&conn->wbuf[4], out.data(), out.size());// 返回的状态码
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
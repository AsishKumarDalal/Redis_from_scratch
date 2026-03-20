#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <fstream>
#include <unordered_map>

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <cstring>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/wait.h>

#define PORT                  6378
#define MAX_EVENTS            1024
#define BUFFER_SIZE           4096
#define REWRITE_LIMIT         (1024 * 1024)
#define RDB_WRITE_THRESHOLD   1000

#define INITIAL_HT_SIZE  4
#define LOAD_FACTOR      1.0

// one node in the linked list chain
// each bucket in hashtable is a linked list of these
struct Entry {
    std::string key;
    std::string value;
    Entry* next;
    Entry(const std::string& k, const std::string& v)
        : key(k), value(v), next(nullptr) {}
};

// the actual hashtable - just an array of Entry pointers
// size = buckets, used = no of.keys stored
struct HashTable {
    std::vector<Entry*> table;
    int size;
    int used;

    explicit HashTable(int s = 0) : size(s), used(0) {
        table.assign(size, nullptr);
    }
};

// ths is basically how redis internally stores all keys
//    keeps TWO hashtables - ht[0] is main, ht[1] used  during rehashing
// rehashidx which bucket we are currently moving over
//  rehashidx == -1 means no rehash going on
class RedisDict {
private:
    HashTable* ht[2];
    int rehashidx;

    unsigned int hash(const std::string& key) const {
        unsigned int h = 5381;
        for (unsigned char c : key)
            h = ((h << 5) + h) + c;
        return h;
    }

    // called when load factor crosses 1.0
    // creates ht[1] with double the size and starts migrating
    void startRehash() {
        ht[1]     = new HashTable(ht[0]->size * 2);
        rehashidx = 0;
    }

    // moves exactly ONE bucket from ht[0] to ht[1]
    // incremental part - we dont move everything at once (to avoid slow)
 
    void rehashStep() {
        if (rehashidx == -1) return;

        // skip empty buckets
        while (rehashidx < ht[0]->size &&
               ht[0]->table[rehashidx] == nullptr)
            ++rehashidx;

        //past all buckets rehash is done
        if (rehashidx >= ht[0]->size) {
            delete ht[0];
            ht[0]     = ht[1];
            ht[1]     = nullptr;
            rehashidx = -1;
            return;
        }

        // move all nodes in this bucket to ht[1]
        
        Entry* node = ht[0]->table[rehashidx];
        while (node) {
            Entry* next       = node->next;
            unsigned int idx  = hash(node->key) % ht[1]->size;
            node->next        = ht[1]->table[idx];
            ht[1]->table[idx] = node;
            ht[1]->used++;
            node = next;
        }
        ht[0]->table[rehashidx] = nullptr;
        ++rehashidx;
    }

public:
    RedisDict() : rehashidx(-1) {
        ht[0] = new HashTable(INITIAL_HT_SIZE);
        ht[1] = nullptr;
    }

    
    ~RedisDict() {
        for (int t = 0; t < 2; t++) {
            if (!ht[t]) continue;
            for (int i = 0; i < ht[t]->size; i++) {
                Entry* n = ht[t]->table[i];
                while (n) { Entry* nx = n->next; delete n; n = nx; }
            }
            delete ht[t];
        }
    }

    void set(const std::string& key, const std::string& value) {
        if (rehashidx != -1) rehashStep();

        // load too high
        if ((double)ht[0]->used >= ht[0]->size * LOAD_FACTOR)
            startRehash();

       
        HashTable* target = (rehashidx != -1) ? ht[1] : ht[0];
        unsigned int idx  = hash(key) % target->size;

      
        for (Entry* n = target->table[idx]; n; n = n->next) {
            if (n->key == key) { n->value = value; return; }
        }

  
        if (rehashidx != -1) {
            unsigned int idx0 = hash(key) % ht[0]->size;
            for (Entry* n = ht[0]->table[idx0]; n; n = n->next) {
                if (n->key == key) { n->value = value; return; }
            }
        }

        //  key - insert 
        Entry* e           = new Entry(key, value);
        e->next            = target->table[idx];
        target->table[idx] = e;
        target->used++;
    }

    
    std::string* get(const std::string& key) {
        if (rehashidx != -1) rehashStep();
        for (int t = 0; t <= 1; t++) {
            if (!ht[t]) continue;
            unsigned int idx = hash(key) % ht[t]->size;
            for (Entry* n = ht[t]->table[idx]; n; n = n->next)
                if (n->key == key) return &n->value;
            if (rehashidx == -1) break; 
        }
        return nullptr;
    }

    bool del(const std::string& key) {
        if (rehashidx != -1) rehashStep();

        for (int t = 0; t <= 1; t++) {
            if (!ht[t]) continue;
            unsigned int idx = hash(key) % ht[t]->size;
            Entry* n = ht[t]->table[idx], *prev = nullptr;
            while (n) {
                if (n->key == key) {
                    if (prev) prev->next        = n->next;
                    else      ht[t]->table[idx] = n->next;
                    delete n;
                    ht[t]->used--;
                    return true;
                }
                prev = n; n = n->next;
            }
            if (rehashidx == -1) break;
        }
        return false;
    }

    template<typename Fn>
    void forEach(Fn fn) const {
        for (int t = 0; t < 2; t++) {
            if (!ht[t]) continue;
            for (int i = 0; i < ht[t]->size; i++)
                for (Entry* n = ht[t]->table[i]; n; n = n->next)
                    fn(n->key, n->value);
        }
    }
    std::vector<std::pair<std::string,std::string>> dumpAll() const {
        std::vector<std::pair<std::string,std::string>> out;
        forEach([&](const std::string& k, const std::string& v) {
            out.emplace_back(k, v);
        });
        return out;
    }

    int count() const {
        return ht[0]->used + (ht[1] ? ht[1]->used : 0);
    }
};

class MiniRedis {
private:
    RedisDict data;

    

    std::unordered_map<std::string,
        std::chrono::steady_clock::time_point> expiry;

    std::ofstream aof;

    // AOF rewrite
     bool  aofRewriteInProgress = false;
    pid_t aofRewritePid        = -1;
    std::vector<std::string> rewriteBuffer; 
    // RESP is the protocol redis uses - *3\r\n$3\r\nSET\r\n etc
    static std::string respSerialize(const std::vector<std::string>& cmd) {
        std::string s = "*" + std::to_string(cmd.size()) + "\r\n";
        for (const auto& arg : cmd)
            s += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
        return s;
    }

public:
    MiniRedis() {
        loadRDB();
        loadAOF();
        aof.open("appendonly.aof", std::ios::app);
    }
    void saveRDB() {
        pid_t pid = fork();
        if (pid < 0) { std::cerr << "RDB fork failed\n"; return; }

        if (pid == 0) {
            std::ofstream out("dump.rdb",
                              std::ios::binary | std::ios::trunc);
            if (!out.is_open()) exit(1);
            data.forEach([&](const std::string& k, const std::string& v) {
                uint32_t kl = (uint32_t)k.size();
                uint32_t vl = (uint32_t)v.size();
                out.write((char*)&kl, 4);
                out.write(k.data(), kl);
                out.write((char*)&vl, 4);
                out.write(v.data(), vl);
            });
            out.close();
            exit(0);
        }
    }

    void loadRDB() {
        std::ifstream in("dump.rdb", std::ios::binary);
        if (!in.is_open()) return; 

        while (true) {
            uint32_t kl = 0;
            if (!in.read((char*)&kl, 4)) break; // EOF
            std::string key(kl, '\0');
            in.read(&key[0], kl);
            uint32_t vl = 0;
            in.read((char*)&vl, 4);
            std::string val(vl, '\0');
            in.read(&val[0], vl);
            data.set(key, val);
        }
    }

    void appendAOF(const std::vector<std::string>& cmd) {
        std::string serialized = respSerialize(cmd);

        if (aof.is_open()) {
            aof << serialized;
            aof.flush(); // flush
        }
        if (aofRewriteInProgress)
            rewriteBuffer.push_back(serialized);
    }

    void loadAOF() {
        std::ifstream file("appendonly.aof");
        if (!file.is_open()) return;

        std::string line;
        while (std::getline(file, line)) {
            if (line.empty() || line[0] != '*') continue;

            int count = std::stoi(line.substr(1));
            std::vector<std::string> cmd;

            for (int i = 0; i < count; i++) {
                std::getline(file, line);   // skip $len line
                std::getline(file, line);  
                if (!line.empty() && line.back() == '\r')
                    line.pop_back(); // strip \r from windows line endings
                cmd.push_back(line);
            }

            if (cmd.empty()) continue;
            // log=false so we dont write back to AOF while loading AOF lol
            if (cmd[0] == "SET" && cmd.size() >= 3)
                set(cmd[1], cmd[2], false);
            else if (cmd[0] == "DEL" && cmd.size() >= 2)
                del(cmd[1], false);
        }
    }

    void startAOFRewrite() {
        if (aofRewriteInProgress) return; // dont fork twice

        aofRewriteInProgress = true;
        rewriteBuffer.clear();

        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "AOF rewrite fork failed\n";
            aofRewriteInProgress = false;
            return;
        }

        if (pid == 0) {
            // child - dump current snapshot to tmp file
            // dumpAll() is safe here becoz fork gave us a frozen COW copy
            std::ofstream tmp("appendonly.aof.tmp", std::ios::trunc);
            if (!tmp.is_open()) exit(1);

            auto all = data.dumpAll();
            for (const auto& kv : all) {
                tmp << "*3\r\n$3\r\nSET\r\n";
                tmp << "$" << kv.first.size()
                    << "\r\n" << kv.first << "\r\n";
                tmp << "$" << kv.second.size()
                    << "\r\n" << kv.second << "\r\n";
            }
            tmp.flush();
            tmp.close();
            exit(0);
        }

        aofRewritePid = pid;
        std::cout << "AOF background rewrite started (pid=" << pid << ")\n";
    }
    void checkAOFRewrite() {
        if (!aofRewriteInProgress) return;

        int   status = 0;
        pid_t result = waitpid(aofRewritePid, &status, WNOHANG);

        if (result == 0) return; // child still running, check again next tick

        if (result < 0) {
            std::cerr << "waitpid error: " << strerror(errno) << "\n";
            aofRewriteInProgress = false;
            aofRewritePid        = -1;
            rewriteBuffer.clear();
            return;
        }
        if (rename("appendonly.aof.tmp", "appendonly.aof") != 0) {
            std::cerr << "rename failed: " << strerror(errno) << "\n";
            aofRewriteInProgress = false;
            aofRewritePid        = -1;
            rewriteBuffer.clear();
            return;
        }
        aof.close();
        aof.open("appendonly.aof", std::ios::app);
        if (!aof.is_open()) {
            std::cerr << "Failed to reopen AOF after rewrite\n";
        }
        for (const auto& entry : rewriteBuffer)
            aof << entry;
        aof.flush();

        rewriteBuffer.clear();
        aofRewriteInProgress = false;
        aofRewritePid        = -1;
        std::cout << "AOF background rewrite completed\n";
    }

    bool shouldRewrite() const {
        if (aofRewriteInProgress) return false;
        std::ifstream in("appendonly.aof",
                         std::ios::ate | std::ios::binary);
        if (!in.is_open()) return false;
        return (long long)in.tellg() > REWRITE_LIMIT;
    }

    void set(const std::string& key, const std::string& value,
             bool log = true) {
        data.set(key, value);
        expiry.erase(key); // setting a key clears its expiry
        if (log) appendAOF({"SET", key, value});
    }

    std::string get(const std::string& key) {
        // check if expired before returning
        auto it = expiry.find(key);
        if (it != expiry.end() &&
            std::chrono::steady_clock::now() > it->second) {
           
            data.del(key);
            expiry.erase(it);
            return "";
        }
        std::string* v = data.get(key);
        return v ? *v : "";
    }

    void del(const std::string& key, bool log = true) {
        data.del(key);
        expiry.erase(key);
        if (log) appendAOF({"DEL", key});
    }
    void activeExpireCycle(int maxChecks = 20) {
        if (expiry.empty()) return;
        auto now    = std::chrono::steady_clock::now();
        int checked = 0;
        for (auto it = expiry.begin();
             it != expiry.end() && checked < maxChecks; ) {
            if (now > it->second) {
                data.del(it->first);
                it = expiry.erase(it);
            } else ++it;
            ++checked;
        }
    }
};

struct Client {
    int         fd = -1;
    std::string in;  // incoming  buffer
    std::string out; // outgoing  buffer
};

void setNonBlocking(int fd) {
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
}

// RESP parser - reads from buf and fills out with command tokens
// returns true if we got a complete command, false if need more data
// modifies buf in place - removes parsed bytes from front
bool parseRESP(std::string& buf, std::vector<std::string>& out) {
    if (buf.empty() || buf[0] != '*') return false;

    size_t pos = buf.find("\r\n");
    if (pos == std::string::npos) return false;

    int    count = std::stoi(buf.substr(1, pos - 1)); // number of args
    size_t idx   = pos + 2;
    out.clear();

    for (int i = 0; i < count; i++) {
        if (idx >= buf.size() || buf[idx] != '$') return false;
        size_t lend = buf.find("\r\n", idx);
        if (lend == std::string::npos) return false;
        int len = std::stoi(buf.substr(idx + 1, lend - idx - 1));
        idx = lend + 2;
        if (idx + (size_t)len + 2 > buf.size()) return false; // incomplete, wait for more data
        out.push_back(buf.substr(idx, len));
        idx += len + 2;
    }

    buf.erase(0, idx); // consume the parsed bytes
    return true;
}

std::string execute(MiniRedis& redis, const std::vector<std::string>& cmd) {
    if (cmd.empty()) return "-ERR empty command\r\n";

    const std::string& op = cmd[0];

    if (op == "PING")
        return "+PONG\r\n";

    if (op == "SET" && cmd.size() >= 3) {
        redis.set(cmd[1], cmd[2]);
        return "+OK\r\n";
    }
    if (op == "GET" && cmd.size() >= 2) {
        std::string v = redis.get(cmd[1]);
        if (v.empty()) return "$-1\r\n"; // null bulk string = key not found
        return "$" + std::to_string(v.size()) + "\r\n" + v + "\r\n";
    }
    if (op == "DEL" && cmd.size() >= 2) {
        redis.del(cmd[1]);
        return ":1\r\n";
    }

    return "-ERR unknown command '" + op + "'\r\n";
}

int main() {
    MiniRedis redis;

    int server = socket(AF_INET, SOCK_STREAM, 0);
    if (server < 0) { perror("socket"); return 1; }

    // SO_REUSEADDR lets  restart server quickly without error
    int opt = 1;
    setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY; // listen on all interfaces
    addr.sin_port        = htons(PORT);

    if (bind(server, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    listen(server, SOMAXCONN);
    setNonBlocking(server);

   
    int epfd = epoll_create1(0);
    if (epfd < 0) { perror("epoll_create1"); return 1; }

    epoll_event ev{};
    ev.events  = EPOLLIN;
    ev.data.fd = server;
    epoll_ctl(epfd, EPOLL_CTL_ADD, server, &ev);

    epoll_event events[MAX_EVENTS];
    std::unordered_map<int, Client> clients;

    auto lastExpire  = std::chrono::steady_clock::now();
    auto lastRDB     = std::chrono::steady_clock::now();
    int  writeCounter = 0;

    std::cout << "MiniRedis running on port " << PORT << "\n";

    while (true) {
        //  up to 10ms for events - short timeout for later  tasks
        int n = epoll_wait(epfd, events, MAX_EVENTS, 10);

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;

            if (fd == server) {
                // new client connecting
                int cfd = accept(server, nullptr, nullptr);
                if (cfd < 0) continue;

                setNonBlocking(cfd);
                epoll_event cev{};
                cev.events  = EPOLLIN | EPOLLET;
                cev.data.fd = cfd;
                epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &cev);
                clients[cfd] = Client{cfd, {}, {}};
                continue;
            }

            if (!(events[i].events & EPOLLIN)) continue;
            bool closed = false;
            while (true) {
                char buf[BUFFER_SIZE];
                int bytes = (int)read(fd, buf, BUFFER_SIZE);
                if (bytes > 0) {
                    clients[fd].in.append(buf, bytes);
                } else if (bytes == 0) {
                    closed = true; break; // client disconnected
                } else {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) break; // done reading
                    closed = true; break; // actual error
                }
            }

            if (closed) {
                epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                close(fd);
                clients.erase(fd);
                continue;
            }

            {
                std::vector<std::string> cmd;
                // parse and execute all complete commands in buffer
                // might have multiple pipelined commands
                while (parseRESP(clients[fd].in, cmd)) {
                    clients[fd].out += execute(redis, cmd);
                    if (!cmd.empty() &&
                        (cmd[0] == "SET" || cmd[0] == "DEL"))
                        ++writeCounter;
                }

                if (!clients[fd].out.empty()) {
                    send(fd, clients[fd].out.data(),
                         clients[fd].out.size(), 0);
                    clients[fd].out.clear();
                }
            }
        }

        auto now = std::chrono::steady_clock::now();
        using ms  = std::chrono::milliseconds;
        using sec = std::chrono::seconds;

        // run expiry check every 100ms
        if (std::chrono::duration_cast<ms>(now - lastExpire).count() > 100) {
            redis.activeExpireCycle();
            lastExpire = now;
        }

        // save RDB every 1000 writes but not more than once per 30d
        if (writeCounter >= RDB_WRITE_THRESHOLD &&
            std::chrono::duration_cast<sec>(now - lastRDB).count() >= 30) {
            redis.saveRDB();
            writeCounter = 0;
            lastRDB      = now;
        }

        // check if rewrite child finished - WNOHANG so this never blocks
        redis.checkAOFRewrite();

        // trigger rewrite if AOF > 1MB
        if (redis.shouldRewrite())
            redis.startAOFRewrite();
    }

    close(server);
    return 0;
}

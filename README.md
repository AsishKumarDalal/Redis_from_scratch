# MiniRedis

A Redis clone written in C++ from scratch. No Redis libraries, no shortcuts — just raw C++ and Linux syscalls.

Built this as a learning project to actually understand how Redis works internally.

---

## What's inside

### Custom Hash Table (`RedisDict`)
Not using `std::unordered_map` for the core storage. Implemented the whole thing from scratch — djb2 hashing, chained collision resolution, and **incremental rehashing** with a two-table design (exactly how Redis's `dict.c` works).

The incremental part means when the table needs to grow, it doesn't move everything at once. It migrates one bucket per operation so the server never stalls on a giant rehash.

### AOF Persistence (Append Only File)
Every write command (`SET`, `DEL`) gets logged to `appendonly.aof` in RESP format so the dataset can be replayed on restart.

When the AOF file gets too large (> 1MB), it triggers a **background rewrite** — forks a child process that writes a compact snapshot, while the parent keeps serving clients. New writes during the rewrite go into a buffer, flushed once the child exits. Main thread never blocks.

### RDB Snapshots
Binary point-in-time snapshots saved to `dump.rdb`. Also uses `fork()` + copy-on-write so the parent doesn't pause. Triggered every 1000 writes (max once per 30s).

On startup, RDB loads first, then AOF replays on top — so you get the snapshot plus any writes that happened after it.

### epoll Event Loop
Single-threaded, non-blocking, edge-triggered I/O. Handles multiple concurrent clients without threads. `epoll_wait` with a 10ms timeout so periodic tasks (expiry, RDB, AOF check) still run between events.

### Key Expiry
Two-layer expiry:
- **Lazy** — checked when a key is accessed
- **Active** — background cycle runs every 100ms, checks up to 20 keys per tick so it doesn't block

---

## Commands supported

| Command | Description |
|---|---|
| `PING` | Returns PONG |
| `SET key value` | Set a key |
| `GET key` | Get a key |
| `DEL key` | Delete a key |

---

## How to build and run

```bash
g++ -std=c++17 -O2 -o miniredis miniredis.cpp
./miniredis
```

Server starts on port `6378`. Connect with `redis-cli`:

```bash
redis-cli -p 6378

127.0.0.1:6378> SET name "hello"
OK
127.0.0.1:6378> GET name
"hello"
127.0.0.1:6378> DEL name
(integer) 1
127.0.0.1:6378> PING
PONG
```

Files created at runtime:
- `appendonly.aof` — command log
- `dump.rdb` — binary snapshot

---

## Architecture

```
Client (redis-cli)
      │
      │ TCP / RESP protocol
      ▼
┌─────────────────────────────────┐
│         epoll event loop        │
│   (edge-triggered, non-blocking)│
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│           MiniRedis             │
│  ┌─────────────────────────┐    │
│  │       RedisDict         │    │
│  │  (two-table hash dict   │    │
│  │   incremental rehash)   │    │
│  └─────────────────────────┘    │
│  ┌───────────┐ ┌─────────────┐  │
│  │    RDB    │ │     AOF     │  │
│  │ fork+COW  │ │ fork+COW   │  │
│  │ dump.rdb  │ │ append.aof  │  │
│  └───────────┘ └─────────────┘  │
└─────────────────────────────────┘
```

---

## The fork + COW trick

The most interesting part of this whole project. When Redis needs to save data to disk it doesn't stop serving clients. It calls `fork()`.

After `fork()`, the child process gets a **copy-on-write snapshot** of the parent's memory. The OS doesn't actually copy anything yet — both processes share the same physical pages. Pages only get copied when one of them writes to it.

So the child can read the entire dataset (as it was at fork time) and write it to disk, while the parent keeps modifying memory freely. Zero locks. Zero pauses.

```
fork()
  ├── Parent → keeps serving clients, writes go to rewriteBuffer
  └── Child  → sees frozen snapshot, writes to appendonly.aof.tmp
                    │
                    ▼ exits when done
Parent detects exit via waitpid(WNOHANG)
  → rename .tmp → .aof   (atomic)
  → flush rewriteBuffer into new file
  → done
```

`waitpid(WNOHANG)` is what makes it truly non-blocking — it returns immediately if the child is still running instead of waiting for it.

---

## Things to add eventually

- [ ] `EXPIRE` / `TTL` commands (expiry map is already there, just need the commands)
- [ ] More commands — `INCR`, `MSET`, `MGET`, `EXISTS`
- [ ] Proper RESP3 support
- [ ] Config file
- [ ] Better RDB format (right now its a simple custom binary format, not compatible with real Redis)

---

## References that helped

- [build-your-own ](https://build-your-own.org/#section-redis) -fully guided by this(Great Tutorial)
  

---

## License

MIT

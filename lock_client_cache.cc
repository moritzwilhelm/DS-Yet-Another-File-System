// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc/slock.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>

#define lock(x) assert(pthread_mutex_lock(&x) == 0)
#define unlock(x) assert(pthread_mutex_unlock(&x) == 0)
#define wait(x, y) assert(pthread_cond_wait(&x, &y) == 0)
#define signal(x)  assert(pthread_cond_signal(&x) == 0)
#define broadcast(x) assert(pthread_cond_broadcast(&x) == 0)


static void *releasethread(void *x) {
    lock_client_cache *cc = (lock_client_cache *) x;
    cc->releaser();
    return nullptr;
}

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, class lock_release_user *_lu)
        : lock_client(xdst), lu(_lu) {
    assert(pthread_mutex_init(&this->seq_num_m, nullptr) == 0);
    assert(pthread_mutex_init(&this->locks_m, nullptr) == 0);
    assert(pthread_mutex_init(&this->release_reqs_m, nullptr) == 0);
    assert(pthread_cond_init(&this->revoke_called, nullptr) == 0);

    client = new rsm_client(xdst);

    srand(time(NULL) ^ last_port);
    rlock_port = ((rand() % 32000) | (0x1 << 10));
    const char *hname;
    // assert(gethostname(hname, 100) == 0);
    hname = "127.0.0.1";
    std::ostringstream host;
    host << hname << ":" << rlock_port;
    id = host.str();
    last_port = rlock_port;

    // register RPC handlers with rlsrpc
    rpcs *rlsrpc = new rpcs(rlock_port);
    rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry);
    rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke);

    pthread_t th;
    int r = pthread_create(&th, NULL, &releasethread, (void *) this);
    assert(r == 0);
}

lock_client_cache::~lock_client_cache() {
    assert(pthread_mutex_destroy(&this->seq_num_m) == 0);
    assert(pthread_mutex_destroy(&this->locks_m) == 0);
    assert(pthread_mutex_destroy(&this->release_reqs_m) == 0);
    assert(pthread_cond_destroy(&this->revoke_called) == 0);

    delete lu;
    delete client;

    // release cached non-revoked locks manually
    int r;
    for (auto &entry : this->locks) {
        Lock &lock = entry.second;
        assert(lock.stat == Lock::NONE || lock.stat == Lock::FREE);
        if (lock.stat == Lock::FREE) {
            assert(cl->call(lock_protocol::release, this->id, lock.acquire_seq_num, entry.first, r) == lock_protocol::OK);
        }
    }
}

lock_client_cache::Lock &lock_client_cache::get_lock(lock_protocol::lockid_t lid) {
    ScopedLock scopedLock(&this->locks_m);
    return this->locks.at(lid);
}

unsigned int lock_client_cache::get_next_seq_num() {
    ScopedLock scopedLock(&this->seq_num_m);
    return this->seq_num++;
}

void lock_client_cache::releaser() {
    lock(this->release_reqs_m);
    while (true) {
        for (lock_protocol::lockid_t lid : this->release_requests) {
            printf("releaser in loop\n");
            Lock &lock = this->get_lock(lid);
            lock(lock.mutex);
            while (lock.taken) {
                printf("releaser lock taken\n");
                wait(lock.release_ready_perfectly, lock.mutex);
                printf("releaser wake up\n");
            }

            printf("releaser before dorelease\n");
            int r;
            // make sure the extent cache is flushed
            if (lu != nullptr) {
                lu->dorelease(lid);
            }
            printf("releaser send to server\n");
            lock_protocol::status ret = client->call(lock_protocol::release, this->id, lock.acquire_seq_num, lid, r);
            printf("after releaser send to server\n");
            assert(lock.stat == Lock::RELEASING);
            if (ret == lock_protocol::OK) {
                lock.stat = Lock::NONE;
            } else {
                printf("releaser send to server... some error %d\n", ret);
                // reset to free to assure next revoke will try to release again
                lock.stat = Lock::FREE;
            }
            broadcast(lock.server_release_called);
            unlock(lock.mutex);
        }
        printf("releaser after iteration\n");
        this->release_requests.clear();
        wait(revoke_called, this->release_reqs_m);
        printf("releaser wake up\n");
    }
}

lock_protocol::status lock_client_cache::stat(lock_protocol::lockid_t lid) {
    int r;
    int ret = client->call(lock_protocol::stat, this->id, lid, r);
    assert(ret == lock_protocol::OK);
    return r;
}

lock_protocol::status lock_client_cache::acquire(lock_protocol::lockid_t lid) {
    printf("ACQUIRE CLIENT %llu %s\n", lid, this->id.c_str());
    lock_protocol::status ret;
    lock(this->locks_m);
    Lock &lock = this->locks[lid];
    unlock(this->locks_m);
    lock(lock.mutex);
    printf("LOCK STATUS: %s\n", (lock.stat == Lock::FREE) ? "FREE" : ((lock.stat == Lock::NONE) ? "None" : ((lock.stat == Lock::LOCKED) ? "Locked" : "Other")));
    switch (lock.stat) {
        case Lock::NONE:
            lock.stat = Lock::ACQUIRING;
            lock.acquire_seq_num = get_next_seq_num();
            int r;
            printf("call acquire server %llu %s\n", lid, this->id.c_str());
            ret = client->call(lock_protocol::acquire, this->id, lock.acquire_seq_num, lid, r);
            printf("after call acquire server %llu %s\n", lid, this->id.c_str());
            printf("%llu %s: ret %d\n", lid, this->id.c_str(), ret);
            broadcast(lock.acquire_returned);
            printf("after broadcast: %llu %s\n", lid, this->id.c_str());
            if (ret == lock_protocol::OK) {
                lock.stat = Lock::LOCKED;
                printf("LOCKED LOCK after creation %llu %s\n", lid, this->id.c_str());
            } else if (ret == lock_protocol::RETRY) {
                printf("call acquire server returned retry %llu %s\n", lid, this->id.c_str());
                lock.stat = Lock::NONE;

                struct timeval now{};
                struct timespec next_timeout{};
                next_timeout.tv_sec = now.tv_sec + 3;
                next_timeout.tv_nsec = 0;
                pthread_cond_timedwait(&lock.retry_called, &lock.mutex, &next_timeout);

                unlock(lock.mutex);
                return acquire(lid);
            } else {
                assert(false && "Unintended acquire return value");
            }
            break;
        case Lock::FREE:
            printf("ACQUIRE CLIENT %llu FREE\n", lid);
            lock.stat = Lock::LOCKED;
            printf("LOCKED LOCK after was free\n");
            break;
        case Lock::LOCKED:
            // same as RELEASING?
            wait(lock.local_release_called, lock.mutex);
            unlock(lock.mutex);
            return acquire(lid);
        case Lock::ACQUIRING:
            wait(lock.acquire_returned, lock.mutex);
            unlock(lock.mutex);
            return acquire(lid);
        case Lock::RELEASING:
            wait(lock.server_release_called, lock.mutex);
            unlock(lock.mutex);
            return acquire(lid);
    }

    lock.taken = true;
    printf("LOCK STATUS After: %s\n", (lock.stat == Lock::FREE) ? "FREE" : ((lock.stat == Lock::NONE) ? "None" : ((lock.stat == Lock::LOCKED) ? "Locked" : "Other")));
    unlock(lock.mutex);

    printf("ACQUIRE CLIENT %llu SUCCESS\n", lid);
    return lock_protocol::OK;
}

lock_protocol::status lock_client_cache::release(lock_protocol::lockid_t lid) {
    printf("RELEASE CLIENT %llu\n", lid);
    Lock &lock = this->get_lock(lid);
    // assert(lock.stat == Lock::LOCKED || lock.stat == Lock::RELEASING);
    lock(lock.mutex);
    lock.taken = false;
    broadcast(lock.local_release_called); // reset clients in locked state
    if (lock.stat == Lock::LOCKED) {
        printf("RELEASE now FREE %llu\n", lid);
        lock.stat = Lock::FREE;
    } else {
        printf("RELEASE wake up releaser thread %llu\n", lid);
        broadcast(lock.release_ready_perfectly);
    }

    unlock(lock.mutex);
    return lock_protocol::OK;
}

rlock_protocol::status lock_client_cache::retry(lock_protocol::lockid_t lid, int &) {
    Lock &lock = this->get_lock(lid);
    lock(lock.mutex);
    broadcast(lock.retry_called);
    unlock(lock.mutex);
    return rlock_protocol::OK;
}

rlock_protocol::status lock_client_cache::revoke(lock_protocol::lockid_t lid, int &) {
    Lock &lock = this->get_lock(lid);
    lock(lock.mutex);
    // assert(lock.stat == Lock::LOCKED || lock.stat == Lock::FREE);
    if (lock.stat == Lock::LOCKED || lock.stat == Lock::FREE) {
        lock.stat = Lock::RELEASING;
        lock(this->release_reqs_m);
        this->release_requests.insert(lid);
        unlock(this->release_reqs_m);
        signal(revoke_called);
    }
    unlock(lock.mutex);
    return rlock_protocol::OK;
}

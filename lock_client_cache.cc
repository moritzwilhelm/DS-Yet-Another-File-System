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
    assert (r == 0);
}

lock_client_cache::~lock_client_cache() {
    assert(pthread_mutex_destroy(&this->seq_num_m) == 0);
    assert(pthread_mutex_destroy(&this->locks_m) == 0);
    assert(pthread_mutex_destroy(&this->release_reqs_m) == 0);
    assert(pthread_cond_destroy(&this->revoke_called) == 0);

    int r;
    for (auto &entry : locks) {
        Lock &lock = entry.second;
        assert(lock.stat == Lock::NONE || lock.stat == Lock::FREE);
        if (lock.stat == Lock::FREE) {
            assert(cl->call(lock_protocol::release, this->id, get_next_seq_num(), entry.first, r) == lock_protocol::OK);
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
            //printf("RELEASER CLIENT\n");
            //lock_protocol::lockid_t lid = this->release_requests.front();
            unlock(this->release_reqs_m);
            Lock &lock = this->get_lock(lid);
            lock(lock.mutex);
            while (lock.taken) {
                wait(lock.release_ready_perfectly, lock.mutex);
            }

            int r;
            //unlock(lock.mutex);
            //printf("RELEASER SENDS %llu %s\n", lid, this->id.c_str());
            assert(cl->call(lock_protocol::release, this->id, get_next_seq_num(), lid, r) == lock_protocol::OK);
            //lock(lock.mutex);
            assert(lock.stat == Lock::RELEASING);
            lock.stat = Lock::NONE;
            broadcast(lock.server_release_called);
            unlock(lock.mutex);
            lock(this->release_reqs_m);
        }
        this->release_requests.clear();
        wait(revoke_called, this->release_reqs_m);
    }
}

lock_protocol::status lock_client_cache::stat(lock_protocol::lockid_t lid) {
    int r;
    int ret = cl->call(lock_protocol::stat, this->id, lid, r);
    assert(ret == lock_protocol::OK);
    return r;
}

lock_protocol::status lock_client_cache::acquire(lock_protocol::lockid_t lid) {
    //printf("ACQUIRE CLIENT %llu %s\n", lid, this->id.c_str());
    lock_protocol::status ret;
    lock(this->locks_m);
    Lock &lock = this->locks[lid];
    unlock(this->locks_m);
    lock(lock.mutex);
    //printf("%s\n", lock.stat == Lock::FREE ? "FREE" : "NOT-FREE");
    switch (lock.stat) {
        case Lock::NONE:
            lock.stat = Lock::ACQUIRING;
            //unlock(lock.mutex);
            int r;
            ret = cl->call(lock_protocol::acquire, this->id, get_next_seq_num(), lid, r);
            //lock(lock.mutex);
            broadcast(lock.acquire_returned);
            if (ret == lock_protocol::OK) {
                lock.stat = Lock::LOCKED;
            } else if (ret == lock_protocol::RETRY) {
                //printf("WAITING FOR RETRY %llu %s\n", lid, this->id.c_str());
                lock.stat = Lock::NONE;
                wait(lock.retry_called, lock.mutex);
                unlock(lock.mutex);
                return acquire(lid);
            } else {
                assert(false && "Unintended acquire return value");
            }
            break;
        case Lock::FREE:
            lock.stat = Lock::LOCKED;
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
            // same as LOCKED?
            wait(lock.server_release_called, lock.mutex);
            unlock(lock.mutex);
            return acquire(lid);
    }

    //printf("GRANTED CLIENT %llu %s\n", lid, this->id.c_str());
    lock.taken = true;
    unlock(lock.mutex);
    return lock_protocol::OK;
}

lock_protocol::status lock_client_cache::release(lock_protocol::lockid_t lid) {
    //printf("RELEASE CLIENT %llu %s\n", lid, this->id.c_str());
    Lock &lock = this->get_lock(lid);
    // assert(lock.stat == Lock::LOCKED || lock.stat == Lock::RELEASING);
    lock(lock.mutex);
    //printf("RELEASE %s\n", lock.stat == Lock::LOCKED ? "LOCKED" : "RELEASING");
    lock.taken = false;
    if (lock.stat == Lock::LOCKED) {
        lock.stat = Lock::FREE;
        broadcast(lock.local_release_called);
    } else {
        broadcast(lock.release_ready_perfectly);
    }

    unlock(lock.mutex);
    return lock_protocol::OK;
}

rlock_protocol::status lock_client_cache::retry(lock_protocol::lockid_t lid, int &) {
    //printf("RETRY CLIENT %llu %s\n", lid, this->id.c_str());
    Lock &lock = this->get_lock(lid);
    lock(lock.mutex);
    broadcast(lock.retry_called);
    unlock(lock.mutex);
    return rlock_protocol::OK;
}

rlock_protocol::status lock_client_cache::revoke(lock_protocol::lockid_t lid, int &) {
    //printf("REVOKE CLIENT %llu %s\n", lid, this->id.c_str());
    Lock &lock = this->get_lock(lid);
    lock(lock.mutex);
    //printf("%d\n", lock.stat);
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

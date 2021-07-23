// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

#define lock(x) assert(pthread_mutex_lock(&x) == 0)
#define unlock(x) assert(pthread_mutex_unlock(&x) == 0)
#define wait(x, y) assert(pthread_cond_wait(&x, &y) == 0)
#define signal(x)  assert(pthread_cond_signal(&x) == 0)


static void *revokethread(void *x) {
    lock_server_cache *sc = (lock_server_cache *) x;
    sc->revoker();
    return nullptr;
}

static void *retrythread(void *x) {
    lock_server_cache *sc = (lock_server_cache *) x;
    sc->retryer();
    return nullptr;
}

rpcc *lock_server_cache::get_client(const std::string &id) {
    rpcc *client;
    if (this->lock_clients.find(id) == this->lock_clients.end()) {
        sockaddr_in dstsock;
        make_sockaddr(id.c_str(), &dstsock);
        client = new rpcc(dstsock);
        if (client->bind() < 0) {
            printf("lock_client: call bind\n");
        }
        this->lock_clients.emplace(id, client);
    } else {
        client = this->lock_clients.at(id);
    }
    return client;
}

lock_server_cache::lock_server_cache(class rsm *_rsm) : lock_server(), rsm(_rsm) {
    assert(pthread_cond_init(&this->revoke_ready, nullptr) == 0);
    assert(pthread_cond_init(&this->retry_ready, nullptr) == 0);

    pthread_t th;
    int r = pthread_create(&th, NULL, &revokethread, (void *) this);
    assert(r == 0);
    r = pthread_create(&th, NULL, &retrythread, (void *) this);
    assert(r == 0);
}

lock_server_cache::~lock_server_cache() {
    assert(pthread_cond_destroy(&this->revoke_ready) == 0);
    assert(pthread_cond_destroy(&this->retry_ready) == 0);

    for (auto &entry: this->lock_clients) {
        delete entry.second;
    }
}

/**
 * This method should be a continuous loop,
 * that sends revoke messages to lock holders
 * whenever another client wants the same lock
 */
void lock_server_cache::revoker() {
    lock(this->server_lock);
    while (true) {
        while (!this->revoke_requests.empty()) {
            //printf("REVOKER WAKEUP\n");
            RevokeRequest rr = this->revoke_requests.front();
            this->revoke_requests.pop();

            int r;
            //printf("REVOKER SENDS %llu %s\n", rr.lid, rr.owner.c_str());
            rpcc *client = this->get_client(rr.owner);
            unlock(this->server_lock);
            assert(client->call(rlock_protocol::revoke, rr.lid, r) == rlock_protocol::OK);
            lock(this->server_lock);
        }
        wait(revoke_ready, server_lock);
    }
}


/**
 * This method should be a continuous loop, waiting for locks
 * to be released and then sending retry messages to those who
 * are waiting for it.
 */
void lock_server_cache::retryer() {
    lock(this->server_lock);
    while (true) {
        while (!this->retry_requests.empty()) {
            //printf("RETRYER WAKEUP\n");
            RetryRequest rr = this->retry_requests.front();
            this->retry_requests.pop();

            int r;
            for (std::string &waiter: rr.waiters) {
                //printf("RETRYER SENDS %llu %s\n", rr.lid, waiter.c_str());
                rpcc *client = this->get_client(waiter);
                unlock(this->server_lock);
                assert(client->call(rlock_protocol::retry, rr.lid, r) == rlock_protocol::OK);
                lock(this->server_lock);
            }
        }
        wait(retry_ready, server_lock);
    }

}

lock_protocol::status lock_server_cache::stat(std::string client, lock_protocol::lockid_t lid, int &r) {
    if (!rsm->amiprimary()) {
        printf("STAT not the primary\n");
        return lock_protocol::RPCERR;
    }
    lock_protocol::status ret = lock_protocol::OK;
    printf("stat request from clt %s\n", client.c_str());
    r = nacquire;
    return ret;
}

lock_protocol::status
lock_server_cache::acquire(std::string client, unsigned int seq_num, lock_protocol::lockid_t lid, int &) {
    if (!rsm->amiprimary()) {
        printf("ACQUIRE not the primary\n");
        return lock_protocol::RPCERR;
    }
    printf("ACQUIRE SERVER %llu %s\n", lid, client.c_str());
    ScopedLock scopedLock(&this->server_lock);
    Lock &lock = this->locks[lid];
    if (lock.owner.empty()) {
        lock = Lock(client);
        printf("GRANTED SERVER %llu %s\n", lid, client.c_str());
        return lock_protocol::OK;
    } else {
        lock.waiters.push_back(client);
        if (!lock.revoked) {
            this->revoke_requests.emplace(lid, lock.owner);
            signal(revoke_ready);
            lock.revoked = true;
        }
        printf("NOT GRANTED SERVER %llu %s\n", lid, client.c_str());
        printf("value %d\n", lock_protocol::RETRY);
        return lock_protocol::RETRY;
    }
}

lock_protocol::status
lock_server_cache::release(std::string client, unsigned int seq_num, lock_protocol::lockid_t lid, int &) {
    if (!rsm->amiprimary()) {
        printf("RELEASE not the primary\n");
        return lock_protocol::RPCERR;
    }
    printf("RELEASE SERVER %llu %s\n", lid, client.c_str());
    ScopedLock scopedLock(&this->server_lock);
    Lock &lock = this->locks.at(lid);
    if (lock.owner != client) {
        return lock_protocol::NOENT;
    }
    lock.owner.clear(); // lock can be re-used
    if (!lock.waiters.empty()) {
        this->retry_requests.emplace(lid, lock.waiters);
        signal(retry_ready);
    }
    return lock_protocol::OK;
}

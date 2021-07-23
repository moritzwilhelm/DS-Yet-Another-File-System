#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <unordered_map>
#include <queue>
#include <utility>
#include <vector>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include "pthread.h"

#include "rsm.h"

class lock_server_cache : public lock_server, public rsm_state_transfer {
private:
    class rsm *rsm;

    std::unordered_map<std::string, rpcc *> lock_clients;

    struct Lock {
        std::string owner;
        std::vector<std::string> waiters;
        bool revoked = false;

        Lock() = default;

        explicit Lock(std::string owner) : owner(std::move(owner)), revoked(false) {
            waiters.clear();
        }
    };

    std::unordered_map<lock_protocol::lockid_t, Lock> locks;

    // Revoke
    struct RevokeRequest {
        lock_protocol::lockid_t lid;
        std::string owner;

        RevokeRequest(lock_protocol::lockid_t lid, std::string owner) : lid(lid), owner(std::move(owner)) {};
    };

    pthread_cond_t revoke_ready;
    std::queue<RevokeRequest> revoke_requests;

    // Retry
    struct RetryRequest {
        lock_protocol::lockid_t lid;
        std::vector<std::string> waiters;

        RetryRequest(lock_protocol::lockid_t lid, std::vector<std::string> waiters) : lid(lid),
                                                                                      waiters(std::move(waiters)) {};
    };

    pthread_cond_t retry_ready;
    std::queue<RetryRequest> retry_requests;

public:
    lock_server_cache(class rsm *_rsm);

    ~lock_server_cache();

    rpcc *get_client(const std::string &id);

    lock_protocol::status stat(std::string client, lock_protocol::lockid_t, int &);

    lock_protocol::status acquire(std::string client, unsigned int seq_num, lock_protocol::lockid_t, int &);

    lock_protocol::status release(std::string client, unsigned int seq_num, lock_protocol::lockid_t, int &);

    void revoker();

    void retryer();


    std::string marshal_state() override {
        // lock any needed mutexes
        ScopedLock scopedLock(&this->server_lock);

        marshall rep;
        rep << locks.size();
        std::unordered_map<lock_protocol::lockid_t, Lock>::iterator iter_lock;
        for (iter_lock = locks.begin(); iter_lock != locks.end(); iter_lock++) {
            lock_protocol::lockid_t lid = iter_lock->first;
            Lock lock = locks[lid];
            rep << lid;
            rep << lock.owner;
            rep << lock.waiters;
            rep << lock.revoked;
        }
        // unlock any mutexes
        return rep.str();
    }

    void unmarshal_state(std::string state) override {
        // lock any needed mutexes
        ScopedLock scopedLock(&this->server_lock);

        unmarshall rep(state);
        unsigned int locks_size;
        rep >> locks_size;
        for (unsigned int i = 0; i < locks_size; i++) {
            lock_protocol::lockid_t lid;
            rep >> lid;
            Lock lock{};
            rep >> lock.owner;
            rep >> lock.waiters;
            int revoked;
            rep >> revoked;
            lock.revoked = (bool) (revoked);
            locks[lid] = lock;
        }
        // unlock any mutexes
    }
};

#endif

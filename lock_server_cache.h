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

class lock_server_cache : public lock_server {
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
    lock_server_cache();

    ~lock_server_cache();

    rpcc *get_client(const std::string &id);

    lock_protocol::status stat(std::string client, lock_protocol::lockid_t, int &);

    lock_protocol::status acquire(std::string client, unsigned int seq_num, lock_protocol::lockid_t, int &);

    lock_protocol::status release(std::string client, unsigned int seq_num, lock_protocol::lockid_t, int &);

    void revoker();

    void retryer();
};

#endif

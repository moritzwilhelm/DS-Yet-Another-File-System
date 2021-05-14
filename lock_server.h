// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"
#include <pthread.h>
#include <unordered_map>
#include <cassert>

class lock_server {

protected:
    int nacquire;

    struct Lock {
        enum {
            LOCKED, FREE
        };
        int status = FREE;
        lock_protocol::lockid_t id;
        int client_id;
        pthread_cond_t is_free_c_;

        Lock(lock_protocol::lockid_t id, int client_id) {
            this->id = id;
            this->client_id = client_id;
            assert(pthread_cond_init(&is_free_c_, nullptr) == 0);
        }

        ~Lock() {
            assert(pthread_cond_destroy(&is_free_c_) == 0);
        }
    };

    pthread_mutex_t server_lock;

    std::unordered_map<lock_protocol::lockid_t, Lock> locks;

public:
    lock_server() {
        nacquire = 0;
        assert(pthread_mutex_init(&server_lock, nullptr) == 0);
    };

    ~lock_server() {
        assert(pthread_mutex_destroy(&server_lock) == 0);
    };

    lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);

    lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);

    lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 








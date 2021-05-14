// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "slock.h"

lock_protocol::status lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r) {
    lock_protocol::status ret = lock_protocol::OK;
    printf("stat request from clt %d\n", clt);
    r = nacquire;
    return ret;
}

lock_protocol::status lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &) {
    ScopedLock scoped_sl(&this->server_lock);

    // create and add lock lid to locks map if it does not exist
    if (this->locks.find(lid) == this->locks.end())
        this->locks.emplace(lid, Lock(lid, clt));
    Lock &lock = this->locks.at(lid);

    // wait until lock is free
    while (lock.status == Lock::LOCKED)
        assert(pthread_cond_wait(&lock.is_free_c_, &this->server_lock) == 0);

    lock.status = Lock::LOCKED;
    lock.client_id = clt;

    return lock_protocol::OK;
}

lock_protocol::status lock_server::release(int clt, lock_protocol::lockid_t lid, int &) {
    ScopedLock scoped_sl(&this->server_lock);

    if (this->locks.find(lid) != this->locks.end()) {
        Lock &lock = this->locks.at(lid);

        // client does not hold the requested lock
        if (lock.client_id != clt)
            return lock_protocol::RPCERR;

        lock.status = Lock::FREE;
        lock.client_id = -1;
        assert(pthread_cond_signal(&lock.is_free_c_) == 0);

        return lock_protocol::OK;
    }

    // lock does not exist
    return lock_protocol::RPCERR;
}

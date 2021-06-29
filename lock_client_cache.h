// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include <unordered_map>
#include "pthread.h"
#include <unordered_set>

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 6.
class lock_release_user {
public:
    virtual void dorelease(lock_protocol::lockid_t) = 0;

    virtual ~lock_release_user() {};
};


// SUGGESTED LOCK CACHING IMPLEMENTATION PLAN:
//
// to work correctly for lab 7,  all the requests on the server run till 
// completion and threads wait on condition variables on the client to
// wait for a lock.  this allows the server to be replicated using the
// replicated state machine approach.
//
// On the client a lock can be in several states:
//  - free: client owns the lock and no thread has it
//  - locked: client owns the lock and a thread has it
//  - acquiring: the client is acquiring ownership
//  - releasing: the client is releasing ownership
//
// in the state acquiring and locked there may be several threads
// waiting for the lock, but the first thread in the list interacts
// with the server and wakes up the threads when its done (released
// the lock).  a thread in the list is identified by its thread id
// (tid).
//
// a thread is in charge of getting a lock: if the server cannot grant
// it the lock, the thread will receive a retry reply.  at some point
// later, the server sends the thread a retry RPC, encouraging the client
// thread to ask for the lock again.
//
// once a thread has acquired a lock, its client obtains ownership of
// the lock. the client can grant the lock to other threads on the client 
// without interacting with the server. 
//
// the server must send the client a revoke request to get the lock back. this
// request tells the client to send the lock back to the
// server when the lock is released or right now if no thread on the
// client is holding the lock.  when receiving a revoke request, the
// client adds it to a list and wakes up a releaser thread, which returns
// the lock the server as soon it is free.
//
// the releasing is done in a separate a thread to avoid
// deadlocks and to ensure that revoke and retry RPCs from the server
// run to completion (i.e., the revoke RPC cannot do the release when
// the lock is free.
//
// a challenge in the implementation is that retry and revoke requests
// can be out of order with the acquire and release requests.  that
// is, a client may receive a revoke request before it has received
// the positive acknowledgement on its acquire request.  similarly, a
// client may receive a retry before it has received a response on its
// initial acquire request.  a flag field is used to record if a retry
// has been received.
//


class lock_client_cache : public lock_client {
private:
    class lock_release_user *lu;

    int rlock_port;
    std::string hostname;
    std::string id;

    pthread_mutex_t seq_num_m;
    unsigned int seq_num = 0;

    struct Lock {
        enum status {
            NONE = 0,
            FREE = 1,
            LOCKED = 2,
            ACQUIRING = 3,
            RELEASING = 4
        };


        Lock() {
            assert(pthread_mutex_init(&mutex, nullptr) == 0);
            assert(pthread_cond_init(&retry_called, nullptr) == 0);
            assert(pthread_cond_init(&acquire_returned, nullptr) == 0);
            assert(pthread_cond_init(&local_release_called, nullptr) == 0);
            assert(pthread_cond_init(&server_release_called, nullptr) == 0);
            assert(pthread_cond_init(&release_ready_perfectly, nullptr) == 0);
        }

        ~Lock() {
            assert(pthread_mutex_destroy(&mutex) == 0);
            assert(pthread_cond_destroy(&retry_called) == 0);
            assert(pthread_cond_destroy(&acquire_returned) == 0);
            assert(pthread_cond_destroy(&local_release_called) == 0);
            assert(pthread_cond_destroy(&server_release_called) == 0);
            assert(pthread_cond_destroy(&release_ready_perfectly) == 0);
        }

        pthread_mutex_t mutex;
        pthread_cond_t retry_called;
        pthread_cond_t acquire_returned;
        pthread_cond_t local_release_called;
        pthread_cond_t server_release_called;
        pthread_cond_t release_ready_perfectly;

        status stat = NONE;
        bool taken = false;
    };

    pthread_mutex_t locks_m;
    std::unordered_map<lock_protocol::lockid_t, Lock> locks;

    pthread_mutex_t release_reqs_m;
    pthread_cond_t revoke_called;
    std::unordered_set<lock_protocol::lockid_t> release_requests;

public:
    static int last_port;

    lock_client_cache(std::string xdst, class lock_release_user *l = 0);

    ~lock_client_cache();

    lock_client_cache::Lock &get_lock(lock_protocol::lockid_t);

    unsigned int get_next_seq_num();

    lock_protocol::status stat(lock_protocol::lockid_t) override;

    lock_protocol::status acquire(lock_protocol::lockid_t) override;

    lock_protocol::status release(lock_protocol::lockid_t) override;

    void releaser();

    rlock_protocol::status retry(lock_protocol::lockid_t, int &);

    rlock_protocol::status revoke(lock_protocol::lockid_t, int &);
};

#endif

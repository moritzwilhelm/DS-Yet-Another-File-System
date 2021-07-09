// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <unordered_map>
#include <ctime>
#include "extent_protocol.h"
#include <pthread.h>


class extent_server {
private:
    // start with 2 since 1 is reserved for root
    unsigned long next_id = 2;

    struct Extent {
        std::string content;
        extent_protocol::attr metadata{};

        Extent() {
            unsigned int time = std::time(nullptr);
            this->metadata = {time, time, time, 0};
        }
    };

    pthread_mutex_t storage_lock;
    std::unordered_map<extent_protocol::extentid_t, Extent> storage;

public:
    extent_server() {
        assert(pthread_mutex_init(&storage_lock, nullptr) == 0);
        // initialize storage with root directory
        this->storage.emplace(1, Extent());
    };

    ~extent_server() {
        assert(pthread_mutex_destroy(&storage_lock) == 0);
    };

    int put(extent_protocol::extentid_t id, std::string, int &);

    int get(extent_protocol::extentid_t id, std::string &);

    int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);

    int setattr(extent_protocol::extentid_t id, extent_protocol::attr, int &);

    int remove(extent_protocol::extentid_t id, int &);

    int get_next_id(extent_protocol::extentid_t id, unsigned long &);
};

#endif

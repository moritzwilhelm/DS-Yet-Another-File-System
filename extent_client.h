// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "rpc.h"
#include <unordered_map>
#include <utility>
#include <ctime>
#include "pthread.h"

class extent_client {
private:
    rpcc *cl;

    struct Extent {
        // indicates whether Extent was created locally or not
        bool is_new = false;

        bool present = true;
        bool dirty = false;
        std::string content{};
        extent_protocol::attr metadata{};

        Extent() {
            unsigned int time = std::time(nullptr);
            this->metadata = {time, time, time, 0};
        }
    };

    pthread_mutex_t storage_lock;
    std::unordered_map<extent_protocol::extentid_t, Extent> storage;

public:
    extent_client(std::string dst);

    ~extent_client();

    extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);

    extent_protocol::status get(extent_protocol::extentid_t eid, std::string &buf);

    extent_protocol::status getattr(extent_protocol::extentid_t eid, extent_protocol::attr &a);

    extent_protocol::status setattr(extent_protocol::extentid_t eid, extent_protocol::attr a);

    extent_protocol::status remove(extent_protocol::extentid_t eid);

    extent_protocol::status get_next_id(extent_protocol::extentid_t id, unsigned long &);

    extent_protocol::status flush(extent_protocol::extentid_t eid);
};

#endif 

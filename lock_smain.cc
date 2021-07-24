#include "rpc.h"
#include <arpa/inet.h>
#include <stdlib.h>
#include <stdio.h>
#include "lock_server_cache.h"
#include "paxos.h"
#include "rsm.h"

#include "jsl_log.h"

// Main loop of lock_server

int main(int argc, char *argv[]) {
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);

    srandom(getpid());

    if (argc != 3) {
        fprintf(stderr, "Usage: %s [master:]port [me:]port\n", argv[0]);
        if (argc == 2) {
            fprintf(stderr, "Only master port provided. Continuing assuming me == master...\n");
            argv[2] = argv[1];
            argc = 3;
        } else {
            exit(1);
        }
    }

    //jsl_set_debug(2);

    rsm rsm(argv[1], argv[2]);
    lock_server_cache ls(&rsm);
    rsm.reg(lock_protocol::stat, &ls, &lock_server_cache::stat);
    rsm.reg(lock_protocol::acquire, &ls, &lock_server_cache::acquire);
    rsm.reg(lock_protocol::release, &ls, &lock_server_cache::release);

    while (1)
        sleep(1000);
}

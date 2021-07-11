#include "paxos.h"
#include "handle.h"
#include <thread>
#include <chrono>
// #include <signal.h>
#include <stdio.h>

// This module implements the proposer and acceptor of the Paxos
// distributed algorithm as described by Lamport's "Paxos Made
// Simple".  To kick off an instance of Paxos, the caller supplies a
// list of nodes, a proposed value, and invokes the proposer.  If the
// majority of the nodes agree on the proposed value after running
// this instance of Paxos, the acceptor invokes the upcall
// paxos_commit to inform higher layers of the agreed value for this
// instance.


bool
operator>(const prop_t &a, const prop_t &b) {
    return (a.n > b.n || (a.n == b.n && a.m > b.m));
}

bool
operator>=(const prop_t &a, const prop_t &b) {
    return (a.n > b.n || (a.n == b.n && a.m >= b.m));
}

std::string
print_members(const std::vector<std::string> &nodes) {
    std::string s;
    s.clear();
    for (unsigned i = 0; i < nodes.size(); i++) {
        s += nodes[i];
        if (i < (nodes.size() - 1))
            s += ",";
    }
    return s;
}

bool isamember(std::string m, const std::vector<std::string> &nodes) {
    for (unsigned i = 0; i < nodes.size(); i++) {
        if (nodes[i] == m) return 1;
    }
    return 0;
}

bool
proposer::isrunning() {
    bool r;
    assert(pthread_mutex_lock(&pxs_mutex) == 0);
    r = !stable;
    assert(pthread_mutex_unlock(&pxs_mutex) == 0);
    return r;
}

// check if the servers in l2 contains a majority of servers in l1
bool
proposer::majority(const std::vector<std::string> &l1,
                   const std::vector<std::string> &l2) {
    unsigned n = 0;

    for (unsigned i = 0; i < l1.size(); i++) {
        if (isamember(l1[i], l2))
            n++;
    }
    return n >= (l1.size() >> 1) + 1;
}

proposer::proposer(class paxos_change *_cfg, class acceptor *_acceptor,
                   std::string _me)
        : cfg(_cfg), acc(_acceptor), me(_me), break1(false), break2(false),
          stable(true) {
    assert(pthread_mutex_init(&pxs_mutex, NULL) == 0);

}

void
proposer::setn() {
    my_n.n = acc->get_n_h().n + 1 > my_n.n + 1 ? acc->get_n_h().n + 1 : my_n.n + 1;
    my_n.m = me; // append node ID  // unique proposal number
}

bool
proposer::run(int instance, std::vector<std::string> c_nodes, std::string c_v) {
    std::vector<std::string> accepts;
    std::vector<std::string> nodes;
    std::vector<std::string> nodes1;
    std::string v;
    bool r = false;

    pthread_mutex_lock(&pxs_mutex);
    printf("start: initiate paxos for %s w. i=%d v=%s stable=%d\n",
           print_members(c_nodes).c_str(), instance, c_v.c_str(), stable);
    if (!stable) {  // already running proposer?
        printf("proposer::run: already running\n");
        pthread_mutex_unlock(&pxs_mutex);
        return false;
    }
    stable = false;
    setn();
    accepts.clear();
    nodes.clear();
    v.clear();
    nodes = c_nodes;
    if (prepare(instance, accepts, nodes, v)) {
        printf("PREPARE DONE\n");
        if (majority(c_nodes, accepts)) {
            printf("paxos::manager: received a majority of prepare responses\n");

            if (v.size() == 0) {
                v = c_v;
            }

            breakpoint1();

            nodes1 = accepts;
            accepts.clear();
            accept(instance, accepts, nodes1, v);

            if (majority(c_nodes, accepts)) {
                printf("paxos::manager: received a majority of accept responses\n");

                breakpoint2();

                decide(instance, accepts, v);
                r = true;
            } else {
                printf("paxos::manager: no majority of accept responses\n");
            }
        } else {
            printf("paxos::manager: no majority of prepare responses\n");
        }
    } else {
        printf("paxos::manager: prepare is rejected %d\n", stable);
    }

    printf("PROPOSE RUN DONE\n");
    stable = true;
    pthread_mutex_unlock(&pxs_mutex);
    return r;
}

rpcc *make_client(std::string &dst) {
    printf("Make Client %s\n", dst.c_str());
    sockaddr_in dstsock;
    make_sockaddr(dst.c_str(), &dstsock);
    return new rpcc(dstsock);
}

bool bind_client(rpcc *client) {
    if (client->bind(rpcc::to(1000)) < 0) {
        printf("proposer_client: call bind\n");
        return false;
    }
    return true;
}

void delay() {
    srand(time(nullptr));
    std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 1000));
    std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 1000));
}

bool
proposer::prepare(unsigned instance, std::vector<std::string> &accepts,
                  std::vector<std::string> nodes,
                  std::string &v) {
    delay();

    prop_t highest_n_i = {0, ""};

    printf("PREPARE i=%d n=%d\n", instance, my_n.n);

    for (std::string &node: nodes) {
        printf("Sending PREPARE to %s\n", node.c_str());
        // Phase 1
        rpcc *client = make_client(node);
        if (!bind_client(client)) {
            printf("bind fail at %s\n", node.c_str());
            continue;
        }

        paxos_protocol::prepareres res;
        paxos_protocol::preparearg arg{instance, this->my_n, v};
        client->call(paxos_protocol::rpc_numbers::preparereq, this->me, arg, res, rpcc::to(1000));
        delete client;
        printf("After sending PREPARE to %s\n", node.c_str());

        // Phase 2
        if (res.oldinstance) {
            acc->commit(res.n_a.n, res.v_a);
            printf("PREPARE oldinstance\n");
            return false;
        } else if (!res.accept) {
            printf("PREPARE reject\n");
            // reset state
            setn();
            accepts.clear();
            v.clear();

            delay();
            return prepare(instance, accepts, nodes, v);
        } else {
            printf("PREPARE accept\n");
            prop_t n_i = res.n_a;
            std::string v_i = res.v_a;
            printf("highest_n_i: %d %s; v: %s\n", highest_n_i.n, highest_n_i.m.c_str(), v.c_str());
            printf("n_i: %d %s; v_i: %s\n", n_i.n, n_i.m.c_str(), v_i.c_str());
            if (!v_i.empty() && (n_i > highest_n_i)) {
                printf("PREPARE old non-comitted value: %s\n", v_i.c_str());
                v = v_i;
                highest_n_i = n_i;
            } else {
                printf("PREPARE no non-comitted aggreed value\n");
            }
            accepts.push_back(node);
        }

    }
    printf("PREPARE SUCCESS\n");
    return true;
}


void
proposer::accept(unsigned instance, std::vector<std::string> &accepts,
                 std::vector<std::string> nodes, std::string v) {
    printf("ACCEPT %s\n", v.c_str());
    for (std::string &node: nodes) {
        printf("ACCEPT sending to %s\n", node.c_str());
        // Phase 2
        rpcc *client = make_client(node);
        if (!bind_client(client)) {
            printf("bind fail at %s\n", node.c_str());
            continue;
        }

        int r;
        paxos_protocol::acceptarg arg{instance, this->my_n, v};
        client->call(paxos_protocol::rpc_numbers::acceptreq, this->me, arg, r, rpcc::to(1000));
        // value was accepted
        if (r) {
            accepts.push_back(node);
        }
        delete client;
    }
    printf("ACCEPT END\n");
}


void
proposer::decide(unsigned instance, std::vector<std::string> accepts,
                 std::string v) {
    printf("DECIDE\n");
    // acc->commit(instance, v); node commits via deciderequest to itself
    for (std::string &node : accepts) {
        printf("DECIDE sending to %s\n", node.c_str());
        // Phase 3
        rpcc *client = make_client(node);
        if (!bind_client(client)) {
            printf("binf fail at %s\n", node.c_str());
            continue;
        }

        int r;
        paxos_protocol::decidearg arg{instance, v};
        client->call(paxos_protocol::rpc_numbers::decidereq, this->me, arg, r, rpcc::to(1000));
        delete client;
    }
    printf("DECIDE END\n");
}

acceptor::acceptor(class paxos_change *_cfg, bool _first, std::string _me,
                   std::string _value)
        : cfg(_cfg), me(_me), instance_h(0) {
    assert(pthread_mutex_init(&pxs_mutex, NULL) == 0);

    n_h.n = 0;
    n_h.m = me;
    n_a.n = 0;
    n_a.m = me;
    v_a.clear();

    l = new log(this, me);

    if (instance_h == 0 && _first) {
        values[1] = _value;
        l->loginstance(1, _value);
        instance_h = 1;
    }

    pxs = new rpcs(atoi(_me.c_str()));
    pxs->reg(paxos_protocol::preparereq, this, &acceptor::preparereq);
    pxs->reg(paxos_protocol::acceptreq, this, &acceptor::acceptreq);
    pxs->reg(paxos_protocol::decidereq, this, &acceptor::decidereq);
}

paxos_protocol::status
acceptor::preparereq(std::string src, paxos_protocol::preparearg a,
                     paxos_protocol::prepareres &r) {
    printf("HANDLE PREPAREREQ src: %s\n", src.c_str());
    printf("received n: %d %s, n_h : %d %s\n", a.n.n, a.n.m.c_str(), n_a.n, n_a.m.c_str());
    // handle a preparereq message from proposer

    r.oldinstance = false;
    r.accept = false;

    if (a.instance <= instance_h) {
        // old_instance case
        r.oldinstance = true;
        r.n_a.n = a.instance;
        r.n_a.m = src;
        r.v_a = value(a.instance);
    } else if (a.n > n_h) {
        printf("accept: old v_a %s\n", v_a.c_str());
        // accept proposal
        n_h = a.n;
        l->loghigh(n_h);
        r.n_a = n_a;
        r.v_a = v_a;
        printf("n_a: %d %s; v_a: %s\n", n_a.n, n_a.m.c_str(), v_a.c_str());
        r.accept = true;
    } else {
        // reject proposal
        r.accept = false;
    }
    return paxos_protocol::OK;

}

paxos_protocol::status
acceptor::acceptreq(std::string src, paxos_protocol::acceptarg a, int &r) {
    printf("HANDLE ACCEPTREQ src: %s\n", src.c_str());
    printf("received n: %d %s, n_h : %d %s\n", a.n.n, a.n.m.c_str(), n_h.n, n_h.m.c_str());
    // handle an acceptreq message from proposer

    if (a.instance <= instance_h) {
        // old_instance case
        r = false;
    } else if (a.n >= n_h) {
        // accept
        printf("accept: new v_a %s\n", a.v.c_str());
        n_a = a.n;
        v_a = a.v;
        l->logprop(a.n, a.v);
        r = true;
    } else {
        // reject
        r = false;
    }

    return paxos_protocol::OK;
}

paxos_protocol::status
acceptor::decidereq(std::string src, paxos_protocol::decidearg a, int &r) {
    printf("HANDLE DECIDEREQ src: %s\n", src.c_str());
    printf("reveiced value %s; instance: %d > instance_h %d\n", a.v.c_str(), a.instance, instance_h);
    // handle an decide message from proposer

    if (a.instance > instance_h) {
        commit(a.instance, a.v);
    }

    return paxos_protocol::OK;
}

void
acceptor::commit_wo(unsigned instance, std::string value) {
    //assume pxs_mutex is held
    printf("acceptor::commit: instance=%d has v= %s\n", instance, value.c_str());
    if (instance > instance_h) {
        printf("commit: highestaccepteinstance = %d\n", instance);
        values[instance] = value;
        l->loginstance(instance, value);
        instance_h = instance;
        n_h.n = 0;
        n_h.m = me;
        n_a.n = 0;
        n_a.m = me;
        v_a.clear();
        if (cfg) {
            pthread_mutex_unlock(&pxs_mutex);
            cfg->paxos_commit(instance, value);
            pthread_mutex_lock(&pxs_mutex);
        }
    }
}

void
acceptor::commit(unsigned instance, std::string value) {
    pthread_mutex_lock(&pxs_mutex);
    commit_wo(instance, value);
    pthread_mutex_unlock(&pxs_mutex);
}

std::string
acceptor::dump() {
    return l->dump();
}

void
acceptor::restore(std::string s) {
    l->restore(s);
    l->logread();
}



// For testing purposes

// Call this from your code between phases prepare and accept of proposer
void
proposer::breakpoint1() {
    if (break1) {
        printf("Dying at breakpoint 1!\n");
        exit(1);
    }
}

// Call this from your code between phases accept and decide of proposer
void
proposer::breakpoint2() {
    if (break2) {
        printf("Dying at breakpoint 2!\n");
        exit(1);
    }
}

void
proposer::breakpoint(int b) {
    if (b == 3) {
        printf("Proposer: breakpoint 1\n");
        break1 = true;
    } else if (b == 4) {
        printf("Proposer: breakpoint 2\n");
        break2 = true;
    }
}

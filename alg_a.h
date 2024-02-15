#pragma once
#include "util.h"
#include <atomic>
#include <mutex>
using namespace std;

class AlgorithmA {
public:
    static constexpr int TOMBSTONE = -1;
    static constexpr int NULL_VALUE = -2;

    char padding0[PADDING_BYTES];
    const int numThreads;
    int capacity;
    char padding2[PADDING_BYTES];

    struct padded_bucket {
        mutex m;
        int key;
        char padding[64 - (sizeof(int) + sizeof(mutex))];
    };
    
    padded_bucket * data;

    AlgorithmA(const int _numThreads, const int _capacity);
    ~AlgorithmA();
    bool insertIfAbsent(const int tid, const int & key);
    bool erase(const int tid, const int & key);
    long getSumOfKeys();
    void printDebuggingDetails(); 
};


/**
 * constructor: initialize the hash table's internals
 * 
 * @param _numThreads maximum number of threads that will ever use the hash table (i.e., at least tid+1, where tid is the largest thread ID passed to any function of this class)
 * @param _capacity is the INITIAL size of the hash table (maximum number of elements it can contain WITHOUT expansion)
 */
AlgorithmA::AlgorithmA(const int _numThreads, const int _capacity)
: numThreads(_numThreads), capacity(_capacity) {
    data = new padded_bucket[capacity];
    for (int i = 0; i < capacity; i++)
        data[i].key = NULL_VALUE;
}

// destructor: clean up any allocated memory, etc.
AlgorithmA::~AlgorithmA() {
    delete[] data;
}

// semantics: try to insert key. return true if successful (if key doesn't already exist), and false otherwise
bool AlgorithmA::insertIfAbsent(const int tid, const int & key) {
    u_int32_t h = murmur3(key);
    for(int i = 0; i < capacity; i++) {
        int index = (h + i) % capacity;
        //cout << index << endl;
        data[index].m.lock();
        int found = data[index].key;
        if(found == key) {
            data[index].m.unlock();
            return false;
        } else if (found == NULL_VALUE) {
            data[index].key = key;
            data[index].m.unlock();
            return true;
        }
        data[index].m.unlock();
    }
    return false;
}

// semantics: try to erase key. return true if successful, and false otherwise
bool AlgorithmA::erase(const int tid, const int & key) {
    u_int32_t h = murmur3(key);
    for(int i = 0; i < capacity; i++) {
        int index = (h + i) % capacity;
        data[index].m.lock();
        int found = data[index].key;
        if(found == NULL_VALUE) {
            data[index].m.unlock();
            return false;
        } else if(found == key) {
            data[index].key = TOMBSTONE;
            data[index].m.unlock();
            return true;
        }
        data[index].m.unlock();
    }
    return false;
}

// semantics: return the sum of all KEYS in the set
int64_t AlgorithmA::getSumOfKeys() {
    int64_t sum = 0;
    for(int i = 0; i < capacity; i++) {
        int key = data[i].key;
        if(key != NULL_VALUE && key != TOMBSTONE)
            sum += key;
    }
    return sum;
}

// print any debugging details you want at the end of a trial in this function
void AlgorithmA::printDebuggingDetails() {
    
}

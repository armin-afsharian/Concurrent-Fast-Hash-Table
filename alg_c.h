#pragma once
#include "util.h"
#include <atomic>
using namespace std;

class AlgorithmC {
public:
    static constexpr int TOMBSTONE = -1;
    static constexpr int NULL_VALUE = -2;

    char padding0[PADDING_BYTES];
    const int numThreads;
    int capacity;
    char padding2[PADDING_BYTES];

    struct padded_bucket {
        atomic<int> key;
        char padding[64 - (sizeof(atomic<int>))];
    };

    padded_bucket * data;

    AlgorithmC(const int _numThreads, const int _capacity);
    ~AlgorithmC();
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
AlgorithmC::AlgorithmC(const int _numThreads, const int _capacity)
: numThreads(_numThreads), capacity(_capacity) {
    data = new padded_bucket[capacity];
    for(int i = 0; i < capacity; i++)
        data[i].key = NULL_VALUE;
}

// destructor: clean up any allocated memory, etc.
AlgorithmC::~AlgorithmC() {
    delete[] data;
}

// semantics: try to insert key. return true if successful (if key doesn't already exist), and false otherwise
bool AlgorithmC::insertIfAbsent(const int tid, const int & key) {
    u_int32_t h = murmur3(key);
    for(int i = 0; i < capacity; i++) {
        int index = (h + i) % capacity;
        int found = data[index].key;
        if(found == key) {
            return false;
        } else if(found == NULL_VALUE) {
            int expected = NULL_VALUE;
            if(data[index].key.compare_exchange_strong(expected, key)) {
                return true;
            } if(data[index].key == key) {
                return false;
            }
        }
    }
    return false;
}

// semantics: try to erase key. return true if successful, and false otherwise
bool AlgorithmC::erase(const int tid, const int & key) {
    u_int32_t h = murmur3(key);
    for(int i = 0; i < capacity; i++) {
        int index = (h + i) % capacity;
        int found = data[index].key;
        if(found == NULL_VALUE) {
            return false;
        } else if(found == key) {
            int expected = key;
            return data[index].key.compare_exchange_strong(expected, TOMBSTONE);
        }
    }
    return false;
}

// semantics: return the sum of all KEYS in the set
int64_t AlgorithmC::getSumOfKeys() {
    int64_t sum = 0;
    for(int i = 0; i < capacity; i++) {
        int key = data[i].key;
        if(key != NULL_VALUE && key != TOMBSTONE)
            sum += key;
    }
    return sum;
}

// print any debugging details you want at the end of a trial in this function
void AlgorithmC::printDebuggingDetails() {
    
}
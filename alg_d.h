#pragma once
#include "util.h"
#include <atomic>
#include <cmath>
using namespace std;

class AlgorithmD {
private:
    enum {
        MARKED_MASK = (int) 0x80000000,     // most significant bit of a 32-bit key
        TOMBSTONE = (int) 0x7FFFFFFF,       // largest value that doesn't use bit MARKED_MASK
        EMPTY = (int) 0,
        MAXIMUM_HASH = (uint32_t)0xFFFFFFFF
    }; // with these definitions, the largest "real" key we allow in the table is 0x7FFFFFFE, and the smallest is 1 !!

    static constexpr int CHUNK_SIZE = 4096;
    static constexpr int CAPCITY_INCREASE = 4;

    struct table {
        char padding0[64];
        atomic<int> * data;
        atomic<int> * old;
        int capacity;
        int oldCapacity;
        int numThreads;
        counter * approxCounter;
        counter * deleteCounter;
        atomic<int> chuncksClaimed;
        atomic<int> chuncksDone;
        table(const int _capacity, const int _numThreads) 
        : capacity(_capacity), numThreads(_numThreads), old(NULL), oldCapacity(0), chuncksClaimed(0), chuncksDone(0) {
            data = new atomic<int>[capacity];
            for(int i = 0; i < capacity; i++)
                data[i].store(EMPTY, memory_order_relaxed);
            approxCounter = new counter(_numThreads);
            deleteCounter = new counter(_numThreads);
        }

        table(table * t) {
            old = t->data;
            oldCapacity = t->capacity;
            int insertCount = t->approxCounter->get();
            int deleteCount = t->deleteCounter->get();
            int numOfKeys = insertCount - deleteCount; // number of keys in the table;
            if (numOfKeys > 0)
                capacity = numOfKeys * CAPCITY_INCREASE;
            else
                capacity = oldCapacity * CAPCITY_INCREASE;
                
            numThreads = t->numThreads;
            approxCounter = new counter(numThreads);
            deleteCounter = new counter(numThreads);
            chuncksClaimed.store(0, memory_order_relaxed);
            chuncksDone.store(0, memory_order_relaxed);
            data = new atomic<int>[capacity];
            for(int i = 0; i < capacity; i++)
                data[i].store(EMPTY, memory_order_relaxed);
        }

        void print(int k) {
            for (int i = 0; i < k; i++) {
                int temp = data[i];
                if (temp == TOMBSTONE)
                    cout << "O";
                else if (temp == EMPTY)
                    cout << ".";
                else
                    cout << 'X';
            }
            cout << "END\n *** \n *** \n";
        }

        ~table() {
            if(data)
                delete[] data;
            delete approxCounter, deleteCounter;
        }

    };
    
    bool expandAsNeeded(const int tid, table * t, int i);
    void helpExpansion(const int tid, table * t);
    void startExpansion(const int tid, table * t);
    void migrate(const int tid, table * t, int myChunk);
    
    char padding0[PADDING_BYTES];
    int numThreads;
    int initCapacity;
    char padding1[PADDING_BYTES];
    atomic<table *> currentTable;
    char padding2[PADDING_BYTES];
    
public:
    AlgorithmD(const int _numThreads, const int _capacity);
    ~AlgorithmD();
    bool insertIfAbsent(const int tid, const int & key, bool disableExpansion);
    bool erase(const int tid, const int & key);
    bool insertForMigration(const int tid, const int & key);
    long getSumOfKeys();
    void printDebuggingDetails(); 
};

/**
 * constructor: initialize the hash table's internals
 * 
 * @param _numThreads maximum number of threads that will ever use the hash table (i.e., at least tid+1, where tid is the largest thread ID passed to any function of this class)
 * @param _capacity is the INITIAL size of the hash table (maximum number of elements it can contain WITHOUT expansion)
 */
AlgorithmD::AlgorithmD(const int _numThreads, const int _capacity)
: numThreads(_numThreads), initCapacity(_capacity) {
    currentTable = new table(_capacity, _numThreads); 
}

// destructor: clean up any allocated memory, etc.
AlgorithmD::~AlgorithmD() {
    table * t = currentTable;
    if(t) {
        if(t->old)
            delete[] currentTable.load()->old;
        if(t->approxCounter)
            delete currentTable.load()->approxCounter;
        if(t->deleteCounter)
            delete currentTable.load()->deleteCounter;
        delete t;
    }
}

bool AlgorithmD::expandAsNeeded(const int tid, table * t, int i) {
    helpExpansion(tid, t);
    if(((t->approxCounter->get()) > (0.5 * t->capacity)) ||
        ((i > 100) && ((t->approxCounter->getAccurate()) > t->capacity/2))) {
            startExpansion(tid, t);
            return true;
    }
    return false;
}

void AlgorithmD::helpExpansion(const int tid, table * t) {
    int totalOldChunks = ceil(t->oldCapacity / (double) CHUNK_SIZE);
    while(t->chuncksClaimed < totalOldChunks) {
        int myChunk = t->chuncksClaimed.fetch_add(1);
        if(myChunk < totalOldChunks) {
            migrate(tid, t, myChunk);
            t->chuncksDone.fetch_add(1);
        }
    }
    while(t->chuncksDone < totalOldChunks);
}

void AlgorithmD::startExpansion(const int tid, table * t) {
    if(currentTable == t) {
        table * t_new = new table(t);
        if(!currentTable.compare_exchange_strong(t, t_new))
            delete t_new;
        else {
            if(t->old)
                delete[] t->old;
        }
    }
    helpExpansion(tid, currentTable);
}

void AlgorithmD::migrate(const int tid, table * t, int myChunk) {
    int start_index = myChunk * CHUNK_SIZE;
    int end_index = min((myChunk + 1) * CHUNK_SIZE, t->oldCapacity);
    for(int i = start_index; i < end_index; i++) {
        int key = t->old[i];
        if(key == TOMBSTONE)
            continue;
        else {
            while(!t->old[i].compare_exchange_strong(key, key | MARKED_MASK)) {
                key = t->old[i];
            }
            if((key != TOMBSTONE) && (key != EMPTY)) {
                insertForMigration(tid, key);
            }
                
        }
    }
}

bool AlgorithmD::insertForMigration(const int tid, const int & key) {
    table * t = currentTable;
    double ii = murmur3(key);
    uint32_t h = floor(ii / MAXIMUM_HASH * (uint32_t)t->capacity);
    for(int i = 0; i < t->capacity; i++) {
        int index = (h + i) % t->capacity;
        int found = t->data[index].load(memory_order_relaxed);
        if(found == key)
            return false;
        else if(found == EMPTY) {
            int expected = EMPTY;
            if(t->data[index].compare_exchange_strong(expected, key)) {
                t->approxCounter->inc(tid);
                return true;
            }
            found = t->data[index];
            if(found == key)
                return false;
        }
    }
        
    return false;
}

// semantics: try to insert key. return true if successful (if key doesn't already exist), and false otherwise
bool AlgorithmD::insertIfAbsent(const int tid, const int & key, bool disableExpansion = false) {
    table * t = currentTable;
    double ii = murmur3(key);
    uint32_t h = floor(ii / MAXIMUM_HASH * (uint32_t)t->capacity);
    for(int i = 0; i < t->capacity; i++) {
        if(!disableExpansion && expandAsNeeded(tid, t, i))
            return insertIfAbsent(tid, key);
        int index = (h + i) % t->capacity;
        int found = t->data[index];
        if(found & MARKED_MASK)
            return insertIfAbsent(tid, key);
        else if(found == key)
            return false;
        else if(found == EMPTY) {
            int expected = EMPTY;
            if(t->data[index].compare_exchange_strong(expected, key)) {
                t->approxCounter->inc(tid);
                return true;
            }
            int found = t->data[index];
            if(found & MARKED_MASK)
                return insertIfAbsent(tid, key);
            else if(found == key)
                return false;
        }
        
    }
    return false;
}

// semantics: try to erase key. return true if successful, and false otherwise
bool AlgorithmD::erase(const int tid, const int & key) {
    table * t = currentTable;
    double ii = murmur3(key);
    uint32_t h = floor(ii / MAXIMUM_HASH * (uint32_t)t->capacity);
    for(int i = 0; i < t->capacity; i++) {
        helpExpansion(tid, t);
        int index = (h + i) % t->capacity;
        int found = t->data[index];
        if(found & MARKED_MASK)
            return erase(tid, key);
        else if(found == EMPTY)
            return false;
        else if(found == key) {
            int expected = key;
            if(t->data[index].compare_exchange_strong(expected, TOMBSTONE)) {
                t->deleteCounter->inc(tid);
                return true;
            }
            int found = t->data[index];
            if(found & MARKED_MASK)
                return erase(tid, key);
            else if(found == TOMBSTONE);
                return false;
        }
    }
    return false;
}

// semantics: return the sum of all KEYS in the set
int64_t AlgorithmD::getSumOfKeys() {
    int64_t sum = 0;
    table * table = currentTable;
    for(int i = 0; i < table->capacity; i++) {
        int key = table->data[i];
        if(key != EMPTY && key != TOMBSTONE)
            sum += key;
    }
    return sum;
}

// print any debugging details you want at the end of a trial in this function
void AlgorithmD::printDebuggingDetails() {
}
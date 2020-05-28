//
// Created by Ido on 5/24/2020.
//

#ifndef PROJECT3_JOBCONTEXT_H
#define PROJECT3_JOBCONTEXT_H

#include <vector>
#include <iostream>
#include "MapReduceFramework.h"
#include "Barrier.h"
#include <atomic>

using namespace std;


class JobContext {
private:

    atomic <int> vecAssign ; //(gives vec to each thread)
    atomic <int> nextPair; //amount of pairs we handed out to threads
    atomic <int> doneMaps; // amount of pairs from input mapped
    atomic <int> k2v2s; // amount of IM pairs created
    atomic <int> shuffled; // amount of IM pairs we shuffled
    atomic <int> doneThreads; //amount of map threads finish
    atomic <int> k2Count; //number of different keys
    atomic <int> reducedk2v2s; //amount of k3v3 pairs we reduced

    JobState state; //state of job
    const MapReduceClient& client;
    int tnum; //num of threads

    vector<IntermediatePair>** vecs;
    vector<K2*>* k2vec;
    IntermediateMap midMap;

    pthread_t* threads;
    pthread_mutex_t* muts;
    pthread_mutex_t outputMut;
    pthread_mutex_t waitMut;
    pthread_cond_t waitcv;
    Barrier barr;


    InputVec input;
    int inPairs; //size of input vec
    OutputVec& output ;

public:
    JobContext(int n, const InputVec& _input, OutputVec& _output ,const MapReduceClient& _client); //ctor
    ~JobContext(); //d'tor
    void init_threads(JobHandle myJob); //init map threads and shuffle thread
    pthread_t* get_threads();
    pthread_mutex_t* get_muts();

    int getMyVec(); //increase atomic var
    int getMyPair(); // increase atomic var
    int getDoneThreads(); // increase atomic var
    atomic <int>* getK2V2s();

    int getTnum(); //return tnum
    void get_state(JobState* _state); //put state in _state struct
    int getInPairs(); //return inpairs

    vector<IntermediatePair>** get_vecs();
    InputVec getInputVec();
    const MapReduceClient& get_client();

    void setMapState();
    void mapUpdate(int x); //update percentage of map phase
    void pushToMap(vector <IntermediatePair>* vecToMap); //push k2v2 pairs to midMap
    void endMapStage(); //change stage to SHUFFLE and percentage to 0
    void shuffleUpdate(); //update percentage of shuffle phase
    void activateBarr(); //activate barrier before reduce phase
    void reducer(); //calls the reduce func
    void reduceUpdate(); //update percentage of reduce phase

    void waiter(); //wait until job is done
    void imDone();
};

#endif //PROJECT3_JOBCONTEXT_H



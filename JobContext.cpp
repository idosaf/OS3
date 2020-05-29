//
// Created by Ido on 5/20/2020.
//

#include "JobContext.h"
#include <vector>
using namespace std;

struct reduceContext{  //struct to send to emit2 via map
    OutputVec& output;
    pthread_mutex_t* outputMut;
};
struct mapContext{  //struct to send to emit3 via reduce
    pthread_mutex_t* myMut;
    vector<IntermediatePair>* myVec;
    int* outputs;
};



JobContext::JobContext(int n, const InputVec& _input, OutputVec& _output,const MapReduceClient& _client):
        client(_client),  tnum(n),  k2vec(new vector<K2*>), barr(Barrier(n)), output(_output){

//printf("ctor\n");
    vecAssign=0; //used to assign vectors to threads
    nextPair=0; //amount of pairs we handed out to threads
    doneMaps=0;// amount of pairs from input mapped
    k2v2s=0; // amount of IM pairs created
    shuffled=0; // amount of IM pairs we shuffled
    doneThreads = (n-1); //amount of map threads finish
    k2Count = 0; //number of different keys
    reducedk2v2s = 0; //amount of k3v3 pairs we reduced

    state={UNDEFINED_STAGE,0}; //init stage of job

    input=_input; //save input vector

    vecs=new vector<IntermediatePair>* [tnum]; // an array of vectors to keep the outputs of emit2
    if (!vecs){
        fprintf(stderr,"alloc error\n");
        exit(1);
    }
    vector<IntermediatePair>* temp; //used for allocation
    for (int i=0; i<tnum-1; i++){ //init vectors for map stage threads
        temp =new vector<IntermediatePair>; //create a vector for each thread to emit k2-v2 pairs to
        if (!temp){
            fprintf(stderr,"alloc error\n");
            exit(1);
        }
        vecs[i]=temp;
    }

    threads=new pthread_t[tnum]; //keep thread id's
    if (!threads){
        fprintf(stderr,"alloc error\n");
        exit(1);
    }

    outputMut = PTHREAD_MUTEX_INITIALIZER; //mutex for accessing output vector
    muts=new pthread_mutex_t[tnum];  //mutexes used for emit2 output vectors
    if (!muts){
        fprintf(stderr,"alloc error\n");
        exit(1);
    }
    for(int i=0; i<tnum; i++){ //init mutex array for threads in map stage
        muts[i]=PTHREAD_MUTEX_INITIALIZER;
    }
    inPairs=_input.size(); //amount of k1v1 pairs to proccess

}
JobContext::~JobContext(){
//printf("dtor\n");
    //--------------------------------------- destroy mutexes


    delete[] muts;


    if (pthread_mutex_destroy(&outputMut) != 0) {
        fprintf(stderr, "system error: mutex error\n");
        exit(1);
    }


    //------------------------------------- //destroy vector array

    for (int i=0; i<tnum-1; i++){
        while(!(vecs[i]->empty())){
            vecs[i]->pop_back();
        }
        delete vecs[i];
    }
    delete [] vecs;
    //-------------------------------------  //other stuff
    while (!k2vec->empty()){
        k2vec->pop_back();
    }
    delete k2vec;

    delete[] threads;
    midMap.clear();
}


void JobContext::get_state(JobState* _state){
   // printf("getstateeeeeeeeeeeeee\n");
    if(pthread_mutex_lock(&muts[tnum-1])!=0) { //lock state mutex
        fprintf(stderr, "system error: mutex error\n");
        exit(1);
    }
    (*(_state)).stage=state.stage; //get values
    _state->percentage=state.percentage;

    if(pthread_mutex_unlock(&muts[tnum-1])!=0){ //unlock state mutex
        fprintf(stderr, "system error: mutex error\n");
        exit(1);
    }
}

pthread_t* JobContext::get_threads(){
 //   printf("getthreads\n");
    return threads;
}
pthread_mutex_t* JobContext::get_muts(){
   // printf("getmuts\n");
    return muts;
}

void * shuffle(void* inCon){

    //   printf("shuffle\n");
    JobContext * context =(JobContext*) inCon;
    int i = 0;
    vector<IntermediatePair> vecToMap; //we empty vectors in to this vectors and then empty them into the map
    int flag = 0; //marks if we put something in the above vector
    JobState state;
    context->get_state(&state);

    while (state.stage!=REDUCE_STAGE && state.stage!=UNDEFINED_STAGE ) { //until we finished shuffling

        if(pthread_mutex_lock(&(context->get_muts()[i]))!=0){ //lock mutex for vector of mapping thread "i"
            fprintf(stderr, "system error: mutex error\n");
            exit(1);
        }

        while(!context->get_vecs()[i]->empty()){ //empty the entire vector
            flag = 1; //marks that we have at least one object to shuffle
            vecToMap.push_back(context->get_vecs()[i]->back()); //push to vector for later mapping
            ( context->get_vecs() )[i]->pop_back(); //remove shuffled pair
        }

        if(pthread_mutex_unlock(&(context->get_muts()[i]))!=0){ //unlock mutex for vector of mapping thread "i"
            fprintf(stderr, "system error: mutex error\n");
            exit(1);
        }

        if (flag) { //if we have something to shuffle
            flag = 0;
            context->pushToMap(&vecToMap);
        }

        i++; //move to next thread
        i=i%(context->getTnum()-1); //return to first vector if we need to circle around
        context->get_state(&state);

    }
    context->activateBarr(); //activate barrier and to release mapp threads into shuffle phase
    context->reducer();
    return 0;
}

void * mapper(void* inCon){ //used to manage map phase
 //   printf("in mapper \n");
    JobContext * context =(JobContext*) inCon; //cast context to desired type
    int vecNum=context->getMyVec(); //use atomic counter to get assigned vector number
    vector<IntermediatePair>* myVec=context->get_vecs()[vecNum]; //get assigned vector
    pthread_mutex_t* myMut=&context->get_muts()[vecNum]; //get assigned mutex
    int pairNum=context->getMyPair(); //using atomic variable to assign pairs to map
    int x=0;
    int* my_outputs=&x;
    mapContext mycon = {myMut,myVec, my_outputs};
    while (pairNum<context->getInPairs()) //while we didn't reach the end of the vector
    {
        *(my_outputs)=0;
        //InputPair myPair = context->getInputVec()[pairNum]; //get pair from input vector
        context->get_client().map(context->getInputVec()[pairNum].first, context->getInputVec()[pairNum].second, &mycon); //map the pair
        context->mapUpdate(*(my_outputs)); //update state and percentage
        pairNum = context->getMyPair(); //get next pair number
    }
    if (context->getDoneThreads()) { //this function uses an atomic variable to notify if all other threads finished mapping
        context->endMapStage(); //updates "state" accordingly
    }
    context->activateBarr(); //barrier to wait till al threads finish mapping



    context->reducer(); //reduce stage

    return 0;
}

vector<IntermediatePair >** JobContext::get_vecs(){ //return pointer to array of vectors used for mapped keys
   // printf("getvecs\n");
    return vecs;
}
int JobContext::getMyVec(){ //assigns vectors to mapping threads
  //  printf("getmymuts\n");
    return ((vecAssign)++);
}
int JobContext::getMyPair(){ //assigns pairs (k1-v1 or k2-v2vector) to threads
  //  printf("getmypair\n");
    return (nextPair)++;
}
int JobContext::getDoneThreads(){ //used to know when map finished
  //  printf("getDonethre\n");
    int num=( (--doneThreads) ==0);
    return num;
}
const MapReduceClient& JobContext::get_client(){ //return reference to client
 //   printf("getclient\n");

    return client;
}
InputVec JobContext::getInputVec(){ //returns input vector
 //   printf("getinputvec\n");
    return input;
}

int JobContext::getInPairs(){ //returns amount of pairs in input vector
  //  printf("getinpairs\n");
    return inPairs;
}

int JobContext::getTnum(){ //returns amount of threads
  //  printf("gettnum\n");
    return tnum;
}

void JobContext::setMapState() //sets state to MAP_STAGE
{
  //  printf("setMapState");
    state.stage = MAP_STAGE;
}
void JobContext::endMapStage() {
  //  printf("endmapstage\n");
    if(pthread_mutex_lock(&muts[tnum-1])!=0){ //lock state mutex
        fprintf(stderr, "system error: mutex error\n");
        exit(1);

    }

    state.stage = SHUFFLE_STAGE;
    state.percentage=(((double) (shuffled) )/( (double)(k2v2s) ) )*100; //change to progress of shuffle stage
    if(state.percentage==100){ //if we already finished shuffling;
        state.percentage=0;
        state.stage=REDUCE_STAGE; //change to reduce stage
        nextPair=0;
        (doneThreads)=tnum;
    }
    if(pthread_mutex_unlock(&muts[tnum-1])!=0){ //release state mutex
        fprintf(stderr, "system error: mutex error\n");
        exit(1);
    }

}

void JobContext::mapUpdate(int x) { //update percentage of map phase
   // printf("mapUpdate\n");
    if(pthread_mutex_lock(&muts[tnum-1])!=0){ //lock state mutex
        fprintf(stderr, "system error: mutex error\n");
        exit(1);

    }
    int done=(++doneMaps); //counts mapped k1v1 pairs
    k2v2s+=x; //counts k2v2s created
    state.percentage= (  ( (double)(done) ) / ( (double)(inPairs))  ) *100;  // calculate progress

    if(pthread_mutex_unlock(&muts[tnum-1])!=0){ //release state mutex
        fprintf(stderr, "system error: mutex error\n");
        exit(1);
    }
    return;
}

void JobContext::shuffleUpdate(){
  //  printf("shuffleupdate\n");


    if(pthread_mutex_lock(&muts[tnum-1])!=0){ //lock state mutex
        fprintf(stderr, "system error: mutex error\n");
        exit(1);
    }
    (shuffled)++; //counts shuffled pairs
    if(state.stage==SHUFFLE_STAGE){
        state.percentage=(((double)( (shuffled))) / ((double)((k2v2s) )))*100; //updates progress
        if(state.percentage==100){ //if we finished shuffling
            state.stage = REDUCE_STAGE; //move to reduce stage
            state.percentage = 0;
            nextPair=0; //reset for pair assignment

        }
    }

    if(pthread_mutex_unlock(&muts[tnum-1])!=0){ //release state mutex
        fprintf(stderr, "system error: mutex error\n");
        exit(1);
    }
}

atomic <int>* JobContext::getK2V2s(){ //returns a reference to k2v2 counter
  //  printf("getK2v2s\n");
    return &k2v2s;
}

void JobContext::pushToMap(vector <IntermediatePair>* vecToMap){
   // printf("pushtomap\n");
    while (!vecToMap->empty()) {
        IntermediatePair toMap = vecToMap->back();
        vecToMap->pop_back();
        if (midMap.count(toMap.first) == 0) {
            (k2Count)++;
            k2vec->push_back(toMap.first);
            vector<V2 *> valVector;
            midMap.insert(pair<K2 *, vector<V2 *> >(toMap.first, valVector));
        }
        midMap[toMap.first].push_back(toMap.second);
        shuffleUpdate();
    }
};

void JobContext::activateBarr(){
   // printf("barrier\n");
    barr.barrier();
}

void JobContext::reducer(){
 //   printf("reducer\n");
    int keyNum=getMyPair();

    reduceContext myCon={output, &outputMut};

    while(keyNum<int(k2vec->size())) {
        K2 *myKey = ( *(k2vec) )[keyNum];
        vector<V2 *> myVec = midMap[myKey];
        client.reduce(myKey, myVec, &myCon);
        reduceUpdate();
        keyNum=getMyPair();
    }

   // printf("done reducing, time to leave.....");


}

void JobContext::reduceUpdate(){
//printf("reduceupdate\n");
    if(pthread_mutex_lock(&muts[tnum-1])!=0){
        fprintf(stderr, "system error: mutex error\n");
        exit(1);
    }

    state.percentage = (((double)(++reducedk2v2s)) / ((double)(k2Count))) * 100;
    if (state.percentage==100){
     //   printf("end reduce \n");

    }


    if(pthread_mutex_unlock(&muts[tnum-1])!=0){
        fprintf(stderr, "system error: mutex error\n");
        exit(1);
    }
}

void JobContext::waiter() {
  //  printf("waiter\n");
    for (int i = 0; i < tnum; i++) {
        if (pthread_join(threads[i], NULL) != 0) {
            fprintf(stderr, "system error: thread release error\n");
            exit(1);
        }
    }
}

void JobContext::init_threads(JobHandle myJob) {
//    printf("initthreads\n");



    for (int i = 0; i < tnum-1; ++i) {
        if(pthread_create(threads + i, nullptr, mapper, myJob)!=0){
            fprintf(stderr, "system error: thread error\n");
            exit(1);
        }
    }
    if(pthread_create(threads + tnum-1, nullptr, shuffle, myJob)!=0){
        fprintf(stderr, "system error: thread error\n");
        exit(1);
    }
}









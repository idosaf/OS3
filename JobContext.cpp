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

    //*(vecAssign)=0; //todo check if needs alloc (gives vec to each thread)
    //*(vecAssign)=atomic<int>(0);
    vecAssign=0;
    nextPair=0; //amount of pairs we handed out to threads
    doneMaps=0;// amount of pairs from input mapped
    k2v2s=0; // amount of IM pairs created
    shuffled=0; // amount of IM pairs we shuffled
    doneThreads = (n-1); //amount of map threads finish
    k2Count = 0; //number of different keys
    reducedk2v2s = 0; //amount of k3v3 pairs we reduced

    state={UNDEFINED_STAGE,0}; //init stage of job

    input=_input;

    vecs=new vector<IntermediatePair>* [tnum]; // an array of vectors to keep the outputs of emit2
    if (!vecs){
        fprintf(stderr,"alloc error");
        exit(1);
    }
    vector<IntermediatePair>* temp; //used for allocation
    for (int i=0; i<tnum-1; i++){ //init vectors for map stage threads
        temp =new vector<IntermediatePair>;
        if (!temp){
            fprintf(stderr,"alloc error");
            exit(1);
        }
        vecs[i]=temp;
    }

    threads=new pthread_t[tnum]; //keep thread id's
    if (!threads){
        fprintf(stderr,"alloc error");
        exit(1);
    }

    outputMut = PTHREAD_MUTEX_INITIALIZER; //mutex for accessing output vector
    waitMut = PTHREAD_MUTEX_INITIALIZER; // mutex used in "wait for job"
    waitcv = PTHREAD_COND_INITIALIZER; // cv for "wait for job"
    muts=new pthread_mutex_t[tnum];  //mutexes used for emit2 output vectors
    if (!muts){
        fprintf(stderr,"alloc error");
        exit(1);
    }
    for(int i=0; i<tnum; i++){ //init mutex array for threads in map stage
        muts[i]=PTHREAD_MUTEX_INITIALIZER;
    }
    inPairs=_input.size(); //amount of k1v1 pairs to proccess

}
JobContext::~JobContext(){ //todo cry

    //---------------------------------- destroy threads
    for(int i=0; i<tnum; i++){
        if (pthread_detach(threads[i])!=0){
            fprintf(stderr, "thread release error");
            exit(1);
        }
    }
    delete[] threads;
    printf("deleted threads \n");
    // --------------------------------- destroy mutexes and cv's

    delete[] muts; //todo make sure deleting is good

    /*
    for (int i=0; i<tnum; i++){
        if (pthread_mutex_destroy(&muts[i]) != 0) { //todo check destruction
            fprintf(stderr, "system error: mutex error");
            exit(1);
        }
    }
    */

    printf("deletd muts \n");





    if (pthread_mutex_destroy(&outputMut) != 0) {
        fprintf(stderr, "system error: mutex error");
        exit(1);
    }

    if (pthread_mutex_destroy(&waitMut) != 0) { //todo check if could be lockec when destroied
        fprintf(stderr, "system error: mutex error");
        exit(1);
    }

    if (pthread_cond_destroy(&waitcv) != 0){
        fprintf(stderr, "[[Barrier]] error on pthread_cond_destroy");
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
    printf("deletd vecs \n");
    //-------------------------------------  //other stuff
    while (!k2vec->empty()){
        k2vec->pop_back();
    }
    delete k2vec;
    printf("deletd k2v2s \n");
    midMap.clear();
    printf("deletd midMap \n");
    InputVec trash;
    input=trash;
}

void JobContext::get_state(JobState* _state){
    if(pthread_mutex_lock(&muts[tnum-1])!=0) { //lock state mutex
        fprintf(stderr, "system error: mutex error");
        exit(1);
    }
    (*(_state)).stage=state.stage; //get values
    _state->percentage=state.percentage;

    if(pthread_mutex_unlock(&muts[tnum-1])!=0){ //unlock state mutex
        fprintf(stderr, "system error: mutex error");
        exit(1);
    }
}

pthread_t* JobContext::get_threads(){
    return threads;
}
pthread_mutex_t* JobContext::get_muts(){
    return muts;
}


void * mapper(void* inCon){ //used to manage map phase
    printf("in mapper \n");
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
    printf("map before barr\n");
    context->activateBarr(); //barrier to wait till al threads finish mapping
    printf("map to reduce\n");
    context->reducer(); //reduce stage
    context->imDone();
    return 0;
}
void * shuffle(void* inCon){
    printf("in shuffle \n");
    JobContext * context =(JobContext*) inCon;
    int i = 0;
    vector<IntermediatePair> vecToMap;
    int flag = 0;
    JobState state;
    context->get_state(&state);

    while (state.stage!=REDUCE_STAGE && state.stage!=UNDEFINED_STAGE ) {

        if(pthread_mutex_lock(&(context->get_muts()[i]))!=0){
            fprintf(stderr, "system error: mutex error");
            exit(1);
        }
        while(! context->get_vecs()[i]->empty()){
            flag = 1;
            vecToMap.push_back(context->get_vecs()[i]->back());
            ( context->get_vecs() )[i]->pop_back();
        }
        if(pthread_mutex_unlock(&(context->get_muts()[i]))!=0){
            fprintf(stderr, "system error: mutex error");
            exit(1);
        }
        if (flag) {
            flag = 0;
            context->pushToMap(&vecToMap);
        }
        i++;
        i=i%(context->getTnum()-1);
        context->get_state(&state);
    }
    printf("shuffle before barr\n");
    context->activateBarr();
    printf("shuffle to reduce\n");
    context->reducer();
    context->imDone();
    return 0;
}

vector<IntermediatePair >** JobContext::get_vecs(){
    return vecs;
}
int JobContext::getMyVec(){
    return ((vecAssign)++);
}
int JobContext::getMyPair(){
    return (nextPair)++;
}
int JobContext::getDoneThreads(){
    int num=( (--doneThreads) ==0);
    return num;
}
const MapReduceClient& JobContext::get_client(){
    return client;
}
InputVec JobContext::getInputVec(){
    return input;
}

int JobContext::getInPairs(){
    return inPairs;
}

int JobContext::getTnum(){
    return tnum;
}

void JobContext::setMapState()
{
    state.stage = MAP_STAGE;
}
void JobContext::endMapStage() {

    if(pthread_mutex_lock(&muts[tnum-1])!=0){
        fprintf(stderr, "system error: mutex error");
        exit(1);

    }
    printf("in end map stage \n");

    state.stage = SHUFFLE_STAGE;
    state.percentage=(((double) (shuffled) )/( (double)(k2v2s) ) )*100;
    if(state.percentage==100){ //if we already finished shuffling;
        state.percentage=0;
        state.stage=REDUCE_STAGE;
        nextPair=0;
        (doneThreads)=tnum;
    }
    printf("shuffled at end map is: %d\n",(int)shuffled);
    if(pthread_mutex_unlock(&muts[tnum-1])!=0){
        fprintf(stderr, "system error: mutex error");
        exit(1);
    }

}

void JobContext::mapUpdate(int x) { //update percentage of map phased

    if(pthread_mutex_lock(&muts[tnum-1])!=0){
        fprintf(stderr, "system error: mutex error");
        exit(1);

    }
    int done=(++doneMaps);
    k2v2s+=x;
    state.percentage= (  ( (double)(done) ) / ( (double)(inPairs))  ) *100;  //

    if(pthread_mutex_unlock(&muts[tnum-1])!=0){
        fprintf(stderr, "system error: mutex error");
        exit(1);
    }
    return;
}

void JobContext::shuffleUpdate(){



    if(pthread_mutex_lock(&muts[tnum-1])!=0){
        fprintf(stderr, "system error: mutex error");
        exit(1);
    }
    (shuffled)++;
    if(state.stage==SHUFFLE_STAGE){
        state.percentage=(((double)( (shuffled))) / ((double)((k2v2s) )))*100;
      //  printf(                "shuffle to %f\n",state.percentage);
     //   printf("shuffled: %d   ,   k2v2s: %d    \n", (int)shuffled, (int) k2v2s);
        if(state.percentage==100){
            state.stage = REDUCE_STAGE;
            state.percentage = 0;
            nextPair=0;
            (doneThreads)=tnum;

        }
    }

    if(pthread_mutex_unlock(&muts[tnum-1])!=0){
        fprintf(stderr, "system error: mutex error");
        exit(1);
    }
}

atomic <int>* JobContext::getK2V2s(){
    return &k2v2s;
}

void JobContext::pushToMap(vector <IntermediatePair>* vecToMap){

    while (!vecToMap->empty()) {
        IntermediatePair toMap = vecToMap->back();
        vecToMap->pop_back();
        if (midMap.count(toMap.first) == 0) {
            (k2Count)++;
            k2vec->push_back(toMap.first);
            //vector<V2 *> *valVector = new vector<V2 *>;
            vector<V2 *> valVector;
           /* if (!valVector){
                fprintf(stderr,"alloc error");
                exit(1);
            }*/
            midMap.insert(pair<K2 *, vector<V2 *> >(toMap.first, valVector));
        }

        midMap[toMap.first].push_back(toMap.second);
        shuffleUpdate();
    }
};



void JobContext::activateBarr(){
    barr.barrier();
}

void JobContext::reducer(){

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

    if(pthread_mutex_lock(&muts[tnum-1])!=0){
        fprintf(stderr, "system error: mutex error");
        exit(1);
    }

    state.percentage = (((double)(++reducedk2v2s)) / ((double)(k2Count))) * 100;
    if (state.percentage==100){
     //   printf("end reduce \n");

    }


    if(pthread_mutex_unlock(&muts[tnum-1])!=0){
        fprintf(stderr, "system error: mutex error");
        exit(1);
    }
}

void JobContext::waiter(){
    if (pthread_mutex_lock(&waitMut) != 0){
        printf("this error\n");
        fprintf(stderr, "error on pthread_mutex_lock");
        exit(1);
    }
    //todo checking is not atomic
   // printf("and now, i shall wait\n");
    if (doneThreads>0){
        if (pthread_cond_wait(&waitcv, &waitMut) != 0) {
            fprintf(stderr, "error on pthread_cond_wait");
            exit(1);
        }
    }
    if (pthread_mutex_unlock(&waitMut) != 0){
        fprintf(stderr, "error on pthread_mutex_unlock");
        exit(1);
    }
}

void JobContext::init_threads(JobHandle myJob) {
    for (int i = 0; i < tnum-1; ++i) {
        if(pthread_create(threads + i, nullptr, mapper, myJob)!=0){
            fprintf(stderr, "thread error");
            exit(1);
        }
    }
    if(pthread_create(threads + tnum-1, nullptr, shuffle, myJob)!=0){
        fprintf(stderr, "thread error");
        exit(1);
    }
}

void JobContext::imDone() {
   // printf("im done bitches! \n");


        if (pthread_mutex_lock(&waitMut) != 0){
            printf("that error\n");
            fprintf(stderr, "error on pthread_mutex_lock");
            exit(1);
        }
        if (--(doneThreads)== 0) {
            printf("we're done bitches! \n");
            printf("done threads = 0\n");
            if (pthread_cond_broadcast(&waitcv) != 0) {
                fprintf(stderr, "error on pthread_cond_broadcast");
                exit(1);
            }
        }
        printf("threads left: %d", (int)doneThreads);
        if (pthread_mutex_unlock(&waitMut) != 0){
            fprintf(stderr, "error on pthread_mutex_unlock");
            exit(1);
        }



}







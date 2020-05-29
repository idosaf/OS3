//
// Created by Ido on 5/20/2020.
//

#include "MapReduceFramework.h"
#include "JobContext.h"

struct reduceContext{
    OutputVec& output;
    pthread_mutex_t* outputMut;
};

struct mapContext{  //struct to send to emit3 via reduce
    pthread_mutex_t* myMut;
    vector<IntermediatePair>* myVec;
    int* outputs;
};

void emit2 (K2* key, V2* value, void* context)
{
        mapContext* myCon= (mapContext *)context;
        if(pthread_mutex_lock((myCon->myMut))!=0){
            fprintf(stderr, "system error: mutex error");
            exit(1);
        }

        myCon->myVec->push_back(pair<K2*,V2*>(key,value));

        if(pthread_mutex_unlock(myCon->myMut)!=0){
            fprintf(stderr, "system error: mutex error");
            exit(1);
        }
    ++( *(myCon->outputs));

}

void emit3 (K3* key, V3* value, void* context){

     reduceContext * myCon=(reduceContext *)context;

    if(pthread_mutex_lock(myCon->outputMut)!=0){
        fprintf(stderr, "system error: mutex error");
        exit(1);
    }

    myCon->output.push_back(pair<K3*,V3*>(key,value));

    if(pthread_mutex_unlock(myCon->outputMut)!=0){
        fprintf(stderr, "system error: mutex error");
        exit(1);
    }

}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){


    JobContext* myHandle=(JobContext*) new JobContext(multiThreadLevel,  inputVec, outputVec , client);
    myHandle->setMapState();
    (myHandle)->init_threads(myHandle);
    return myHandle;
}

void waitForJob(JobHandle job){
    //printf("waiting \n");
    JobContext * myJob = (JobContext *) job;
    myJob->waiter();
  //  printf("done waiting \n");

}
void getJobState(JobHandle job, JobState* state){
    JobContext * myJob = (JobContext *) job;
    myJob->get_state(state);
}
void closeJobHandle(JobHandle job){
    printf("closing job\n");
   // printf("closing \n");
    waitForJob(job);
    JobContext * myJob = (JobContext *) job;
    delete myJob;
}





#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/stat.h>
#include <fcntl.h>
#define TRUE 1

/*
    autors: Eldad Kronfeld 313429607
            Peter Zidan 316219716
*/


typedef int Clock_t;
typedef unsigned int Key_t;
typedef void* data_t;

struct Resource
{
    Key_t id_resource;
    char name[100];
    int cap;
    int used;
    sem_t sem;
    sem_t mutex;

}typedef Resource;

struct Repair
{
    Key_t repairId;
    char name[100];
    Clock_t time;
    int numberResources;
    Key_t *resources; //free

}typedef Repair;

struct Request
{
    Clock_t time;
    Key_t carId;
    int n;
    Key_t *repairs; //free
    pthread_t thread;

} typedef Request;

struct Node
{
    Key_t key;
    data_t data;
    struct Node *next,*prev;

}typedef Node;

struct Chain
{
    int nodes;
    Node *start;
}typedef Chain;

struct HashTable
{
    int n,number_of_items;
    Chain **table; //free
    int (*hash)(Key_t,int);
    Key_t (*getkey)(data_t);
    void (*delete)(data_t);
}typedef HashTable;

struct File_Controller
{
    HashTable *table;
    char *name;
    data_t (*create_data)(FILE*);
};


//////////////////////
// hash related functions
//////////////////////
int hash(Key_t,int);

//////////////////////
// get_key functions
//////////////////////
Key_t key_repairs(data_t data);
Key_t key_requests(data_t data);
Key_t key_resources(data_t data);

//////////////////////
//create data related functions
//////////////////////
data_t create_repair(FILE*);
data_t create_request(FILE*);
data_t create_resource(FILE*);

//////////////////////
//delete related functions
//////////////////////
void delete_repair(data_t data);
void delete_request(data_t data);
void delete_resource(data_t data);

//////////////////////
//Node related functions
//////////////////////
Node* create_Node(void* data,Key_t key);
void free_Node(Node* node,void (*delete)(data_t));

//////////////////////
//Chain related functions
//////////////////////
Chain* create_Chain();
void add_Node(Chain*,Node*);
Node* find_Node(Chain*,Key_t);
void free_Chain(Chain*,void (*delete)(data_t));

//////////////////////
//Hash Table ADT
/////////////////////
void init_HashTable(HashTable *ht,int n,int (*hash)(Key_t,int),
                    Key_t (*getkey)(data_t),void (*delete)(data_t));
void add(HashTable*,data_t in);
Node* find(HashTable*,Key_t);
void delete(HashTable*,Node*);

void free_table(HashTable*);

//////////////////////
//file reader 
//////////////////////
//thread main function for file I/O
void* proccess_File(void* in);

//////////////////////
//simulation handler 
//////////////////////
void* simulator(void *in);
void start_Requests(Chain *chain);

//////////////////////
//resource handles 
//////////////////////
int resources_use(Repair *r);
int resources_release(Repair *r);


//////////////////////
//request handles 
//////////////////////
void *request_handler(void *in);

void printErr(char *str){
    printf(str);
    exit(-1);
}

void print_repairs(data_t data){
    Repair *rep=(Repair*)data;
    int i;
    printf("%u %s %u %d ",rep->repairId,rep->name,rep->time,rep->numberResources);
    for(i=0;i<rep->numberResources;i++)
        printf("%u ",rep->resources[i]);
    
}
void print_request(data_t data){
    Request* req = (Request*)data;
    int i;
    printf("%u %u %d ",req->carId,req->time,req->n);
    for(i=0;i<req->n;i++)
        printf("%u ",req->repairs[i]);

}
void print_resource(data_t data){
    Resource* r=(Resource*)data;
    printf("%u %s %d %d",r->id_resource,r->name,r->cap,r->used);
}
void print_all(Chain* c,void (*print)(data_t)){
    Node* node = c->start;
    while(node != NULL){
        print(node->data);
        printf(" key=%u \n",node->key);
        node = node->next;
    }
}

HashTable resources,requests,repairs;
sem_t mutex;
int main(int argc,char* argv[]){
    //print results into a file without changing the program
    int file;
    if((file = open("output.txt",O_WRONLY|O_CREAT,S_IRWXG|S_IRWXU|S_IRWXO)) == -1)
        printf("couldn't open output.txt");
    int tmp = dup2(file,1);
    if(tmp == -1) 
        printf("couldn't switch to output");
    
    if(argc <3 )   
        printErr("invalid input");
    //initiate the hash tables
    init_HashTable(&resources,29,hash,key_resources,delete_resource);
    init_HashTable(&requests,24,hash,key_requests,delete_request);
    init_HashTable(&repairs,29,hash,key_repairs,delete_repair);

    sem_init(&mutex,0,1);
    //create the file_controller for the I/O threads
    struct File_Controller control[3];
    //resource 
    control[0].create_data = create_resource;
    control[0].name = argv[1];
    control[0].table = &resources;
    //repair
    control[2].create_data = create_repair;
    control[2].name = argv[2];
    control[2].table = &repairs;
    //request
    control[1].create_data = create_request;
    control[1].name = argv[3];
    control[1].table = &requests;

    
    //threads for I/O from files
    pthread_t input_handles[3];
    int i;
    for(i=0;i<3;i++)
        if(pthread_create(&input_handles[i],NULL,proccess_File,&control[i]) != 0 )
        printErr("couldn't create thread in I/O");
    
    for(i=0;i<3;i++)
       pthread_join(input_handles[i],NULL);
    //finished initializing the data into the hash tables
    printf("\n");
    for(i=0;i<requests.n;i++)
        start_Requests(requests.table[i]);
    pthread_t timer;
    if(pthread_create(&timer,NULL,simulator,NULL) != 0)
        printErr("couldn't start timer\n");
    void *res;
    Node* node;    
    for(i=0;i<requests.n;i++){
        node = requests.table[i]->start;
        while(node != NULL){
            Request* req =(Request*)node->data;
            pthread_join(req->thread,&res);
            node = node->next;
        }
    }
    free_table(&requests);
    free_table(&resources);
    free_table(&repairs);
    printf("END\n");

}

//hash
int hash(Key_t key,int n){
    return key%n;
}

//keys
Key_t key_repairs(data_t data){
    return ((Repair*)data)->repairId;
}
Key_t key_requests(data_t data){
    return ((Request*)data)->time;
}
Key_t key_resources(data_t data){
    return ((Resource*)data)->id_resource;
}

//delete data
void delete_repair(data_t data){
    Repair* r = (Repair*) data;
    free(r->resources);
    free(r);
}
void delete_request(data_t data){
    Request* r = (Request*) data;
    free(r->repairs);
    free(r);
}
void delete_resource(data_t data){
    free(data);
}

void bubble_Sort(Key_t *arr,int n){
    int i;
    int j;
    Key_t tmp;
    for(i=0;i<n;i++)
        for(j=0;j<n-1-i;i++)
            if(arr[j]> arr[j+1]){
                tmp = arr[j+1];
                arr[j+1] = arr[j];
                arr[j] = tmp;
            }
}
//create data
data_t create_repair(FILE* f){
    Repair* r;
    int i;
    if((r=(Repair*)malloc(sizeof(Repair))) == NULL) printErr("couldn't create repair\n");
    fscanf(f,"%u\t%[^\t]\t%u\t%d",&(r->repairId),r->name,&(r->time),&(r->numberResources));
    if((r->resources=malloc(sizeof(Key_t)*r->numberResources)) == NULL) printErr("couldn't create repair's resources\n");
    for(i=0;i<r->numberResources;i++)
        fscanf(f,"%u",&(r->resources[i]));
    bubble_Sort(r->resources,r->numberResources);
    return (data_t)r;
}
data_t create_request(FILE* f){
    int i=0;
    Request* r;
    if((r=(Request*)malloc(sizeof(Request))) == NULL) printErr("couldn't create request!\n");
    fscanf(f,"%u\t%u\t%d",&(r->carId),&(r->time),&(r->n));
    if((r->repairs=malloc(sizeof(Key_t)*r->n)) == NULL) printErr("couldn't create request's repairs!\n");
    for(i=0;i<r->n;i++)
        fscanf(f,"\t%u",&(r->repairs[i]));
    return (data_t)r;
}
data_t create_resource(FILE* f){
    Resource* r;
    char buffer[100];
    if((r=(Resource*)malloc(sizeof(Resource))) == NULL) printErr("couldn't create resource!\n");
    fscanf(f,"%u\t%[^\t]\t%d",&r->id_resource,buffer,&r->cap);
    strcpy(r->name,buffer);
    sem_init(&r->mutex,0,1);
    r->used = 0;
    return (data_t)r;
}

//nodes
Node* create_Node(data_t data,Key_t key){
    Node* tmp;
    if((tmp = (Node*)malloc(sizeof(Node))) == NULL)
        printErr("couldn't create Node");
    tmp->data = data;
    tmp->key = key; 
    return tmp;
}
void free_Node(Node* node,void (*delete)(data_t)){
    delete(node->data);
    free(node);
}

//chain
Chain* create_Chain(){
    Chain* chain;
    if((chain = malloc(sizeof(Chain))) ==  NULL) printErr("couldn't create chain");
    chain->nodes = 0;
    chain->start = NULL;
    return chain;
}
Node* find_Node(Chain* chain,Key_t key){
    Node* node = chain->start;
    while(node != NULL){
        if(node->key == key)
            return node;
        node = node->next;
    }
    return node;
}
void add_Node(Chain* chain,Node* node){
    chain->nodes+=1;
    node->prev = NULL;
    if(chain->start != NULL)
        chain->start->prev = node;
    node->next = chain->start;
    chain->start = node;
}
void free_Chain(Chain* chain,void (*delete)(data_t)){
    Node* node = chain->start;
    while(node != NULL){
        Node* tmp = node;
        node = node->next;
        free_Node(tmp,delete);
    }
}

//hash table 
void init_HashTable(HashTable *ht,int n,int (*hash)(Key_t,int),
                    Key_t (*getkey)(data_t),void (*delete)(data_t)){
        int i;
        ht->n = n;
        if((ht->table = malloc(sizeof(Chain*)*n)) == NULL) printErr("couldn't create hash table");
        for(i=0;i<n;i++)
            ht->table[i] = create_Chain();
        ht->hash = hash;
        ht->getkey = getkey;
        ht-> delete = delete;
}
void add(HashTable* table,data_t in){
    Chain** array = table->table;
    Key_t key = table->getkey(in);
    Node* node = create_Node(in,key);
    int index = table->hash(key,table->n);
    Chain* chain = array[index];
    add_Node(chain,node);
    table->number_of_items++;
}
Node* find(HashTable* table,Key_t key){
    int index = hash(key,table->n);
    Chain* chain = table->table[index];
    return find_Node(chain,key);
}
void delete(HashTable* table,Node* node){
    Node *prev = node->prev,*next=node->next;
    if(prev == NULL){
        int index = table->hash(node->key,table->n);
        table->table[index]->start = next;
    }
    else{
        prev->next = next;
    }
    free_Node(node,table->delete); 
    table->number_of_items--; 
}

void free_table(HashTable* ht){
    int i;
    for(i=0;i<ht->n;i++)
        free_Chain(ht->table[i],ht->delete);
    free(ht->table);
}
//file proccess 
void* proccess_File(void* in){
    struct File_Controller *control = (struct File_Controller*)in;
    FILE *f;
    if((f = fopen(control->name,"r")) == NULL) printErr("couldn't open file %s");
    while(!feof(f))
        add(control->table,control->create_data(f));
    return NULL;
}



//simulation vars 
Clock_t CLOCK = -1;

//simulation functions

//return the amount of threads that started

void start_Requests(Chain *chain){
    Node *node = chain->start;
    while(node != NULL){
        Request* r = (Request*)node->data;
        if(pthread_create(&r->thread,NULL,request_handler,(void*)r) != 0) 
            printErr("couldn't open thread in sim");
        node=node->next;
    }
}

void *simulator(void *in){
    //init sems
    CLOCK = -1;
    do{ 
        CLOCK++;
        sleep(1);
            
    }while(TRUE);
}


void print_start_job(Request* r,Repair* rep){
    printf("car: %u time: %u started %s.\n",r->carId,CLOCK,rep->name);
}
void print_end_job(Request* r,Repair* rep){
    printf("car: %u time: %u completed %s.\n",r->carId,CLOCK,rep->name);
}
//thread main
int ireq =0;

void *request_handler(void *in){
    Request *r = (Request*)in;
    Key_t* missions = r->repairs;
    int i=0;
    Repair* rep;
    while(r->time > CLOCK);
    while(i < r->n){
        //start of new repair
        rep = (Repair*)(find(&repairs,missions[i])->data);
        while(resources_use(rep) == -1);
        print_start_job(r,rep);
        sleep(rep->time);
        resources_release(rep);
        print_end_job(r,rep);
        i++;
    }
    return NULL;
}

int resources_use(Repair *r){
    int n = r->numberResources;
    int i,j;
    Resource *res;

    //wait for all
    for(i=0;i<n;i++){
        res = (Resource*)(find(&resources,r->resources[i])->data);
        sem_wait(&res->mutex);
    }
    //if not enough
    for(i=0;i<n;i++){
        res = (Resource*)(find(&resources,r->resources[i])->data);
        if(res->used == res->cap){
            for(j=0;j<n;j++){
                Resource* tmp = (Resource*)(find(&resources,r->resources[j])->data);
                sem_post(&(tmp->mutex));
            }
            return -1;
         }
    }

    for(i=0;i<n;i++){
        res = (Resource*)(find(&resources,r->resources[i])->data);
        res->used++;
    }
    //post 
    for(i=0;i<n;i++){
        res = (Resource*)(find(&resources,r->resources[i])->data);
        sem_post(&res->mutex);
    }
    return 0;      
}

int resources_release(Repair *r){
    int n = r->numberResources;
    int i;
    Resource *res;

    //wait for all
    for(i=0;i<n;i++){
        res = (Resource*)(find(&resources,r->resources[i])->data);
        sem_wait(&res->mutex);
    }

    //release all the resources
    for(i=0;i<n;i++){
        res = (Resource*)(find(&resources,r->resources[i])->data);
        res->used--;
    }
    //post 
    for(i=0;i<n;i++){
        res = (Resource*)(find(&resources,r->resources[i])->data);
        sem_post(&res->mutex);
    }
    return 0;      
}
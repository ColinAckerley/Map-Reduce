#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/wait.h>
#include "mapred.h"

int app = 0;
int impl = 0;
int maps = 0;
int reduces = 0;
FILE *infile;
FILE *outfile;
int parentID;
struct inputList *inputHead = NULL;
int inputCount = 0;
int largest = 0;
int smallest = INT_MAX;

struct HashNode **HashTable;
pthread_mutex_t lock1;

int main(int argc, char **argv){
	//READ AND PARSE ARGUMENTS
	int x;
	for(x = 0; x < argc-1; x++){
		if(strcmp(argv[x],"--app") == 0){
			if(strcmp(argv[x+1],"sort") == 0){
				app = 1;
			}
		} else if(strcmp(argv[x],"--impl") == 0){
			if(strcmp(argv[x+1],"threads") == 0){
				impl = 1;
			}
		} else if(strcmp(argv[x],"--maps") == 0){
			maps = atoi(argv[x+1]);
		} else if(strcmp(argv[x],"--reduces") == 0){
			reduces = atoi(argv[x+1]);
		} else if(strcmp(argv[x],"--input") == 0){
			infile = fopen(argv[x+1], "r");
		} else if(strcmp(argv[x],"--output") == 0){
			outfile = fopen(argv[x+1], "w");
		}
	}
	readInput();
	/**FOR DEBUGGING
	printf("App: %d, Impl: %d, Maps: %d, Reduces: %d, inputCount %d\n", app, impl, maps, reduces,inputCount);
	fflush(stdout);
	struct inputList *inNode = inputHead;
	while(inNode != NULL){
		if(inNode->data != NULL){
			printf("%s ", inNode->data);
		}
		inNode = inNode->next;
	}
	printf("\n");
	**/
	pthread_mutex_init(&lock1, NULL);
	HashTable = (struct HashNode **)calloc(1,sizeof(struct HashNode*)*reduces);
	mapSetup();
	
	/**MORE DEBUGGING
	if(getpid() == parentID){
		struct HashNode *temp;
		for(x=0; x<reduces; x++){
			temp = HashTable[x];
			while(temp != NULL){
				if(app == 1){
					printf("%d ", temp->num);
				} else {
					printf("%s ", temp->string);
				}
				temp = temp->next;
			}
		}
		printf("\n");
		
	}**/
	freeData();
	pthread_mutex_destroy(&lock1);
	return 0;
}

void freeData(){
	free(HashTable);
}

void mapSetup(){
	int x, y, nodeCount = 0;
	pthread_t *threadIDs;
	if(impl == 1){
		threadIDs = (pthread_t *)malloc(sizeof(pthread_t)*maps);
	}
	parentID = getpid();
	struct inputList *listChunkEnd, *tempHead, *newHead;
	newHead = inputHead;
	//Split input into ceiling of inputCount/maps, last thread will get <= input of the others
	for(x=0; x<maps; x++){
		if(nodeCount >= inputCount){
			pthread_create(&threadIDs[x], NULL, map, NULL);
			continue;
		}	
		nodeCount++;
		listChunkEnd = newHead;
		for(y=0; y<(inputCount+maps-1)/maps-1; y++){
			nodeCount++;
			if(nodeCount <= inputCount){		
				listChunkEnd = listChunkEnd -> next;
			}
		}
		tempHead = listChunkEnd -> next;
		listChunkEnd -> next = NULL;
		if(impl == 1){
			//thread
			pthread_create(&threadIDs[x], NULL, map, (void *)newHead);
		} else {
			//fork
			fflush(stdout);
			fork();
			if(getpid() != parentID){
				//CHILD CASE
				printf("CHILD %d EXECUTING\n", x);
				map(newHead);
				break;
			}
		}	
		newHead = tempHead;
	}
	if(impl == 1){
		for(x=0; x<maps; x++){
			pthread_join(threadIDs[x],NULL);
			printf("Child thread %d joined\n", (int)threadIDs[x]);
		}
	} else {
		while(wait(NULL) > 0);
	}
	//printf("PROCESS %d FINISHED\n", getpid());
}

void readInput(){
	//reads input, parses into a linked list (word/number per node),
	struct inputList *inNode = NULL;
	inNode = (struct inputList *)calloc(1,sizeof(struct inputList)*1);
	char buffer[2048];
    while(1){
        if(feof(infile))break;
        char *token;
        fgets(buffer, 2048, infile);
        token = strtok(buffer, " .,;:!-\n\t\r");
        while(token != NULL){
			inputCount++;
			inNode -> data = strdup(token);
			if(largest < (int)atoi(inNode -> data)){
				largest = (int)atoi(inNode -> data);
			}
			if(smallest > atoi(inNode -> data) && isdigit(inNode -> data[0])){
				smallest = atoi(inNode -> data);
			}
			if(inputHead == NULL){
				inputHead = inNode;
			}
			inNode -> next = (struct inputList *)calloc(1,sizeof(struct inputList)*1);
			inNode = inNode -> next;
			token = strtok(NULL, " .,;:!-\n\t\r");
		}
		memset(buffer, 0, 2048);
    }
}

void *map(void *dataList){
	if(dataList == NULL){
		return NULL;
	}
	fflush(stdout);
	//generic map to hash data to the hashtable for reduce step, hash function varies on app type
	struct inputList *dataPtr = (struct inputList *)dataList;
	struct inputList *temp;
	int index;
	temp = dataPtr;
	while(dataPtr != NULL){
		temp = temp->next;
		//map into the hash table
		if(app == 1){
			index = hashFuncSort(atoi(dataPtr->data));
		} else {
			index = hashFuncWcount(dataPtr->data);
		}
		if(impl == 1){	
			hashInsert(index, dataPtr);
		} else {
			//SHARED MEMORY INSERT
		}
		free(dataPtr);
		dataPtr = temp;		
	}
	return NULL;
}

int hashFuncSort(int value){
	//hash table of size r contains input value range divided by r 
	int blockSize = (largest-smallest)/reduces;
	int x, y = smallest;	
	for(x=0; x<reduces-1; x++){
		if(value >= y && value < y+blockSize){
			return x;
		} else {
			y = y+blockSize;
		}
	}
	return x;
}

int hashFuncWcount(char *string){
	int value = (int)tolower(string[0]);
	int blockSize = (26)/reduces;
	int x, y = 97;
	for(x=0; x<reduces-1; x++){
		if(value < y+blockSize){
			return x;
		} else {
			y = y+blockSize;
		}
	}
	return x;
}

void hashInsert(int index, struct inputList * dataPtr){
	//inputs value into hashtable
	pthread_mutex_lock(&lock1);
	struct HashNode *currentNode = HashTable[index];
	struct HashNode *newNode = (struct HashNode *)malloc(sizeof(struct HashNode));
	if(app == 1){
		//printf("inserted");
		newNode->num = atoi(dataPtr->data);
	} else {
		newNode->string = strdup(dataPtr->data);
	}
	newNode -> next = NULL;
	if(currentNode == NULL){
		HashTable[index] = newNode;
	} else {
		while(currentNode -> next != NULL){
			currentNode = currentNode -> next;
		}
		currentNode -> next = newNode;
	}
	pthread_mutex_unlock(&lock1);
}
<<<<<<< HEAD
=======
int stringCmpFunc(const void *a, const void *b) 
{ 
    const char **ia = (const char **)a;
    const char **ib = (const char **)b;
    return strcmp(*ia, *ib);
} 
void reduce(int index) //Reduce function for integers
{
	int curSize = 0; //Size of the current linked list
	struct HashNode *head = HashTable[index]; //Get the head of the linked list from the hashtable
	struct HashNode *linkedList = head; //Pointer to the head to traverse the linked list
	while(linkedList != NULL) //Gets the size of the current linked list 
	{
		curSize++;
		linkedList = linkedList->next;
	}
	linkedList = head;
	if(app==1)
	{
		int linkedListTraverse = 0; //Array index for each linked list node
		int toSort[curSize]; //Creates an array to be used with quicksort 
		while(linkedList != NULL)
		{
			toSort[linkedListTraverse] = linkedList->num; //Copy the data from the linked list into an array 
			linkedList = linkedList->next;
			linkedListTraverse++;
		}
		qsort(toSort, curSize, sizeof(int), numCmpFunc); //Sort the current node
		int i =0;
		for(; i < curSize; i++)
		{
			printf("%d", toSort[i]);
		}
	}
	else
	{
		char* toSort[curSize];
		int linkedListTraverse = 0; //Array index for each linked list node
		while(linkedList != NULL)
		{
			toSort[linkedListTraverse] = linkedList->string; //Copy the data from the linked list into an array 
			linkedList = linkedList->next;
			linkedListTraverse++;
		}
		qsort(toSort, curSize, sizeof(char*), stringCmpFunc); //Sort the current node
		int i =0;
		for(;i < curSize; i++)
		{
			printf("%s", toSort[i]);
		}
	}
}
>>>>>>> 9f9852741c16638cd9265282125304c3c75f78c0

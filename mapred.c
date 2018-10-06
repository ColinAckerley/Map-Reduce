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

struct HashNode *HashTable;

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
			printf("%s\n", inNode->data);
		}
		inNode = inNode->next;
	}
	printf("%d %d %d\n", largest, smallest, inputCount);**/
	
	HashTable = (struct HashNode *)calloc(1,sizeof(struct HashNode)*reduces);
	mapSetup();
	freeData();
	return 0;
}

void freeData(){
	free(HashTable);
}
int numCmpFunc (const void * a, const void * b) //Function to compare numbers for quicksort
{
   return ( *(int*)a - *(int*)b );
}

void mapSetup(){
	int x, y, nodeCount = 0;
	pthread_t *threadIDs;
	if(impl == 1){
		threadIDs = (pthread_t *)malloc(sizeof(pthread_t)*maps);
	} else {
		parentID = getpid();
	}
	struct inputList *listChunkEnd, *tempHead, *newHead;
	newHead = inputHead;
	//Split input into ceiling of inputCount/maps, last thread will get <= input of the others
	newHead = inputHead;
	for(x=0; x<maps; x++){		
		nodeCount++;
		listChunkEnd = newHead;
		for(y=0; y<(inputCount+maps-1)/maps-1; y++){
			nodeCount++;
			if(nodeCount <= inputCount-1){		
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
				
				break;
			}
		}	
		newHead = tempHead;
		if(nodeCount >= inputCount){
			break;
		}

	}
	if(impl == 1){
		for(x=0; x<maps; x++){
			pthread_join(threadIDs[x],NULL);
			printf("Child thread %d joined\n", (int)threadIDs[x]);
		}
	} else {
		while(wait(NULL) > 0);
	}
	printf("PROCESS %d FINISHED\n", getpid());
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
	//printf("Child thread\n");
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
		hashInsert(index, dataPtr);
		free(dataPtr);
		dataPtr = temp;		
	}
	return NULL;
}

int hashFuncSort(int value){
	//hash table of size r contains input value range divided by r 
	int blockSize = (largest-smallest)/reduces;
	int x, index = 0;	
	for(x=smallest; x<=largest; x=x+blockSize){
		if(value >= x && value < x+blockSize+reduces){
			printf("%d value, %d index, %d to %d\n", value, index, x, x+blockSize);
			return index;
		} else {
			index++;
		}
	}
	printf("%d value, %d index, %d to %d\n", value, index, x, x+blockSize);
	return index;
}

int hashFuncWcount(char *value){
	int index = 0;
	return index;
}

void hashInsert(int index, struct inputList * dataPtr){
	//inputs value into hashtable
	struct HashNode *currentNode = &HashTable[index];
	struct HashNode *newNode = (struct HashNode *)malloc(sizeof(struct HashNode));
	if(app == 1){
		newNode->num = *dataPtr->data;
	} else {
		newNode->string = strdup(dataPtr->data);
	}
	newNode -> next = NULL;
	while(currentNode -> next != NULL){
		currentNode = currentNode -> next;
	}
	currentNode -> next = newNode;
}
void reduce(int index) //Reduce function for integers
{
	int curSize = 0; //Size of the current linked list
	int linkedListTraverse = 0; //Array index for each linked list node
	Node *head = hashtable[index]; //Get the head of the linked list from the hashtable
	Node *linkedList = head; //Pointer to the head to traverse the linked list
	while(linkedList != NULL) //Gets the size of the current linked list 
	{
		curSize++;
		linkedList = linkedList->next;
	}
	int toSort[curSize]; //Creates an array to be used with quicksort 
	while(linkedList != NULL)
	{
		toSort[linkedListTraverse] = linkedList->data; //Copy the data from the linked list into an array 
		linkedList = linkedList->next;
		linkedListTraverse++;
	}
	qsort(toSort, curSize, sizeof(int), numCmpFunc); //Sort the current node
}


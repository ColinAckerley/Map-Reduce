#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/ipc.h>
#include <sys/types.h>
#include <sys/shm.h>
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
struct reduceNode **sortedArray;
struct HashNode **HashTable;
int shmid;
struct shmNode *shmArray;
int shmLockid;
struct shmLockNode *shmLock;
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
	printf("App: %d, Impl: %d, Maps: %d, Reduces: %d, inputCount %d\n", app, impl, maps, reduces,inputCount);
	HashTable = (struct HashNode **)calloc(1,sizeof(struct HashNode*)*reduces);
	sortedArray = (struct reduceNode **)calloc(1,sizeof(struct reduceNode*)*inputCount);
	mapSetup();
	reduceSetup(); 
	//~ int i = 0;
	//~ while(i < inputCount)
	//~ {
		//~ printf("%s %d\n",sortedArray[i]->word, sortedArray[i]->count);
		//~ i++;
	//~ }
	//~ //MORE DEBUGGING
	//~ if(getpid() == parentID){
		//~ struct HashNode *temp;
		//~ for(x=0; x<reduces; x++){
			//~ temp = HashTable[x];
			//~ while(temp != NULL){
				//~ if(app == 1){
					//~ //printf("%d ", temp->num);
				//~ } else {
					//~ //printf("%s ", temp->string);
				//~ }
				//~ temp = temp->next;
			//~ }
		//~ }
		//~ printf("\n");
	//~ }
	freeData();
	pthread_mutex_destroy(&lock1);
	return 0;
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
		free(token);
		memset(buffer, 0, 2048);
    }
    free(inNode);
}

void freeData(){
	int x;
	struct HashNode *temp1, *temp2;
	for(x=0; x<reduces; x++){
		temp1 = HashTable[x];
		while(temp1 != NULL){
			if(app == 0){
				free(temp1->string);
			}
			temp2 = temp1->next;
			free(temp1);
			temp1 = temp2;
		}
	}
	free(HashTable);
	for(x=0; x<inputCount; x++){
		if(sortedArray[x] != NULL){
			if(app == 0){
				free(sortedArray[x]->word);
			}
			//printf("%d ", &sortedArray[x]->num);
			//free(sortedArray[x]);
		}
	}
	free(sortedArray);
}

void mapSetup(){
	int x, y, nodeCount = 0;
	pthread_t *threadIDs;
	if(impl == 1){
		threadIDs = (pthread_t *)malloc(sizeof(pthread_t)*maps);
		pthread_mutex_init(&lock1, NULL);
	} else {
		shmid = shmget(IPC_PRIVATE, sizeof(struct shmNode)*inputCount,IPC_CREAT|0666);
		shmArray = (struct shmNode *)shmat(shmid,0,0);
		shmLockid = shmget(IPC_PRIVATE, sizeof(struct shmLockNode),IPC_CREAT|0666);
		shmLock = (struct shmLockNode *)shmat(shmLockid,0,0);
		pthread_mutexattr_t mutexAttribute;
		pthread_mutexattr_init(&mutexAttribute);
		pthread_mutexattr_setpshared(&mutexAttribute, PTHREAD_PROCESS_SHARED);
		pthread_mutex_init(&shmLock->mutex, &mutexAttribute);
		//~ pthread_condattr_t conditionAttribute;
		//~ pthread_condattr_init(&conditionAttribute);
		//~ pthread_condattr_setpshared(&conditionAttribute, PTHREAD_PROCESS_SHARED);
		//~ pthread_cond_init(&shmLock->condition, &conditionAttribute);
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
			pthread_create(&threadIDs[x], NULL, map, (void *)newHead);
		} else {
			fflush(stdout);
			fork();
			if(getpid() != parentID){
				map(newHead);
				break;
			}
		}	
		newHead = tempHead;
	}
	if(impl == 1){
		for(x=0; x<maps; x++){
			pthread_join(threadIDs[x],NULL);
			//printf("Child thread %d joined\n", (int)threadIDs[x]);
		}
		free(threadIDs);
		
	} else {
		for(x=0; x<maps; x++){
			wait(NULL);
		}
		if(getpid() == parentID){
			for(x=0; x<inputCount; x++){
				//printf("%d %d %d %s\n", shmArray[x].taken, shmArray[x].index, shmArray[x].num, shmArray[x].string);
				if(app == 1){
					hashInsert((int)shmArray[x].index, (int)shmArray[x].num, "");
				} else {
					hashInsert((int)shmArray[x].index, 0, (char *)shmArray[x].string);
				}
			}
			//printf("\n");
			shmctl(shmid, IPC_RMID, NULL);
			shmdt((void *)shmArray);
			shmctl(shmLockid, IPC_RMID, NULL);
			shmdt((void *)shmLock);
		}
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
			hashInsert(index, atoi(dataPtr->data), "");
			//printf("%d \n", index);
		} else {
			index = hashFuncWcount(dataPtr->data);
			hashInsert(index, 0, dataPtr->data);
		}
		if(impl == 0){
			int x = 0;
			//race condition
			pthread_mutex_lock(&shmLock->mutex);
			while(shmArray[x].taken == 1){
				x++;
			}
			shmArray[x].taken = 1;
			shmArray[x].index = index;
			if(app == 1){
				shmArray[x].num = atoi(dataPtr->data);
			} else {
				strcpy(shmArray[x].string,dataPtr->data);
			}
			pthread_mutex_unlock(&shmLock->mutex);
		}
		free(dataPtr->data);
		free(dataPtr);
		dataPtr = temp;		
	}
	return NULL;
}

int hashFuncSort(int value){
	//hash table of size r contains input value range divided by r 
	int blockSize = (largest-smallest)/reduces;
	blockSize = blockSize == 0 ? 1 : blockSize;
	int x, y = smallest;	
	for(x=0; x<reduces-1; x++){
		if(value < y+blockSize){
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
	blockSize = blockSize == 0 ? 1 : blockSize;
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

void hashInsert(int index, int num, char *string){
	//inputs value into hashtable
	pthread_mutex_lock(&lock1);
	struct HashNode *currentNode = HashTable[index];
	struct HashNode *newNode = (struct HashNode *)malloc(sizeof(struct HashNode));
	if(app == 1){
		newNode->num = num;
	} else {
		newNode->string = strdup(string);
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

int stringCmpFunc(const void *a, const void *b){ 
    const char **ia = (const char **)a;
    const char **ib = (const char **)b;
    return strcmp(*ia, *ib);
} 

int numCmpFunc (const void * a, const void * b){
	const int *A = a, *B = b;
	return (*A > *B) - (*A < *B);
}

void reduceSetup(){
	pthread_t *threadIDs;
	int* values;
	int i;
	if(impl == 1){
		threadIDs = (pthread_t *)malloc(sizeof(pthread_t)*reduces);
		values = (int *)malloc(sizeof(int)*reduces);
		for(i=0; i<reduces; i++){
			values[i] = i;
			pthread_create(&threadIDs[i], NULL, reduce, &values[i]);
		}
	}
	if(impl == 1){
		for(i=0; i<reduces; i++){
			pthread_join(threadIDs[i],NULL);
			//printf("Child thread %d joined\n", (int)threadIDs[x]);
		}
		free(threadIDs);
		free(values);
	}
}

void *reduce(void *num){ //Reduce function 
	int index = *(int*)num;
	int curSize = 0; //Size of the current linked list
	struct HashNode *head = HashTable[index]; //Get the head of the linked list from the hashtable
	struct HashNode *linkedList = head; //Pointer to the head to traverse the linked list
	while(linkedList != NULL){ //Gets the size of the current linked list 
		curSize++;
		linkedList = linkedList->next;
	}
	linkedList = head;
	int linkedListTraverse = 0; //Array index for each linked list node
	int *numSort = numSort = (int *)malloc(sizeof(int)*curSize);
	char **wordSort = (char **)malloc(sizeof(char*)*curSize);
	while(linkedList != NULL){
		if(app == 1){
			numSort[linkedListTraverse] = (int)linkedList->num; //Copy the data from the linked list into an array 
		} else {
			wordSort[linkedListTraverse] = strdup(linkedList->string); //Copy the data from the linked list into an array 
		}
		linkedList = linkedList->next;
		linkedListTraverse++;
	}
	if(app == 1){
		qsort(numSort, curSize, sizeof(int), numCmpFunc); //Sort the current node
		int i, j = 0;
		struct reduceNode *numSortArray = (struct reduceNode *)malloc(sizeof(struct reduceNode)*curSize);
		for(i = 0; i < curSize; i++){
			numSortArray[i].num = (int)numSort[i];
		}
		i = 0;
		while(sortedArray[i] != NULL){
			i++;
		}
		for(j = 0; j < curSize; j++, i++){  
			sortedArray[i] = &numSortArray[j];
		}
		free(numSort);
		return (void*) 0;
	} else {/**
		int i = 0;
		while(i < curSize) //Convert all of the words to lowercase
		{
			char* str = wordSort[i];
			char *p;
			for (p = str; *p != '\0'; p++)
				*p = (char)tolower(*p);
			i++;
		}
		qsort(wordSort, curSize, sizeof(char*), stringCmpFunc); //Sort the current node	
		int curWordIndex = 0;
		int checkWordIndex = 1;
		int curArrayIndex = 0;
		reduceNode *curWord;
		reduceNode* wordCountArray = (reduceNode*) malloc(sizeof(reduceNode)*curSize);
		while(curWordIndex < curSize)
		{
			curWord = (reduceNode*) malloc(sizeof(reduceNode));
			curWord->word = wordSort[curWordIndex]; //Set the word
			curWord->count = 1; //Start the count out at 1
			while(strcmp(wordSort[curWordIndex], wordSort[checkWordIndex]) == 0) //While the two words are equal
			{
				checkWordIndex++; //Advance the leading index check
				curWord->count++;
				if(checkWordIndex >= curSize) //If the leading index goes beyond the array bounds
				{
					break;
				}
			}
			if(checkWordIndex < curSize)
			{
				curWordIndex = checkWordIndex;
				checkWordIndex = curWordIndex + 1;
			}
			else if(checkWordIndex >= curSize) //If the leading index goes beyond the array bounds
			{
				if(strcmp(curWord->word, wordSort[curWordIndex]) != 0)
				{
					curWord->word = wordSort[curWordIndex]; //Set the word
					curWord->count = 1; //Start the count out at 1
					curArrayIndex++;
					wordCountArray[curArrayIndex] = curWord;
				}
				else
				{
					wordCountArray[curArrayIndex] = curWord;
					curArrayIndex++;
				}
				break;
			}
			wordCountArray[curArrayIndex] = curWord;
			curWord = NULL;
			curArrayIndex++;
		}
		int i = 0;
		for(; i < curArrayIndex; i++)
		{
			printf("%s %d\n", wordCountArray[i]->word, wordCountArray[i]->count);
		}**/
		free(wordSort);
		return (void*) 0;
	}
	return (void*) 0;
}

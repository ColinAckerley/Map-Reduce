#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapred.h"
#include <limits.h>

int app = 0;
int impl = 0;
int maps = 0;
int reduces = 0;
FILE *infile;
FILE *outfile;
struct inputList *inputHead = NULL;
int inputCount = 0;
int largest = 0;
int smallest = INT_MAX;

struct HashNode{
	int num;
	char *string;
	struct HashNode* next;
};

struct inputList{
    char *data;
    struct inputList *next;
};

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
	fflush(stdout);
	struct inputList *inNode = inputHead;
	/***FOR DEBUGGING
	while(inNode != NULL){
		if(inNode->data != NULL){
			printf("%s\n", inNode->data);
		}
		inNode = inNode->next;
	}
	printf("%d %d %d", largest, smallest, inputCount);***/
	
	HashTable = (struct HashNode *)calloc(1,sizeof(struct HashNode)*reduces);
	mapSetup();
	
}

void freeData(){
	int x;
	struct inputList *temp;
	temp = inputHead;
	for(x=0; x<inputCount; x++){
		inputHead = inputHead->next;
		free(temp);
		temp = inputHead;		
	}
	free(HashTable);
}

void mapSetup(){
	int x, y;
	struct inputList *listChunkEnd, *tempHead, *newHead;
	newHead = inputHead;
	for(x=0; x<maps; x++){
		listChunkEnd = newHead;
		for(y=0; y<(inputCount/reduces)-1; y++){
			listChunkEnd = listChunkEnd -> next;
		}
		if(x+1 == maps && listChunkEnd -> next != NULL){
			//if inputCount/reduces not evenly divided, add remainder to last map node
			listChunkEnd = listChunkEnd -> next;
		}
		tempHead = listChunkEnd -> next;
		listChunkEnd -> next = NULL;
		//fork/thread part of input to mapnode (FROM inputHead TO listChunkEnd)
		if(impl == 1){
			//thread
			
		} else {
			//fork
			
		}	
		newHead = tempHead;
	}
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

void *mapSort(void *dataList){
	struct inputList *dataPtr = (inputList *)dataList;
	struct inputList *temp;
	int index;
	temp = dataPtr;
	while(dataPtr != NULL){
		temp = temp->next;
		//map into the hash table
		index = hashFuncSort(dataPtr->data);
		hashInsert(index, dataPtr);
		free(dataPtr);
		dataPtr = temp;		
	}
}

int hashFuncSort(int value){
	int blockSize = (largest-smallest)/reduces;
	int x, index = 0;
	for(x=smallest; x<largest; x=x+blockSize){
		if(value >= x && value < x+blockSize){
			return index;
		} else {
			index++;
		}
	}
	return index;
}

void hashInsert(index, dataPtr){
	if(app == 1){
		
	struct node *currentNode = hashTable[key];
	if(hashTable[key] != NULL){
		currentNode = hashTable[key];
		if(currentNode -> value == num){
				printf("duplicate\n");
				return;
		}
		while(currentNode -> next != NULL){
			if(currentNode -> value == num){
				printf("duplicate\n");
				return;
			}
			currentNode = currentNode -> next;
		}
		struct node *newNode = (struct node *)malloc(sizeof(struct node ));
		newNode -> value = num;
		newNode -> next = NULL;
		currentNode -> next = newNode;
		printf("inserted\n");
		return;
	} else {
		if(currentNode != NULL && currentNode -> value == num){
			printf("duplicate\n");
			return;
		}
		struct node *newNode = (struct node *)malloc(sizeof(struct node ));
		newNode -> value = num;
		newNode -> next = NULL;
		hashTable[key] = newNode;
		printf("inserted\n");
		return;
	}
}

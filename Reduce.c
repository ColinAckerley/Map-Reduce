#include <stdio.h>
#include <stdlib.h>
int main
{
	reduce(0);
}
int numCmpFunc (const void * a, const void * b) //Function to compare numbers for quicksort
{
   return ( *(int*)a - *(int*)b );
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

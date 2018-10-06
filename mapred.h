struct inputList{
    char *data;
    struct inputList *next;
};
struct HashNode{
	int num;
	char *string;
	struct HashNode* next;
};
int main(int , char **);
void readinput();
void mapSetup();
void readInput();
void freeData();
void hashInsert(int, struct inputList *);
int hashFuncSort(int);
int hashFuncWcount(char *);
void *map(void *);
<<<<<<< HEAD
=======
int numCmpFunc (const void *, const void *);
int strCmpFunc(const void *, const void *);
void reduce(int);
>>>>>>> 9f9852741c16638cd9265282125304c3c75f78c0

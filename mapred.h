struct inputList{
    char *data;
    struct inputList *next;
};
struct HashNode{
	int num;
	char *string;
	struct HashNode* next;
};
struct shmNode{
	int taken;
	int index;
	int num;
	char string[2048];
};
struct reduceNode{
	int count;
	char *word;
	int num;
};
struct shmLockNode{
	pthread_mutex_t mutex;
    pthread_cond_t  condition;
};
int main(int , char **);
void readinput();
void mapSetup();
void readInput();
void freeData();
void hashInsert(int, int, char *);
int hashFuncSort(int);
int hashFuncWcount(char *);
void *map(void *);
void marshallHashTable();
void reduceSetup();
int numCmpFunc (const void *, const void *);
int strCmpFunc(const void *, const void *);
void* reduce(void*);

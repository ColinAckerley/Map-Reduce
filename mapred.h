struct inputList{
    char *data;
    struct inputList *next;
};
struct HashNode{
	int num;
	char *string;
	struct HashNode* next;
};
<<<<<<< HEAD
struct shmNode{
	int taken;
	int index;
	int num;
	char string[2048];
};
struct shmLockNode{
	pthread_mutex_t mutex;
    pthread_cond_t  condition;
};
=======
typedef struct wordCount
{
	int count;
	char *word;
	struct wordCount* next;
} wordCount;
>>>>>>> 7e739b3e5336c4a0781a9c193383bd0f2a6d1e39
int main(int , char **);
void readinput();
void mapSetup();
void readInput();
void freeData();
void hashInsert(int, int, char *);
int hashFuncSort(int);
int hashFuncWcount(char *);
void *map(void *);
<<<<<<< HEAD
void marshallHashTable();
=======
void reduceSetup();
int numCmpFunc (const void *, const void *);
int strCmpFunc(const void *, const void *);
void* reduce(void*);

>>>>>>> 7e739b3e5336c4a0781a9c193383bd0f2a6d1e39

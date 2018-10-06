struct inputList{
    char *data;
    struct inputList *next;
};
struct HashNode{
	int num;
	char *string;
	struct HashNode* next;
};
typedef struct wordCount
{
	int count;
	char *word;
	struct wordCount *next;
} wordCount;
int main(int , char **);
void readinput();
void mapSetup();
void readInput();
void freeData();
void hashInsert(int, struct inputList *);
int hashFuncSort(int);
int hashFuncWcount(char *);
void *map(void *);
int numCmpFunc (const void *, const void *);
int strCmpFunc(const void *, const void *);
void reduce(int);


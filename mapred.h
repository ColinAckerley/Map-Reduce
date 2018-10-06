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


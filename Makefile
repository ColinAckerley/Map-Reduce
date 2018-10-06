all:mapred

mapred: mapred.c
	gcc -Wall -Werror -fsanitize=address -pthread -o mapred mapred.c

clean:
	rm -rf mapred

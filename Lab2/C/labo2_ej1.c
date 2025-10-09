#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

int main(void) {

	pid_t child_pid;

	printf("main PPID: %d\n", getppid());
	printf("main PID: %d\n", getpid());

	child_pid = fork();

	if(child_pid != 0) {
		printf("Proceso padre, PID: %d\n", (int) getpid());
		printf("Proceso hijo, PID: %d\n", (int) child_pid);
	} else {
		printf("Ejecución proceso hijo, PID %d\n", (int) getpid());
	}

	sleep(60);

	return EXIT_SUCCESS;
}

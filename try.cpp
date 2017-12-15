#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <bits/stdc++.h>
using namespace std;

/* Write some random text to the pipe. */

void
write_to_pipe (int file)
{
  FILE *stream;
  stream = fdopen (file, "w");
  char buf[1000000];
  setvbuf ( stream , buf , _IOFBF , 100000);
  int x = 0;
  while(x++ < 10000){
    fprintf (stream, "%d\n", rand()%200000000);
    // fflush(stream);
    // usleep(200000);
  }
  std::cout<<"Parent complete\n";
  fclose (stream);
}

int
main (void)
{
  pid_t pid;
  int mypipe[2];

  /* Create the pipe. */
  if (pipe (mypipe))
    {
      fprintf (stderr, "Pipe failed.\n");
      return EXIT_FAILURE;
    }

  /* Create the child process. */
  pid = fork ();
  if (pid == (pid_t) 0)
    {
      puts("child started\n");
      /* This is the child process.
         Close other end first. */
      close (mypipe[1]);
      char str[16];
      sprintf(str, "%d", mypipe[0]);
      execl( "lookup", str, "1", (char*)0 );
      std::cout<<"error in execl\n";
      return EXIT_SUCCESS;
    }
  else if (pid < (pid_t) 0)
    {
      /* The fork failed. */
      fprintf (stderr, "Fork failed.\n");
      return EXIT_FAILURE;
    }
  else
    {
      /* This is the parent process.
         Close other end first. */
      close (mypipe[0]);
      write_to_pipe (mypipe[1]);
      return EXIT_SUCCESS;
    }
}

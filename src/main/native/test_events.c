#include "utils.h"

//----------------------------------------------------------------------------- //
//----------------------------------------------------------------------------- //

int main(int argc, char *argv[]) {
  char *fname = argv[1]; 

  // open file and get its size
  FILE *f = fopen(fname, "r");
  fseek(f, 0, SEEK_END);
  size_t file_size = ftell(f);

  printf("filename = %s (size = %lu bytes)\n", fname, file_size);

  void *buffer = malloc(file_size);

  // fill buffer
  fseek(f, 0, SEEK_SET);
  fread(buffer, file_size, 1, f);
  fclose(f);

  printf("calling get_events:\n");
  char *res = get_events(buffer, file_size, "template", 0, 0);
  //printf("%s\n-- get_events done.\n", res);


  printf("Done.\n");

  return 0;
}

//-----------------------------------------------------------------------------//
//-----------------------------------------------------------------------------//

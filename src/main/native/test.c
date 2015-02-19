#include "utils.h"

//----------------------------------------------------------------------------- //
//----------------------------------------------------------------------------- //

int main(int argc, char *argv[]) {
	char *res, *dir = argv[1];
	char filename[4092];
    DIR *dip;
	struct dirent   *fichero;

	if ((dip = opendir(argv[1])) == NULL) {
			perror("opendir");
			return 0;
	}
	printf("Directory stream is now open\n");

	int numficheros = 0;
	while ((fichero = readdir(dip)) != NULL)
	{

		char *fname = fichero->d_name;

		if (strcmp(fname, ".") == 0 || strcmp(fname, "..") == 0) continue;

		sprintf(filename, "%s/%s", dir, fname);

		numficheros++;

		FILE *f = fopen(filename, "r");

		fseek(f, 0, SEEK_END);

		size_t file_size = ftell(f);

		void *buffer = malloc(file_size);

		// fill buffer
		fseek(f, 0, SEEK_SET);
		fread(buffer, file_size, 1, f);
		fclose(f);

		printf("calling get_fastqs:\n");
		res = get_fastqs(buffer, file_size);
		if (res != NULL) {
			//printf("%s\n-- get_fastqs done.\n", res);
			free(res);
		}

		printf("calling get_info:\n");
		res = get_info(buffer, file_size);
		if (res != NULL) {
			//printf("%s\n--- get_info done.\n", res);
			free(res);
		}

		free(buffer);
	}

	printf("\n\nreaddir() found a total of %i files\n", numficheros);

	if (closedir(dip) == -1)
	{
			perror("closedir");
			return 0;
	}

	printf("\nDirectory stream is now closed\n");
    //method();
	return 0;


/*
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

  printf("calling get_fastqs:\n");
  char *res = get_fastqs(buffer, file_size);
  printf("%s\n-- get_fastqs done.\n", res);
  
  printf("calling get_info:\n");
  res = get_info(buffer, file_size);
  printf("%s\n--- get_info done.\n", res);



  printf("Done.\n");

  return 0;
  */
}

//-----------------------------------------------------------------------------//
//-----------------------------------------------------------------------------//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hdf5.h"
#include "hdf5_hl.h"


//------------------------------------------------------------------------------//

#define NUM_FIELDS 15

typedef struct event {
	double mean;
	double start;
	double stdv;
	double length;
	char model_state[5];
	double model_level;
	int move;
	double p_model_state;
	char mp_state[5];
	double p_mp_state;
	double p_A;
	double p_C;
	double p_G;
	double p_T;
	int raw_index;
} event_t;


//------------------------------------------------------------------------------//

char *get_fastqs(char *file_image, int file_size);
char *get_info(char *file_image, int file_size);
char *get_events(char *file_image, int file_size, char *src, int start_time, int end_time);

//------------------------------------------------------------------------------//
//------------------------------------------------------------------------------//

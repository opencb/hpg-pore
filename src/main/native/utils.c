#include "utils.h"

//----------------------------------------------------------------------------- //
//----------------------------------------------------------------------------- //

void display_datatype(hid_t fid, char *path, char *attr_name) {
	hid_t group = H5Gopen2(fid, path, H5P_DEFAULT);
	hid_t attr = H5Aopen(group, attr_name, H5P_DEFAULT);
	hid_t tid = H5Aget_type(attr);
	size_t buf_size = 1024;
	char buf[1024];
	H5LT_dtype_to_text(tid, buf, H5LT_DDL, &buf_size, 1);
	printf("\t%s\t%s\t%s\n", path, attr_name, buf);
	H5Aclose(attr);
	H5Gclose(group);
}

//----------------------------------------------------------------------------- //

typedef struct nt_counters {
	int numA;
	int numT;
	int numG;
	int numC;
	int numN;
} nt_counters_t;

void nt_counters_init(nt_counters_t *counters) {
	counters->numA = 0;
	counters->numT = 0;
	counters->numG = 0;
	counters->numC = 0;
	counters->numN = 0;
}

void nt_counters_compute(char *seq, size_t length, 
		nt_counters_t *counters) {
	char c;
	for (int i = 0; i < length; i++) {
		c = seq[i];
		if (c == 'A' || c == 'a') {
			counters->numA++;
		} else if (c == 'T' || c == 't') {
			counters->numT++;
		} else if (c == 'G' || c == 'g') {
			counters->numG++;
		} else if (c == 'C' || c == 'c') {
			counters->numC++;
		} else if (c == 'N' || c == 'n') {
			counters->numN++;
		}
	}
}

//----------------------------------------------------------------------------- //

void concat_string(char *key, char *value, char *dest) {
	strcat(dest, key);
	strcat(dest, "\t");
	strcat(dest, value);
	strcat(dest, "\n");
}

void concat_int(char *key, int value, char *dest) {
	char str[1024];
	sprintf(str, "%i", value);

	strcat(dest, key);
	strcat(dest, "\t");
	strcat(dest, str);
	strcat(dest, "\n");
}

void concat_float(char *key, float value, char *dest) {
	char str[1024];
	sprintf(str, "%f", value);

	strcat(dest, key);
	strcat(dest, "\t");
	strcat(dest, str);
	strcat(dest, "\n");
}

void concat_doubleT(float value, char *dest) {
	char str[1024];
	sprintf(str, "%f", value);

	strcat(dest, str);
	strcat(dest, "\t");
}

void concat_intT(int value, char *dest) {
	char str[1024];
	sprintf(str, "%i", value);
	strcat(dest, str);
}
//----------------------------------------------------------------------------- //

char *get_seq(char *fastq) {
	const char s[2] = "\n";
	char *token;

	token = strtok(fastq, s);
	while (token != NULL) {
		token = strtok(NULL, s);
		return token;
	}
}

//----------------------------------------------------------------------------- //

void concat_seq_values(char *fastq, char *dest) {
	if (fastq) {
		char *seq = get_seq(fastq);

		if (!seq) goto empty_seq;

		concat_int("seq_length", strlen(seq), dest);

		nt_counters_t nt_counters;
		nt_counters_init(&nt_counters);
		nt_counters_compute(seq, strlen(seq), &nt_counters);
		concat_int("A", nt_counters.numA, dest);
		concat_int("T", nt_counters.numT, dest);
		concat_int("G", nt_counters.numG, dest);
		concat_int("C", nt_counters.numC, dest);
		concat_int("N", nt_counters.numN, dest);

		return;
	}

	empty_seq:
	concat_string("seq_length", "0", dest);
	concat_string("A", "0", dest);
	concat_string("T", "0", dest);
	concat_string("G", "0", dest);
	concat_string("C", "0", dest);
	concat_string("N", "0", dest);
}
//----------------------------------------------------------------------------- //



char *get_string_attr(hid_t fid, char *path, char *attr_name) {
  //  display_datatype(fid, path, attr_name);

  char *attr_data = NULL;

  hid_t group = H5Gopen2(fid, path, H5P_DEFAULT);
  if (group < 0) {
    printf("%s does not exist!!!\n", path);
    attr_data = (char *) calloc(2, sizeof(char));
    goto attr_string_done;
  }

  hid_t attr = H5Aopen(group, attr_name, H5P_DEFAULT);
  if (attr < 0) {
    printf("%s does not exist for %s!!!\n", attr_name, path);
    attr_data = (char *) calloc(2, sizeof(char));
    goto attr_string_done;
  }

  hsize_t attr_size = H5Aget_storage_size(attr);

  attr_data = (char *) calloc(attr_size + 1, sizeof(char));

  H5LTget_attribute_string(fid, path, attr_name, attr_data);

attr_string_done:

  if (attr > 0) H5Aclose(attr);
  if (group > 0) H5Gclose(group);

  return attr_data;
  free(attr_data);
}

//----------------------------------------------------------------------------- //

char *get_dataset(hid_t fid, char *path, size_t *dataset_size) {
	char *res;
	int size = 0;


	// dataset
	hid_t dset_id = H5Dopen2(fid, path, H5P_DEFAULT);
	if (dset_id < 0) {
		//printf("Error: opening dataset %s\n", path);
		*dataset_size = 0;
		return NULL;
	}

	hsize_t data_size = H5Dget_storage_size(dset_id);

	hid_t mt_id = H5Dget_type(dset_id);
	res = (char *) malloc(data_size * sizeof(char));
	if (H5Dread(dset_id, mt_id, H5S_ALL, H5S_ALL, H5P_DEFAULT, res) < 0) {
		//printf("Error reading dataset\n");
		free(res);
		*dataset_size = 0;
		return NULL;
	}

	*dataset_size = data_size;

	// closing...
	if (dset_id > 0) H5Dclose(dset_id);

	return res;
}


//----------------------------------------------------------------------------- //

void concat_float_attr(hid_t fid, char *path, char *attr_name, char *dest) {
	//  display_datatype(fid, path, attr_name);

	char attr_data[1024] = "";

	hid_t group = H5Gopen2(fid, path, H5P_DEFAULT);
	if (group < 0) {
		printf("%s does not exist!!!\n", path);
		goto float_done;
	}

	hid_t attr = H5Aopen(group, attr_name, H5P_DEFAULT);
	if (attr < 0) {
		printf("%s does not exist for %s!!!\n", attr_name, path);
		goto float_done;
	}


	float value;

	H5LTget_attribute_float(fid, path, attr_name, &value);

	sprintf(attr_data, "%f", value);

	float_done:
	concat_string(attr_name, attr_data, dest);

	// closing...
	if (attr > 0) H5Aclose(attr);
	if (group > 0) H5Gclose(group);

}

//----------------------------------------------------------------------------- //

void concat_int_attr(hid_t fid, char *path, char *attr_name, char *dest) {
	//  display_datatype(fid, path, attr_name);

	char attr_data[1024] = "";

	hid_t group = H5Gopen2(fid, path, H5P_DEFAULT);
	if (group < 0) {
		printf("%s does not exist!!!\n", path);
		goto int_done;
	}

	hid_t attr = H5Aopen(group, attr_name, H5P_DEFAULT);
	if (attr < 0) {
		printf("%s does not exist for %s!!!\n", attr_name, path);
		goto int_done;
	}

	int value;

	H5LTget_attribute_int(fid, path, attr_name, &value);

	sprintf(attr_data, "%i", value);

	int_done:
	concat_string(attr_name, attr_data, dest);

	// closing...
	if (attr > 0) H5Aclose(attr);
	if (group > 0) H5Gclose(group);

}

//----------------------------------------------------------------------------- //

void concat_string_attr(hid_t fid, char *path, char *attr_name, char *dest) {
	//  display_datatype(fid, path, attr_name);

	char *attr_data;

	hid_t group = H5Gopen2(fid, path, H5P_DEFAULT);
	if (group < 0) {
		printf("%s does not exist!!!\n", path);
		attr_data = (char *) calloc(2, sizeof(char));
		goto string_done;
	}

	hid_t attr = H5Aopen(group, attr_name, H5P_DEFAULT);
	if (attr < 0) {
		printf("%s does not exist for %s!!!\n", attr_name, path);
		attr_data = (char *) calloc(2, sizeof(char));
		goto string_done;
	}

	hsize_t attr_size = H5Aget_storage_size(attr);

	attr_data = (char *) calloc(attr_size + 1, sizeof(char));

	H5LTget_attribute_string(fid, path, attr_name, attr_data);

	string_done:
	concat_string(attr_name, attr_data, dest);

	free(attr_data);

	// closing...
	if (attr > 0) H5Aclose(attr);
	if (group > 0) H5Gclose(group);
}

//----------------------------------------------------------------------------- //

char *get_info(char *file_image, int file_size) {

	/* Turn off error handling */
	H5Eset_auto(H5E_DEFAULT, NULL, NULL);

	// open file image
	int flags = H5LT_FILE_IMAGE_DONT_COPY & H5LT_FILE_IMAGE_DONT_RELEASE;
	hid_t fid = H5LTopen_file_image(file_image, file_size, flags);

	if (fid < 0) {
		printf("Error: unable to open image file\n");
		return NULL;
	}

	size_t alloc_info_size = 8092;
	char *info = (char *) calloc(alloc_info_size, sizeof(char));

	// commons attributes
	concat_string_attr(fid, "/Analyses/Basecall_2D_000", "name", info);
	concat_string_attr(fid, "/Analyses/Basecall_2D_000", "time_stamp", info);
	concat_string_attr(fid, "/Analyses/Basecall_2D_000", "version", info);

	concat_int_attr(fid, "/UniqueGlobalKey/channel_id", "channel_number", info);

	concat_string_attr(fid, "/UniqueGlobalKey/tracking_id", "asic_id", info);
	concat_string_attr(fid, "/UniqueGlobalKey/tracking_id", "asic_temp", info);
	concat_string_attr(fid, "/UniqueGlobalKey/tracking_id", "device_id", info);
	concat_string_attr(fid, "/UniqueGlobalKey/tracking_id", "exp_script_purpose", info);
	concat_string_attr(fid, "/UniqueGlobalKey/tracking_id", "exp_start_time", info);
	concat_string_attr(fid, "/UniqueGlobalKey/tracking_id", "flow_cell_id", info);
	concat_string_attr(fid, "/UniqueGlobalKey/tracking_id", "heatsink_temp", info);
	concat_string_attr(fid, "/UniqueGlobalKey/tracking_id", "run_id", info);
	concat_string_attr(fid, "/UniqueGlobalKey/tracking_id", "version_name", info);

	size_t size_te, size_co, size_2d;
	char *fastq_te, *fastq_co, *fastq_2d;

	fastq_te = get_dataset(fid, "/Analyses/Basecall_2D_000/BaseCalled_template/Fastq", &size_te);
	fastq_co = get_dataset(fid, "/Analyses/Basecall_2D_000/BaseCalled_complement/Fastq", &size_co);
	fastq_2d = get_dataset(fid, "/Analyses/Basecall_2D_000/BaseCalled_2D/Fastq", &size_2d);

	size_t info_size = strlen(info);
	size_t aux = (size_te + size_co + size_2d) / 2;


	if (info_size + aux >= alloc_info_size) {
		alloc_info_size = info_size + aux + 512;
		char *aux = (char *) calloc(alloc_info_size, sizeof(char));
		memcpy(aux, info, info_size);
		free(info);
		info = aux;
	}

	// template
	if (fastq_te) {
		concat_string("-te", "", info);
		concat_float_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_1d_template", "mean_qscore", info);
		concat_int_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_1d_template", "called_events", info);
		concat_int_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_1d_template", "num_events", info);

		//concat_seq_values(fastq_te, info);
		free(fastq_te);
	}

	// complement
	if (fastq_co) {
		concat_string("-co", "", info);
		concat_float_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_1d_complement", "mean_qscore", info);
		concat_int_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_1d_complement", "called_events", info);
		concat_int_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_1d_complement", "num_events", info);

		//concat_seq_values(fastq_co, info);
		free(fastq_co);
	}

	// 2D
	if (fastq_2d) {
		concat_string("-2d", "", info);
		concat_float_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_2d", "mean_qscore", info);
		concat_int_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_2d", "sequence_length", info);

		//concat_seq_values(fastq_2d, info);
		free(fastq_2d);
	}

	// close file image
	H5Fclose(fid);

	return info;
}

//----------------------------------------------------------------------------- //

char *get_fastqs(char *file_image, int file_size) {

  /* Turn off error handling */
	H5Eset_auto(H5E_DEFAULT, NULL, NULL);

  // open file image
  int flags = H5LT_FILE_IMAGE_DONT_COPY & H5LT_FILE_IMAGE_DONT_RELEASE;
  hid_t fid = H5LTopen_file_image(file_image, file_size, flags);

  if (fid < 0) {
    printf("Error: unable to open image file\n");
    return NULL;
  }

  size_t size_te, size_co, size_2d, size;
  char *fastq_te, *fastq_co, *fastq_2d, *fastq;

  char* run_id = get_string_attr(fid, "/UniqueGlobalKey/tracking_id", "run_id");
  int run_id_len = strlen(run_id);

  fastq_te = get_dataset(fid, "/Analyses/Basecall_2D_000/BaseCalled_template/Fastq", &size_te);
  fastq_co = get_dataset(fid, "/Analyses/Basecall_2D_000/BaseCalled_complement/Fastq", &size_co);
  fastq_2d = get_dataset(fid, "/Analyses/Basecall_2D_000/BaseCalled_2D/Fastq", &size_2d);

  size = size_te + size_co + size_2d + run_id_len + 100;
  fastq = (char *) calloc(size, sizeof(char));

  char *p = fastq;
  if (fastq_te) {
    memcpy(p, run_id, run_id_len);
	p += run_id_len;
    memcpy(p, "-te\n", 4);
    p += 4;
    memcpy(p, fastq_te, size_te);
    p += size_te;
    free(fastq_te);
  }
  if (fastq_co) {
    memcpy(p, run_id, run_id_len);
	p += run_id_len;
    memcpy(p, "-co\n", 4);
    p += 4;
    memcpy(p, fastq_co, size_co);
    p += size_co;
    free(fastq_co);
  }
  if (fastq_2d) {
    memcpy(p, run_id, run_id_len);
	p += run_id_len;
    memcpy(p, "-2D\n", 4);
    p += 4;
    memcpy(p, fastq_2d, size_2d);
    p += size_2d;
    free(fastq_2d);
  }

  free(run_id);

  // close file image
  H5Fclose(fid);

  return fastq;
}

//----------------------------------------------------------------------------- //

// src = complement, template,2d
// if start_time and end_time are 0, all events



size_t event_offsets[NUM_FIELDS] = { HOFFSET(event_t, mean),
                                     HOFFSET(event_t, start),
                                     HOFFSET(event_t, stdv),
                                     HOFFSET(event_t, length),
                                     HOFFSET(event_t, model_state),
                                     HOFFSET(event_t, model_level),
                                     HOFFSET(event_t, move),
                                     HOFFSET(event_t, p_model_state),
                                     HOFFSET(event_t, mp_state),
                                     HOFFSET(event_t, p_mp_state),
                                     HOFFSET(event_t, p_A),
                                     HOFFSET(event_t, p_C),
                                     HOFFSET(event_t, p_G),
                                     HOFFSET(event_t, p_T),
                                     HOFFSET(event_t, raw_index) };

size_t event_sizes[NUM_FIELDS] = { sizeof(double),
                                    sizeof(double),
                                    sizeof(double),
                                    sizeof(double),
                                    sizeof(5 * sizeof(char)),
                                    sizeof(double),
                                    sizeof(int),
                                    sizeof(double),
                                    sizeof(5 * sizeof(char)),
                                    sizeof(double),
                                    sizeof(double),
                                    sizeof(double),
                                    sizeof(double),
                                    sizeof(double),
                                    sizeof(int)};

char *get_events(char *file_image, int file_size, char *src, int start_time, int end_time) {

	/* Turn off error handling */
	H5Eset_auto(H5E_DEFAULT, NULL, NULL);

	// open file image
	int flags = H5LT_FILE_IMAGE_DONT_COPY & H5LT_FILE_IMAGE_DONT_RELEASE;
	hid_t fid = H5LTopen_file_image(file_image, file_size, flags);

	if (fid < 0) {
		printf("Error: %s: unable to open image file\n", __func__);
		return NULL;
	}

	herr_t     status;
	hsize_t    num_cols;
	hsize_t    num_rows;


	char *path;
	if (!strcmp (src,"template")) {
		path = "/Analyses/Basecall_2D_000/BaseCalled_template/Events";

	} else if (!strcmp (src,"2D")) {
		path = "/Analyses/Basecall_2D_000/BaseCalled_2D/Events";

	} else if (!strcmp (src,"complement")) {
		path = "/Analyses/Basecall_2D_000/BaseCalled_complement/Events";

	}
	/* check if the file exist*/
	/*H5G_loc_t	 loc;
	H5G_loc_t	 dset_loc;
	H5G_loc_reset(&dset_loc);
	H5TRACE3("i", "i*si", fid, path, H5P_DATASET_ACCESS_DEFAULT);
	if(H5G_loc_find(&loc, path, &dset_loc, H5P_DATASET_ACCESS_DEFAULT, H5AC_dxpl_id) < 0){
		printf ("paso por aqui dentro H5AC... \n");
		return NULL;
	}*/
	/*hid_t did = H5I_BADID;
	if (( did = H5Dopen2 (fid, path, H5P_DEFAULT))<0){

		return NULL;
	}*/

	/* Get table info  */
	if((status = H5TBget_table_info (fid, path, &num_cols, &num_rows))<0){
		return NULL;
	}

	printf("Table %s has %d cols and %d rows\n",path, (int)num_cols, (int)num_rows);


	size_t size;
	event_t eventsTable[num_rows];

	/*Get events table*/
	status = H5TBread_table(fid, path, sizeof(event_t), event_offsets, event_sizes, eventsTable);

	char *events = (char *) calloc(sizeof(event_t)*num_rows*2, sizeof(char));
	if( start_time > end_time) {
		//I think the people not is a retard people, I think they has wrong
		int aux = start_time;
		start_time = end_time;
		end_time = aux;
	}
	int final = 0;
	int principio = 0;
	if(end_time !=0){
		double start = eventsTable[0].start;

		if(end_time > eventsTable[num_rows-1].start -start){
			final = eventsTable[num_rows-1].start-start;
		}else{
			final = start + end_time;
		}
		if (start_time > 0){
			start = start + start_time;
			if((eventsTable[num_rows-1].start) > start){//check if final is higth than start-time
				while ( eventsTable[principio].start < start){
					principio++;
				}
			}else{
				return NULL;
			}

		}

	}else {
		final = eventsTable[num_rows-1].start;
	}
	char *p = events;
	char *cabecera = "mean\tstart\tstdv\tlength\tmodel_state\tmodel_level\tmove\tp_model_state\tmp_state\tp_mp_state\tp_A\tp_C\tp_G\tp_G\tp_T\traw_index\n";
	memcpy(p, cabecera, 200);

	int i = principio;
	while (eventsTable [i].start <= final){
		concat_doubleT(eventsTable[i].mean,p);
		concat_doubleT(eventsTable[i].start,p);
		concat_doubleT(eventsTable[i].stdv,p);
		concat_doubleT(eventsTable[i].length,p);
		strncat(p, eventsTable[i].model_state, 5);
		strcat(p, "\t");
		concat_doubleT(eventsTable[i].model_level,p);
		concat_intT(eventsTable[i].move,p);
		strcat(p, "\t");
		concat_doubleT(eventsTable[i].p_model_state,p);
		strncat(p, eventsTable[i].mp_state, 5);
		strcat(p, "\t");
		concat_doubleT(eventsTable[i].p_mp_state,p);
		concat_doubleT(eventsTable[i].p_A,p);
		concat_doubleT(eventsTable[i].p_C,p);
		concat_doubleT(eventsTable[i].p_G,p);
		concat_doubleT(eventsTable[i].p_T,p);
		concat_intT(eventsTable[i].raw_index,p);
		strcat(p, "\n");
		i++;
	}

	//printf("size = %i, strlen = %i\n", sizeof(event_t)*num_rows, strlen(p));


	/*char *p = events;
	if (events_te) {
		memcpy(p, "-te\n", 3);
		p += 3;
		memcpy(p, events_te, size_te);
		p += size_te;
		free(events_te);
	}
	if (events_co) {
		memcpy(p, "-co\n", 3);
		p += 3;
		memcpy(p, events_co, size_co);
		p += size_co;
		free(events_co);
	}*/

	// close file image
	H5Fclose(fid);

	return events;
}

//----------------------------------------------------------------------------- //
//----------------------------------------------------------------------------- //

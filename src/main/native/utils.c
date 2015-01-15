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

char *get_dataset(hid_t fid, char *path, size_t *dataset_size) {
  char *res;
  int size = 0;

  // dataset
  hid_t dset_id = H5Dopen2(fid, path, H5P_DEFAULT);
  if (dset_id < 0) {
    printf("Error: opening dataset %s\n", path);
    *dataset_size = 0;
    return NULL;
  }

  hsize_t data_size = H5Dget_storage_size(dset_id);

  hid_t mt_id = H5Dget_type(dset_id);
  res = (char *) malloc(data_size * sizeof(char));
  if (H5Dread(dset_id, mt_id, H5S_ALL, H5S_ALL, H5P_DEFAULT, res) < 0) {
    printf("Error reading dataset\n");
    free(res);
    *dataset_size = 0;
    return NULL;
  } 

  *dataset_size = data_size;
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

  if (attr > 0) H5Aclose(attr);
  if (group > 0) H5Gclose(group);
}

//----------------------------------------------------------------------------- //

char *get_info(char *file_image, int file_size) {
  // open file image
  int flags = H5LT_FILE_IMAGE_DONT_COPY & H5LT_FILE_IMAGE_DONT_RELEASE;
  hid_t fid = H5LTopen_file_image(file_image, file_size, flags);

  if (fid < 0) {
    printf("Error: unable to open image file\n");
    return NULL;
  }

  char info[8092] = "";

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

  size_t dataset_size;
  char *fastq;

  // template
  fastq = get_dataset(fid, "/Analyses/Basecall_2D_000/BaseCalled_template/Fastq", &dataset_size);
  if (fastq) {
    concat_string("-te", "", info);
    concat_float_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_1d_template", "mean_qscore", info);
    concat_int_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_1d_template", "called_events", info);
    concat_int_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_1d_template", "num_events", info);

    concat_seq_values(fastq, info);
    if (fastq) free(fastq);
  }
  
  // complement
  fastq = get_dataset(fid, "/Analyses/Basecall_2D_000/BaseCalled_complement/Fastq", &dataset_size);
  if (fastq) {
    concat_string("-", "complement", info);
    concat_float_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_1d_complement", "mean_qscore", info);
    concat_int_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_1d_complement", "called_events", info);
    concat_int_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_1d_complement", "num_events", info);

    concat_seq_values(fastq, info);
    if (fastq) free(fastq);
  }

  // 2D
  fastq = get_dataset(fid, "/Analyses/Basecall_2D_000/BaseCalled_2D/Fastq", &dataset_size);
  if (fastq) {
    concat_string("-", "2d", info);
    concat_float_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_2d", "mean_qscore", info);
    concat_int_attr(fid, "/Analyses/Basecall_2D_000/Summary/basecall_2d", "sequence_length", info);

    concat_seq_values(fastq, info);
    if (fastq) free(fastq);
  }

  return info;
}

//----------------------------------------------------------------------------- //

char *get_fastqs(char *file_image, int file_size) {

  // open file image
  int flags = H5LT_FILE_IMAGE_DONT_COPY & H5LT_FILE_IMAGE_DONT_RELEASE;
  hid_t fid = H5LTopen_file_image(file_image, file_size, flags);

  if (fid < 0) {
    printf("Error: unable to open image file\n");
    return NULL;
  }

  size_t size_te, size_co, size_2d, size;
  char *fastq_te, *fastq_co, *fastq_2d, *fastq;

  fastq_te = get_dataset(fid, "/Analyses/Basecall_2D_000/BaseCalled_template/Fastq", &size_te);
  fastq_co = get_dataset(fid, "/Analyses/Basecall_2D_000/BaseCalled_complement/Fastq", &size_co);
  fastq_2d = get_dataset(fid, "/Analyses/Basecall_2D_000/BaseCalled_2D/Fastq", &size_2d);

  size = size_te + size_co + size_2d + 100;
  fastq = (char *) calloc(size, sizeof(char));

  char *p = fastq;
  if (fastq_te) {
    memcpy(p, "-te\n", 3);
    p += 3;
    memcpy(p, fastq_te, size_te);
    p += size_te;
    free(fastq_te);
  }
  if (fastq_co) {
    memcpy(p, "-co\n", 3);
    p += 3;
    memcpy(p, fastq_co, size_co);
    p += size_co;
    free(fastq_co);
  }
  if (fastq_2d) {
    memcpy(p, "-2d\n", 3);
    p += 3;
    memcpy(p, fastq_2d, size_2d);
    p += size_2d;
    free(fastq_2d);
  }

  return fastq;
}


//----------------------------------------------------------------------------- //
//----------------------------------------------------------------------------- //

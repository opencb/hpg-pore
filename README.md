
Hadoop-pore README

*****************************************************
*   Building the libopencb_pore.so                  *
*   Folder: src/main/native                         *
*****************************************************

-----------------------------------------------------
  RUN TEST
-----------------------------------------------------

1. Build hdf5 libraries (libhdf5.a and libhdf5_hl.a)

  $ cd src/main/third-party/hdf5-1.8.14/
  $ ./configure
  $ make

2. Build the test

  $ cd ../../native
  $ gcc -std=gnu99 utils.c test.c -o test -I . -I ../third-party/hdf5-1.8.14/src/ -I ../third-party/hdf5-1.8.14/hl/src/ ../third-party/hdf5-1.8.14/hl/src/.libs/libhdf5_hl.a ../third-party/hdf5-1.8.14/src/.libs/libhdf5.a -lm -lz -ldl

3. Exectute the test

   $ ./test LomanLabz_PC_E.coli_MG1655_ONI_3058_1_ch12_file24_strand.fast5


-----------------------------------------------------
  BUILD DYNAMIC LIBRARY (libopencb_pore.so)
-----------------------------------------------------

1. Renames some files that contain a function 'main', these files must be excluded

   $ mv ../third-party/hdf5-1.8.14/src/H5make_libsettings.c ../third-party/hdf5-1.8.14/src/H5make_libsettings.c.BACKUP
   $ mv ../third-party/hdf5-1.8.14/src/H5detect.c ../third-party/hdf5-1.8.14/src/H5detect.c.BACKUP

2. Build the libopencb_pore.so (you need to set the variable JAVA_HOME)

   $ gcc -std=gnu99 -D_LARGEFILE64_SOURCE=1 org_opencb_hadoop_pore_NativePoreSupport.c utils.c ../third-party/hdf5-1.8.14/src/*.c ../third-party/hdf5-1.8.14/hl/src/*.c -shared -fPIC -o libopencb_pore.so -I . -I $JAVA_HOME/include/ -I $JAVA_HOME/include/linux/ -I ../third-party/hdf5-1.8.14/src/ -I ../third-party/hdf5-1.8.14/hl/src/


*****************************************************
*   Building the hadoop-pore.jar                    *
*                                                   *
*****************************************************

Dependencies: maven, hadoop

1. Compile the hadoop-pore.jar



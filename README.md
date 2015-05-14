HPG Pore README

Welcome to HPG Pore !

HPG Pore is a toolkit to explore and analyze **nanopore sequencing data** that can run both on a single computer and on the Hadoop distributed computing framework.

HPG Pore allows efficient management of huge amounts of data, thus constituting a practical solution for the near future as well as a promising model for the development of new tools to deal with genomic big data.

CONTACT
------- 
  You can contact any of the following developers:

    * Joaquín Tárraga (jtarraga@gmail.es)
    * Susi Gallego (sgaort@gmail)
    * Ignacio Medina (igmecas@gmail.es)

REQUIREMENTS
-------------

To build the HPG Pore application you need to install in your system:

    * Java
    * Maven
    * gcc
    * zlib
    * Hadoop (if you want HPG Pore to run on a Hadoop cluster)


DOWNLOAD and BUILDING
---------------------

  HPG Pore has been opened to the community and released in GitHub, so you can download by invoking the following commands:

    $ git clone https://github.com/opencb/hpg-pore.git
  
  Finally, use the script build.sh to build the HPG Pore application:

    $ cd hpg-pore
    $ ./build.sh

RUNING
-------

  For command line options invoke:

    $ ./hpg-pore.sh -h
    

A tutorial and further documentation are available at http://github.com/opencb/hpg-pore/wiki

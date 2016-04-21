#KassandraMRHelper

[![Build Status](https://travis-ci.org/Knewton/KassandraMRHelper.svg)](https://travis-ci.org/Knewton/KassandraMRHelper)
[![Coverage Status](https://coveralls.io/repos/Knewton/KassandraMRHelper/badge.svg?branch=master)](https://coveralls.io/r/Knewton/KassandraMRHelper?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.knewton.mapreduce/KassandraMRHelper/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.knewton.mapreduce/KassandraMRHelper/)

##Short Summary
The KassandraMRHelper library provides necessary Record Readers, InputFormats 
and Mapper classes to help you with the process of reading data directly from 
Cassandra SSTables. By using this library you will avoid the sstable2json step. 
This library does not require a live Cassandra cluster.

KassandraMRHelper is compatible with Cassandra versions 2.x.

Relevant [blog post](http://www.knewton.com/tech/blog/2013/11/cassandra-and-hadoop-introducing-the-kassandramrhelper)

##Building the Project
To build the example you can just run

`mvn clean package`

However there's three other maven profiles that may be of interest to you. 
The default one is set for EMR but there's a HadoopMapReduce (for running the 
example in a regular non EMR Hadoop cluster) and a local option for running it 
in eclipse or on a local Hadoop cluster.

Choosing a profile will write a property to knewton-site.xml named 
"com.knewton.mapreduce.environment" and you can then access it from the 
configuration and using the MREnvironment enum type.

To select a profile you can do:

`mvn clean package -P HadoopMapReduce`

##Usage
You can use com.knewton.mapreduce.cassandra.WriteSampleSSTable in the test 
source packages to generate a sample SSTable with student events to use as 
input. To run it from the command line you can use:

```bash
java -cp ./KassandraMRHelper-0.1.jar:./KassandraMRHelper-0.1-tests.jar \
	com.knewton.mapreduce.cassandra.WriteSampleSSTable
```

```bash
usage: WriteSampleSSTable [OPTIONS] <output_dir>
 -e,--studentEvents <arg>   The number of student events per student to be
                            generated. Default value is 10
 -h,--help                  Prints this help message.
 -s,--students <arg>        The number of students (rows) to be generated.
                            Default value is 100.
```
##Things To Watch Out For
1.	There's a property in knewton-site.xml named com.knewton.cassandra.backup.compression. 
	You should set this to true only if you are reading from a backup SSTable
	location that has extra snappy compression on top of any cassandra compression
	scheme. If you're using Priam, for example, and have enabled compression in 
	Cassandra then your tables are probably double compressed. You DO NOT need to set
	this property to true if you are using only the Cassandra compression since the 
	library will auto detect that by the presence of the CompressionInfo.db file.

##Contributing
Contributions are always welcome and encouraged!

If you would like to contribute to this project, please contact the current
project maintainer, or use the Github pull request feature.

The project maintainer is [Giannis Neokleous](https://github.com/gneokleo)

You can find the style files [here](https://github.com/Knewton/KnewtonStyles)

###Future Work
1.  Add SSTable RecordWriters and OutputFormats. This will directly write the
    SSTables without the need of having a live cluster.
    * Create cluster partitioners for partitioning keys based on the
        Cassandra ring topology.

##Author
Giannis Neokleous		

www.giann.is

## License
Licensed under the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0.html). See the LICENSE file for more details.

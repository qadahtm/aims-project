# AIMS Tools

## Workload Generation

There are 10 transaction classes. Instances of each class will randomly read a tuple and update another tuple but within the range of data objects associated with the class. 

### Limitations:
* Currently, the implementation is very limited and it expects Posgresql running on localhost:5432 and a database called "tpcc". 
* I am not handling the case a transaction instance will try to access a non-existant tuple which will throw an exception and stop the workload run process. In this case simply re-run the command. This expected as the original data tuples are generated randomly to begin with. This should be fiexed soon.
* Testing: I tested the current version of the code with 50K data objects and 50K workload. 

### Getting source code

You need git to be installed and configured proparly.

```sh
$ git clone https://github.com/qadahtm/aims-project ; cd aims-project
```

### Usage procedure
Most probably you want to run the following in sequence.

1. To build the project:
```sh
$ ./sbt pack
```

2. To create a table for the data:

```sh
$ ./target/pack/bin/DemoWorkloadDriver init
```

3. Load data object into the database:

```sh
$ ./target/pack/bin/DemoWorkloadDriver load 50000
```

The above command will load 50K data objects into the database, in a table called "randomdata"

4. Run random workload. 

```sh
$ ./target/pack/bin/DemoWorkloadDriver run 100
```

The above command will run 10 transaction instances from each transaction class. The number must be a multiple of 10 as it will be equally distributed among the transaction classes.

## Tested OS
MAC OSX, and Ubuntu Linux
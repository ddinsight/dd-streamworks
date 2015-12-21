

## CLI TOOL for dd-streamworks

The management of tasks in dd-streamworks is done through a CLI tool. The CLI tool has following features.
- Task module generation, deletion, upload and download
- Modification of task configuration
- Task reallocation on workers
- Service configuration of workers that are saved on zookeeper

### Setup
CLI tool is included in the distribution of stream_worker source code of dd-streamworks
In the usual setup where stream_worker is running in its usual configuration, no additional packages need to be installed but in the case that CLI tool is used alone, the following additional packages should be installed.
```
pip install pymongo kazoo
```

The addresses of both zookeeper and mongodb server should be configured since the CLI tool uses both.

```
export OW_TASKMODULE=mongodb://<user>:<password>@<host>/<db>
export OW_ZOOKEEPER=<zookeeper url>
```

### Command

####Obtain task module list
```
ocadm list [<module name>, ...]
```

This prints the list of modules that are uploaded to the server. Module name parameter defines the operation as follows.
- Without module name, the list of modules is printed
- With module name set to `all` , in addition to the module list, task information of task information of each module is printed
- With module name set to the list of these modules, the list of modules that are contained in the list is printed as well as task information for each module.

####Generate task module template

```
ocadm create -d ../devmodule/development <module name>
```

generates module template in the directory with the same name as the module name. The directory resides in stream_worker/devmodule/development.
If a worker is executed in development mode, it automatically loads the module in oceanworker/devmodule/development and allocates one task slot. Hence, for testing, you don't need to upload the module under development to the server and allocate a worker for it.

#### Task module upload

```
ocadm upload -d ../devmodule/development -w <worker slot 수> <module name>
```

Uploads a task module to a server. With -w option, the number of default worker slots of all the tasks that are included in the module can be configured.
The default number is used if the slot number is not specified. The defaults are as follows.
- A module that is registered for the first time : 0
- An updated module that has already been registered : the same as the original module

#### Task module deletion

```
ocadm remove <module name>
```

Deletes a task module registered on the server. The slot that had been allocated to the task is automatically returned as a free slot.

#### Task configuration change

```
ocadm task <task name> -w <worker slot>
```

Configures the number of worker slots that are to be allocated to each task.

#### Task reallocation

```
ocadm rebalance [<task name>, ...]
```

Reallocates tasks to workers. This is usually used to scale-out worker processes after they are added for execution.

With task name set to `all`, all the tasks but **consistent-hashing tasks** are reallocated.
In the case that `rebalance all` has been performed, consistent-hashing task should be rebalanced separately.

#### Obtain service config information
```
ocadm getconfig <service name> <filename>
```

Reads configuration info saved in zookeeper.
Currently, the service supports the following two.
- `henem`
- `oceanworker`

Without a specified filename, the console is used for output.


#### Configuration of service config information
```
ocadm setconfig <service name> <filename>
```

Update the configuration data on zookeeper.
The parameter is the same as `getconfig`.


---
Copyright 2015 AirPlug Inc.

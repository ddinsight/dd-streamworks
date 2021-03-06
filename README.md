

dd-streamworks
===

dd-streamworks is the stream data processing part of DD Insight. This sub-project includes two main parts. One resides on the job_server directory including job server engine that is capable of  runtime task balancing and the other on the stream_worker directory including sample workers and modules that are needed for real-time analysis of DD Insight

**Features**
> - A server framework for real-time data processing
> - Ability to scale-out in runtime
> - Load distribution by runtime task balancing (with zero down-time)
> - Run time task deployment ( add / update / delete of a module )
> - Runtime reconfiguration ( interoperated with zookeeper )
> - Pub/sub-based messaging architecture and consistent-hashing supported


### 
**Getting Started**
> Run a docker script with the files in ['dd-streamworks/job_server/docker']() directory

### 
**Developers Guide**
> For further information on how to use CLI for task module upload, delete, assignment of the number of workers on each task, check [Streamworks CLI User Guide](http://github.com/ddinsight/dd-streamworks) document.

**Note**
#### *Information Regarding Cell/Wi-Fi Location*
The locations for cell/Wi-Fi stations that are used in the map-based analytics tools can be constructed as follows. Note that there are various ways to do this for both methods of offline update and stream update.
> - [Offline] Import pre-built location databases
>  * Network operator's own station databases
>  * Open databases in the public domain (eg. opencellid.org) 
> - [Stream] Compute from the data reported by mobile devices. 
>  * WPS with CellID & BSSID
>  * Simple approximation by moving-average with device location reported by Android
> - [Offline/Stream] Utilize API services for geolocation 
>  * Google geolocation API with CellID or BSSID from the information reported by the device
> - etc.

---
### 
**Authors**
> - Spring Choi (Job Server, Workers)
> - Jay Lee (Workers)

### 
**Contributors**
> - [See contributors on Github](http://)

### 
**License**
> dd-streamworks is released under [Apache v2 License](http://)


### 

----------

Copyright 2015 AirPlug Inc.
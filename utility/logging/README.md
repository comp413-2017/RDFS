# Starting with the logger
For each executable using this framework, you must call the macro "INITIALIZE_EASYLOGGINGPP"  
directly after you include easylogging++ for the first time. This can only be done once! So just  
do it in "main.cc" files. Then, every file linked with e.g. "main.cc" just #includes easylogging++.h  
and can use the logging stuff as necessary.  

System-level configurations are turned on by #defining constants before including easylogging for the  
first time. Again, this should be done in a main cc file. There are a bunch of configurations,  
but the ones relevant to us are ELPP_FRESH_LOG_FILE (truncate or append to the log file), and  
ELPP_THREAD_SAFE. Thread safety is off by default. If you're building a threaded executable,  
be sure to define the latter macro. The first isn't as important, but I went ahead and defined  
that for namenode.

# Logging handles
- Global: 	Generic level that represents all levels. Useful when setting global configuration for all levels.  
- Trace: 	Information that can be useful to back-trace certain events - mostly useful than debug logs.  
- Debug: 	Informational events most useful for developers to debug application. Only applicable if NDEBUG is not defined (for non-VC++) or _DEBUG is defined (for VC++).  
- Fatal: 	Very severe error event that will presumably lead the application to abort.  
- Error: 	Error information but will continue application to keep running.  
- Warning: 	Information representing errors in application but application will keep running.  
- Info: 	Mainly useful to represent current progress of application.  
- Verbose: 	Information that can be highly useful and vary with verbose logging level. Verbose logging is not applicable to hierarchical logging.  
- Unknown: 	Only applicable to hierarchical logging and is used to turn off logging completely.  

Check out config/nn-log-conf.conf and config/dn-log-conf.conf to see how the different logger options  
are exercised on a per-handle basis. 

# Using the logger
After all of this configuration, etc., using the logger is as simple as including the header file  
and doing:  
```
LOG(HANDLE) << "My log message"; //(no endl necessary).  
```
where HANDLE is one of the ones above (uppercase, so INFO, ERROR, etc. Important note: abort() is  
called when you log to the  FATAL handle. I don't know that we'll ever want to use this handle  
(unless there truly is a logged  event at which we want to shut everything down)  

# Further Information
https://github.com/easylogging/easyloggingpp
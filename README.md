# FireX Flame

Flame is a lighweight server that collects, serves and presents data from a FireX execution. 
If you are not already familiar with FireX, it's worthwhile [reading the docs](http://www.firexapp.com/)
 before continuing here. 
Though Flame serves the Flame UI, documentation for the UI itself [can be found here](https://github.com/FireXStuff/firex-flame-ui). 
This documentation focuses only on responsibilities of the Flame server.

Briefy, FireX is a general-purpose automation platform that affords task definition and execution 
by writing lightly annotated Python code. 

Flame receives events produced by the firexapp backend and aggregates data from those events to 
create the Flame datamodel. This model is then made availabe via Flame's REST and SocketIO APIs. 
Further, Flame serves the Flame UI, a Single Page Application for viewing data from a single FireX execution.

## Flames are Ephemeral
The Flame instance launched by a particular firexapp execution typically only exists to serve the data capture 
and data fetch needs of the launching execution. FireX achieves it's reliability by being distributed, and 
relying on short-lived servers instead of a single, 'always on' server is a core part of being distributed. 
The Flame server as a system process last longer than the firexapp run itself, ensuring that data can still 
be viewed after the firexapp execution has completed. To prevent Flame server processes from remaining indefinitely, 
they self-terminate automatically after 2 days. The lifetime of Flame servers can be adjusted by 
supplying '--flame_timeout <timeout in secs>'. 

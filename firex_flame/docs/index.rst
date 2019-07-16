Welcome to FireX Flame's Documentation!
=====================================

Flame is a lighweight server that collects, serves and presents data from a FireX execution.
If you are not already familiar with FireX, it's worthwhile
`reading the docs <http://www.firexapp.com/>`_ before continuing here.

Though Flame serves the Flame UI, documentation for the UI itself
`can be found here <https://github.com/FireXStuff/firex-flame-ui>`_.
This documentation focuses only on responsibilities of the Flame server.

Briefy, FireX is a general-purpose automation platform that affords task definition and execution
by writing lightly annotated Python code.

Flame receives events produced by the firexapp backend and aggregates data from those events to
create the Flame data-model. This model is then made availabe via Flame's REST and SocketIO APIs.
Further, Flame serves the Flame UI, a Single Page Application for viewing data from a single FireX execution.


API Reference Indices
==================

* :ref:`genindex`
* :ref:`modindex`

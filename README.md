# AiiDA submission controller

A prototype of a class to help controlling the number of submissions performed at any given time.

This is just a prototype.

It includes an abstract base class that implements the main logic, a very simple example of an implementation
to compute a 12x12 addition table, and a main script to run it (and get results and show them).

To use it, you are supposed to launch the script (e.g. in a `screen` terminal) with something like this:
```bash
while true ; do verdi run submission_controller.py ; sleep 5 ; done
```
where you can adapt the sleep time (typically for real simulations you might want something in the
range of 5-10 minutes, or anyway so that at every new run you have at least some new processes to submit,
but still less that the maximum number of available slots, to try to keep the 'queue' quite filled at any
given time.

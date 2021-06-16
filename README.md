# AiiDA submission controller

Some classes to help managing large number of submissions, while controlling the maximum number of submissions running at any given time.

This project is still in early phase and the API might change.

It includes an abstract base class that implements the main logic, a very simple example of an implementation
to compute a 12x12 addition table (in `examples/add_in_batches.py`), and a main script to run it (and get results and show them).

To use it, you are supposed to launch the script (e.g. in a `screen` terminal) with something like this:
```bash
cd examples
while true ; do verdi run add_in_batches.py ; sleep 5 ; done
```
where you can adapt the sleep time.
Typically, for real simulations, you might want something in the
range of 5-10 minutes, or anyway so that at every new run you have at least some new processes to submit,
but still less that the maximum number of available slots, to try to keep the 'queue' quite filled at any
given time.

There is also a second subclass that, rather than just creating new submissions from some extras, will use (input) nodes in another group as a reference for which calculations to run (e.g.: a group of crystal structures, representing the inputs to a set of workflows).

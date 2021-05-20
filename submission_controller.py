"""A prototype class to submit processes in batches, avoiding to submit too many.

Author: Giovanni Pizzi (2021).
"""

from aiida import orm, engine, plugins
import abc

class BaseSubmissionController:
    """Controller to submit a maximum number of processes (workflows or calculations) at a given time.
    
    This is an abstract base class: you need to subclass it and define the abstract methods.
    """
    def __init__(self, group_label, max_concurrent):
        """
        Create a new controller to manage (and limit) concurrent submissions.

        :param group_label: a group label: the group will be created at instantiation (if not existing already,
            and it will be used to manage the calculations)
        :param extra_unique_keys: a tuple or list of keys of extras that are used to uniquely identify
            a process in the group. E.g. ('value1', 'value2').
        
        :note: try to use actual values that allow for an equality comparison (strings, bools, integers), and avoid
           floats, because of truncation errors.
        """
        self._group_label = group_label
        self._max_concurrent = max_concurrent

        # Create the group if needed
        self._group, _ = orm.Group.objects.get_or_create(self.group_label)

    @property
    def group_label(self):
        """Return the label of the group that is managed by this class."""
        return self._group_label

    @property
    def group(self):
        """Return the AiiDA ORM Group instance that is managed by this class."""
        return self._group

    @property
    def max_concurrent(self):
        """Value of the maximum number of concurrent processes that can be run."""
        return self._max_concurrent

    def get_all_submitted_pks(self):
        """Return a dictionary of all processes that have been already submitted (i.e., are in the group).
        
        :return: a dictionary where:

            - the keys are the tuples with the values of extras that uniquely identifies processes, in the same
              order as returned by get_extra_unique_keys().

            - the values are the corresponding process PKs.

        :note: this returns all processes, both active and completed (sealed).
        """
        projections = [f'extras.{unique_key}' for unique_key in self.get_extra_unique_keys()] + ['id']

        qb = orm.QueryBuilder()
        qb.append(orm.Group, filters={'label': self.group_label}, tag='group')
        qb.append(orm.ProcessNode, project=projections, tag='process',  with_group='group')
        all_submitted = {}
        for data in qb.all():
            all_submitted[tuple(data[:-1])] = data[-1]

        return all_submitted

    def get_all_submitted_processes(self):
        """Return a dictionary of all processes that have been already submitted (i.e., are in the group).
        
        :return: a dictionary where:

            - the keys are the tuples with the values of extras that uniquely identifies processes, in the same
              order as returned by get_extra_unique_keys().

            - the values are the corresponding AiiDA ProcessNode instances.

        :note: this returns all processes, both active and completed (sealed).
        """
        projections = [f'extras.{unique_key}' for unique_key in self.get_extra_unique_keys()] + ['*']

        qb = orm.QueryBuilder()
        qb.append(orm.Group, filters={'label': self.group_label}, tag='group')
        qb.append(orm.ProcessNode, project=projections, tag='process',  with_group='group')
        all_submitted = {}
        for data in qb.all():
            all_submitted[tuple(data[:-1])] = data[-1]

        return all_submitted

    def _check_submitted_extras(self):
        """Return a set with the extras of the processes tha have been already submitted."""
        return set(self.get_all_submitted_pks().keys())

    def _count_active_in_group(self):
        """Count how many active (unsealed) processes there are in the group."""
        qb = orm.QueryBuilder()
        qb.append(orm.Group, filters={'label': self.group_label}, tag='group')
        qb.append(orm.ProcessNode, project='id', tag='process',  with_group='group', filters={'or': [{'attributes.sealed': False}, {'attributes': {'!has_key': 'sealed'}}]})
        return(qb.count())

    @property
    def num_active_slots(self):
        """Number of active slots (i.e. processes in the group that are unsealed)."""
        return self._count_active_in_group()

    @property
    def num_available_slots(self):
        """Number of available slots (i.e. how many processes would be submitted in the next batch submission)."""
        return max(0, self.max_concurrent - self.num_active_slots)

    @property
    def num_to_run(self):
        """Number of processes that still have to be submitted."""
        return len(self.get_all_extras_to_submit().difference(self._check_submitted_extras()))

    @property
    def num_already_run(self):
        """Number of processes that have already been submitted (and might or might not have finished)."""
        return len(self._check_submitted_extras())

    def submit_new_batch(self, dry_run=False, sort=True):
        """Submit a new batch of calculations, avoiding to have more than self.max_concurrent active at the same time."""

        to_submit = []
        extras_to_run = set(self.get_all_extras_to_submit()).difference(self._check_submitted_extras())
        if sort:
            extras_to_run = sorted(extras_to_run)
        for workchain_extras in extras_to_run:
            to_submit.append(workchain_extras)
            if len(to_submit) + self._count_active_in_group() >= self.max_concurrent:
                break

        if dry_run:
            return {key: None for key in to_submit}

        submitted = {}
        for workchain_extras in to_submit:
            # Get the inputs and the process calculation for submission
            inputs, process_class = self.get_inputs_and_processclass_from_extras(workchain_extras)

            # Actually submit
            res = engine.submit(process_class, **inputs)            
            # Add extras, and put in group
            res.set_extra_many({key: value for key, value in zip(self.get_extra_unique_keys(), workchain_extras)})
            self.group.add_nodes([res])
            submitted[workchain_extras] = res

        return submitted

    @abc.abstractmethod
    def get_extra_unique_keys(self):
        """Return a tuple of the kes of the unique extras that will be used to uniquely identify your workchains."""
        pass

    @abc.abstractmethod
    def get_all_extras_to_submit(self):
        """
        Return a *set* of the values of all extras uniquely identifying all simulations that you want to submit.
        
        Each entry of the set must be a tuple, in same order as the keys returned by get_extra_unique_keys().

        :note: for each item, pass extra values as tuples (because lists are not hashable, so you cannot make
            a set out of them).
        """
        pass

    @abc.abstractmethod
    def get_inputs_and_processclass_from_extras(self, extras_values):
        """Return the inputs and the process class for the process to run, associated a given tuple of extras values.
        
        :param extras_values: a tuple of values of the extras, in same order as the keys returned by
            get_extra_unique_keys().
        
        :return: ``(inputs, process_class)``, that will be used as follows:
           
           submit(process_class, **inputs)
        """
        pass


class AdditionTableSubmissionController(BaseSubmissionController):
    """The implementation of a SubmissionController to compute a 12x12 table of additions."""

    def __init__(self, code_name, *args, **kwargs):
        """Pass also a code name, that should be a code associated to an `arithmetic.add` plugin."""
        super().__init__(*args, **kwargs)
        self._code = orm.load_code(code_name)
        self._process_class = plugins.CalculationFactory('arithmetic.add')

    def get_extra_unique_keys(self):
        """Return a tuple of the kes of the unique extras that will be used to uniquely identify your workchains.

        Here I will use the value of the two integer operands as unique identifiers.
        """
        return ['left_operand', 'right_operand']

    def get_all_extras_to_submit(self):
        """
        I want to compute a 12x12 table.

        I will return therefore the following list of tuples: [(1, 1), (1, 2), ..., (12, 12)].
        """
        all_extras = set()
        for left_operand in range(1, 13):
            for right_operand in range(1, 13):
                all_extras.add((left_operand, right_operand))
        return all_extras

    def get_inputs_and_processclass_from_extras(self, extras_values):
        """Return inputs and process class for the submission of this specific process.

        I just submit an ArithmeticAdd calculation summing the two values stored in the extras:
        ``left_operand + right_operand``.
        """
        inputs = {'code': self._code, 'x': orm.Int(extras_values[0]), 'y': orm.Int(extras_values[1])}
        return inputs, self._process_class


if __name__ == "__main__":
    import sys

    ## IMPORTANT: make sure that you have a `add@localhost` code, that you can setup (once you have a
    ## localhost computer) using the following command, for instance:
    ##
    ##    verdi code setup -L add --on-computer --computer=localhost -P arithmetic.add --remote-abs-path=/bin/bash -n

    # Create a controller
    controller = AdditionTableSubmissionController(
        code_name = 'add@localhost',
        group_label = 'tests/addition_table',
        max_concurrent = 10
    )

    print("Max concurrent :", controller.max_concurrent)
    print("Active slots   :", controller.num_active_slots)
    print("Available slots:", controller.num_available_slots)
    print("Already run    :", controller.num_already_run)
    print("Still to run   :", controller.num_to_run)
    print()

    ## Uncomment the following two lines if you just want to do a dry-run without actually submitting anything
    #print("I would run next:")
    #print(controller.submit_new_batch(dry_run=True))
    
    print("Let's run a new batch!")
    # Note: the number might differ from controller.num_available_slots shown above, as some more
    # calculations might be over in the meantime.
    run_processes = controller.submit_new_batch(dry_run=False)
    for run_process_extras, run_process in run_processes.items():
        print(f"{run_process_extras} --> PK = {run_process.pk}")

    print()

    ## Print results
    print(">>> RESULTS UP TO NOW:")
    print("    Legend:")
    print("      ###: not yet submitted")
    print("      ???: submitted, but no results (not finished or failed)")
    all_submitted = controller.get_all_submitted_processes()
    sys.stdout.write('   |')
    for right in range(1, 13):
        sys.stdout.write(f'{right:3d} ')
    sys.stdout.write('\n')
    sys.stdout.write('----' + '----' * 12)
    sys.stdout.write('\n')

    # Print table
    for left in range(1, 13):
        sys.stdout.write(f'{left:2d} |')
        for right in range(1, 13):
            process = all_submitted.get((left, right))
            if process is None:
                result = '###' # No node
            else:
                try:
                    result = f'{process.outputs.sum.value:3d}'
                except AttributeError:
                    result = f'???'  # Probably not completed, does not have output 'sum'
            sys.stdout.write(result + ' ')
        sys.stdout.write('\n')

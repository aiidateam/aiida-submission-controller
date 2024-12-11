# -*- coding: utf-8 -*-
"""An example of a SubmissionController implementation to compute a 12x12 table of additions."""
import time

from aiida import load_profile, orm
from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
from pydantic import field_validator

from aiida_submission_controller import BaseSubmissionController


class AdditionTableSubmissionController(BaseSubmissionController):
    """The implementation of a SubmissionController to compute a 12x12 table of additions."""

    code_label: str
    """Label of the `code.arithmetic.add` `Code`."""

    @field_validator("code_label")
    def _check_code_plugin(cls, value):
        plugin_type = orm.load_code(value).default_calc_job_plugin
        if plugin_type == "core.arithmetic.add":
            return value
        raise ValueError(f"Code with label `{value}` has incorrect plugin type: `{plugin_type}`")

    def get_extra_unique_keys(self):
        """Return a tuple of the keys of the unique extras that will be used to uniquely identify your workchains.

        Here I will use the value of the two integer operands as unique identifiers.
        """
        return ["left_operand", "right_operand"]

    def get_all_extras_to_submit(self):
        """I want to compute a 12x12 table.

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
        builder = ArithmeticAddCalculation.get_builder()
        builder.code = orm.load_code(self.code_label)
        builder.x = orm.Int(extras_values[0])
        builder.y = orm.Int(extras_values[1])

        return builder


def main():
    """Run the simulations defined in this class, as a showcase of the functionality of this class."""
    import sys  # pylint: disable=import-outside-toplevel

    ## IMPORTANT: make sure that you have a `add@localhost` code, that you can setup (once you have a
    ## localhost computer) using the following command, for instance:
    ##
    ##  verdi code setup -L add --on-computer --computer=localhost -P core.arithmetic.add --remote-abs-path=/bin/bash -n
    # Create a controller
    load_profile()

    group, _ = orm.Group.collection.get_or_create(label="tests/addition_table")

    controller = AdditionTableSubmissionController(
        code_label="add@localhost",
        group_label=group.label,
        max_concurrent=10,
    )

    while True:
        print("Max concurrent :", controller.max_concurrent)
        print("Active slots   :", controller.num_active_slots)
        print("Available slots:", controller.num_available_slots)
        print("Already run    :", controller.num_already_run)
        print("Still to run   :", controller.num_to_run)
        print()

        ## Uncomment the following two lines if you just want to do a dry-run without actually submitting anything
        # print("I would run next:")
        # print(controller.submit_new_batch(dry_run=True))

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
        sys.stdout.write("   |")
        for right in range(1, 13):
            sys.stdout.write(f"{right:3d} ")
        sys.stdout.write("\n")
        sys.stdout.write("----" + "----" * 12)
        sys.stdout.write("\n")

        # Print table
        for left in range(1, 13):
            sys.stdout.write(f"{left:2d} |")
            for right in range(1, 13):
                process = all_submitted.get((left, right))
                if process is None:
                    result = "###"  # No node
                else:
                    try:
                        result = f"{process.outputs.sum.value:3d}"
                    except AttributeError:
                        result = "???"  # Probably not completed, does not have output 'sum'
                sys.stdout.write(result + " ")
            sys.stdout.write("\n")

        time.sleep(10)

        if controller.num_to_run == 0:
            break


if __name__ == "__main__":
    main()

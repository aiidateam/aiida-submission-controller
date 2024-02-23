#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""An example of a SubmissionController implementation for a small set of PwBaseWorkChains."""

import time

from aiida import load_profile, orm
from aiida_quantumespresso.workflows.pw.base import PwBaseWorkChain
from ase.build import bulk

from aiida_submission_controller import FromGroupSubmissionController


class PwBaseSubmissionController(FromGroupSubmissionController):
    """SubmissionController to run PwBaseWorkChains from a group of `StructureData` nodes."""

    pw_code: str
    """The label of the `Code` to use for the `PwBaseWorkChain`."""
    overrides: dict = {}
    """A dictionary of overrides to pass to `PwBaseWorkChain.get_builder_from_protocol()`."""

    def get_inputs_and_processclass_from_extras(self, extras_values):
        parent_node = self.get_parent_node_from_extras(extras_values)

        if not isinstance(parent_node, orm.StructureData):
            raise ValueError("The parent node is not a StructureData node.")

        builder = PwBaseWorkChain.get_builder_from_protocol(
            code=orm.load_code(self.pw_code),
            structure=parent_node,
            overrides=self.overrides,
        )
        return builder


def main():
    """Main execution when called as a script."""

    load_profile()

    # To make the example easier to execute, the parent structure group and the workchain group are created here
    # and the structure group is populated with some example structures in case it does not exist yet.
    structure_group, created = orm.Group.collection.get_or_create("structures")
    workchain_group, _ = orm.Group.collection.get_or_create("workchain/base")

    if created:
        for cell_size in (3.9, 4.0, 4.1, 4.2, 4.3, 4.4):
            structure = orm.StructureData(ase=bulk("Al", a=cell_size, cubic=True))
            structure.store()
            structure_group.add_nodes(structure)

    controller = PwBaseSubmissionController(
        unique_extra_keys=("_aiida_hash",),
        parent_group_label=structure_group.label,
        group_label=workchain_group.label,
        max_concurrent=1,
        pw_code="pw@localhost",  # Replace with the label of a code configured for Quantum ESPRESSO pw.x
    )
    while True:
        controller.submit_new_batch(verbose=True)
        time.sleep(30)


if __name__ == "__main__":
    main()

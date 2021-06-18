# -*- coding: utf-8 -*-
"""FromGroupSubmissionController implementation for PwBaseWorkChain."""

import typing as ty

from aiida import load_profile, orm, plugins

from aiida_submission_controller import FromGroupSubmissionController


class PwBaseSubmissionController(FromGroupSubmissionController):
    """FromGroupSubmissionController implementation for PwBaseWorkChain."""
    def __init__(self,
                 code_label,
                 pseudo_family_label,
                 *args,
                 parent_group_filters=None,
                 protocol=None,
                 overrides=None,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self._code = orm.load_code(code_label)
        self._pseudo_family_label = pseudo_family_label
        self._pseudo_family = orm.load_group(pseudo_family_label)
        self._parent_group_filters = parent_group_filters if parent_group_filters is not None else {}
        self._protocol = protocol
        self._overrides = overrides if overrides is not None else {}
        self._process_class = plugins.WorkflowFactory(
            'quantumespresso.pw.base')

    def get_extra_unique_keys(self) -> ty.Tuple[str]:
        """Return a tuple of the extra key or keys used to uniquely identify your workchains."""
        return ('mpid', )

    def get_all_extras_to_submit(self) -> ty.Set[ty.Tuple[str]]:
        """Return a set of all the unique extras to submit."""
        extras_projections = self.get_process_extra_projections()
        extras_projections += ('attributes.kinds', )

        qbuild = orm.QueryBuilder()
        qbuild.append(orm.Group,
                      filters={'id': self.parent_group.pk},
                      tag='group')
        qbuild.append(orm.Node,
                      filters=self._parent_group_filters,
                      project=extras_projections,
                      tag='process',
                      with_group='group')
        results = qbuild.all()

        pseudo_family_elements = set(self._pseudo_family.elements)
        extras = []
        for (mpid, kinds) in results:
            kind_names = set(kind['name'] for kind in kinds)
            if kind_names.issubset(pseudo_family_elements):
                extras.append((mpid, ))

        # I return sorted results for reproducibility of the order of execution
        extras = sorted(extras)

        assert len(set(extras)) == len(
            extras), 'There are duplicate extras in the parent group'
        return set(extras)

    def get_inputs_and_processclass_from_extras(self,
                                                extras_values: ty.Tuple[str]):
        """Construct the inputs and get the process class from the values of the uniquely identifying extras."""
        structure = self.get_parent_node_from_extras(extras_values)
        overrides = self._overrides.update(
            {'pseudo_family': self._pseudo_family_label})
        builder = self._process_class.get_builder_from_protocol(
            self._code,
            structure,
            protocol=self._protocol,
            overrides=overrides)

        return builder, self._process_class


def main():
    """Submit a set of PwBaseWorkChains."""
    # === PARAMETERS === #
    profile = 'asc'
    code_label = 'pw-6.7MaX_conda@localhost'
    pseudo_family_label = 'SSSP/1.1/PBE/efficiency'
    parent_group_label = 'structures/mp/2018_10_18'
    parent_group_filters = {'attributes.sites': {'longer': 0, 'shorter': 3}}
    target_group_label = 'tests/pw_base'
    max_concurrent = 2
    dry_run = False
    # === PARAMETERS === #

    load_profile(profile)
    controller = PwBaseSubmissionController(
        code_label,
        pseudo_family_label,
        parent_group_filters=parent_group_filters,
        parent_group_label=parent_group_label,
        group_label=target_group_label,
        max_concurrent=max_concurrent)

    print('Max concurrent :', controller.max_concurrent)
    print('Active slots   :', controller.num_active_slots)
    print('Available slots:', controller.num_available_slots)
    print('Already run    :', controller.num_already_run)
    print('Still to run   :', controller.num_to_run)
    print()

    print('Submitting batch...')
    run_processes = controller.submit_new_batch(dry_run=dry_run)
    if not dry_run:
        for run_process_extras, run_process in run_processes.items():
            print(f'{run_process_extras} --> PK = {run_process.pk}')
    print('Done.')
    print()


if __name__ == '__main__':
    main()

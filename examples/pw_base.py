"""An example of a SubmissionController implementation for a small set of PwBaseWorkChains."""

import typing as ty

from aiida import orm, plugins, load_profile
from aiida_submission_controller.submission_controller import BaseSubmissionController
from qe_tools import CONSTANTS
import warnings


class PwBaseSubmissionController(BaseSubmissionController):
    """The implementation of a SubmissionController to run a small set of PwBaseWorkChains."""

    INPUT_PLUGIN = 'quantumespresso.pw'
    WORKFLOW_ENTRY_POINT = 'quantumespresso.pw.base'

    def __init__(
            self,
            code_id: ty.Union[str, int],
            structure_group_id: ty.Union[str, int],
            structure_filters: ty.Dict[str, ty.Any],
            pseudo_family_id: ty.Union[str, int],
            *args,
            **kwargs
        ):
        """Add a `code_id` argument for loading a code associated to `quantumespresso.pw`."""
        super().__init__(*args, **kwargs)
        self._code = orm.load_code(identifier=code_id)
        self._process_class = plugins.WorkflowFactory(self.WORKFLOW_ENTRY_POINT)
        self._structure_group = orm.load_group(identifier=structure_group_id)
        self._structure_filters = structure_filters
        self._pseudo_family = orm.load_group(identifier=pseudo_family_id)

        # assert self._code.get_plugin_name() == self.INPUT_PLUGIN

    def get_extra_unique_keys(self) -> ty.Tuple[str]:
        """Return a tuple of the extra key or keys used to uniquely identify your workchains."""
        return ('mpid',)

    def get_all_extras_to_submit(self) -> ty.Set[ty.Tuple[str]]:
        """Return a set of all the unique extras to submit."""
        pseudo_family_elements = set(self._pseudo_family.elements)
        
        qb = orm.QueryBuilder()
        qb.append(orm.Group, filters={'label': self._structure_group.label}, tag='group')
        qb.append(orm.StructureData,
                  project=['extras.mpid', 'attributes.kinds'],
                  tag='structure',
                  with_group='group',
                  filters={
                      'extras': {'has_key': 'mpid'},
                      **self._structure_filters
                  }
            )
        qr = qb.all()

        all_extras = []
        for (mpid, kinds) in qr:
            kind_names = set(kind['name'] for kind in kinds)
            if kind_names.issubset(pseudo_family_elements):
                all_extras.append((mpid,))
        all_extras = set(all_extras)
        
        # all_extras = set((mpid,) for mpid in qb.all(flat=True))
        return all_extras

    def _get_structure_from_extras(self, extras_values: ty.Tuple[str]) -> orm.StructureData:
        qb = orm.QueryBuilder()
        qb.append(orm.Group, filters={'label': self._structure_group.label}, tag='group')
        qb.append(orm.StructureData,
                  project='*',
                  tag='structure',
                  with_group='group',
                  filters={
                      'extras.mpid': extras_values[0]
                  }
            )
        structure = qb.all(flat=True)[0]
        return structure

    def get_inputs_and_processclass_from_extras(self, extras_values: ty.Tuple[str]):
        """Construct the inputs and get the process class from the values of the uniquely identifying extras."""
        structure = self._get_structure_from_extras(extras_values)
        try:
            pseudos = self._pseudo_family.get_pseudos(structure=structure)
            ecutwfc, ecutrho = self._pseudo_family.get_recommended_cutoffs(structure=structure)

            inputs = {
                'kpoints_distance': orm.Float(0.15),
                'pw': {
                    'metadata': {
                        'options': {
                            'resources': {
                                'num_machines': 1,
                                'num_mpiprocs_per_machine': 1
                            },
                            'max_wallclock_seconds': 30 * 60,
                            'withmpi': True
                        }
                    },
                    'code': self._code,
                    'structure': structure,
                    'pseudos': pseudos,
                    'parameters': orm.Dict(dict={
                        'CONTROL': {
                            'calculation': 'scf',
                        },
                        'SYSTEM': {
                            'ecutwfc': ecutwfc,
                            'ecutrho': ecutrho,
                            'nosym': False,
                            'smearing': 'gaussian',
                            'degauss': 0.2 / CONSTANTS.ry_to_ev
                        },
                        'ELECTRONS': {
                            'conv_thr': 1e-10,
                            'mixing_beta': 4e-1,
                            'electron_maxstep': 0,
                            'scf_must_converge': False
                        }
                    })
                },
            }

            return inputs, self._process_class

        except ValueError:
            warnings.warn(f'Cannot run mpid {extras_values[0]} for lack of pseudos.')
            return None, None


if __name__ == '__main__':
    # import sys

    load_profile('qnscf')

    controller = PwBaseSubmissionController(
        code_id='pw-6.7_qnscf',
        structure_group_id='mp_all',
        structure_filters={
            'attributes.sites': {
                'longer': 0,
                'shorter': 2
            }
        },
        pseudo_family_id='SSSP/1.1/PBE/efficiency',
        group_label='tests/mp_all/pw_pdos_qnscf',
        max_concurrent=100
    )

    print("Max concurrent :", controller.max_concurrent)
    print("Active slots   :", controller.num_active_slots)
    print("Available slots:", controller.num_available_slots)
    print("Already run    :", controller.num_already_run)
    print("Still to run   :", controller.num_to_run)
    print()

    run_processes = controller.submit_new_batch(dry_run=False)
    for run_process_extras, run_process in run_processes.items():
        print(f'{run_process_extras} --> <{run_process}>')

    print()

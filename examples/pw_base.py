# -*- coding: utf-8 -*-
"""An example of a SubmissionController implementation for a small set of PwBaseWorkChains."""

import typing as ty
import warnings

from aiida import load_profile, orm, plugins
from qe_tools import CONSTANTS

from aiida_submission_controller import BaseSubmissionController


class PwBaseSubmissionController(BaseSubmissionController):
    """The implementation of a SubmissionController to run a small set of PwBaseWorkChains."""

    WORKFLOW_ENTRY_POINT = "quantumespresso.pw.base"

    def __init__(
        self,
        pw_code_id: ty.Union[str, int],
        structure_group_id: ty.Union[str, int],
        pseudo_family_id: ty.Union[str, int],
        *args,
        structure_filters: ty.Optional[ty.Dict[str, ty.Any]] = None,
        **kwargs,
    ):
        """A SubmissionController for PwBaseWorkChains."""
        super().__init__(*args, **kwargs)
        self._code = orm.load_code(identifier=pw_code_id)
        self._process_class = plugins.WorkflowFactory(self.WORKFLOW_ENTRY_POINT)
        self._structure_group = orm.load_group(identifier=structure_group_id)
        self._structure_filters = (
            structure_filters if structure_filters is not None else {}
        )
        self._pseudo_family = orm.load_group(identifier=pseudo_family_id)

    def get_extra_unique_keys(self) -> ty.Tuple[str]:
        """Return a tuple of the extra key or keys used to uniquely identify your workchains."""
        return ("mpid",)

    def get_all_extras_to_submit(self) -> ty.Set[ty.Tuple[str]]:
        """Return a set of all the unique extras to submit."""
        pseudo_family_elements = set(self._pseudo_family.elements)

        qbuild = orm.QueryBuilder()
        qbuild.append(
            orm.Group, filters={"label": self._structure_group.label}, tag="group"
        )
        qbuild.append(
            orm.StructureData,
            project=["extras.mpid", "attributes.kinds"],
            tag="structure",
            with_group="group",
            filters={"extras": {"has_key": "mpid"}, **self._structure_filters},
        )
        res = qbuild.all()

        all_extras = []
        for mpid, kinds in res:
            kind_names = set(kind["name"] for kind in kinds)
            if kind_names.issubset(pseudo_family_elements):
                all_extras.append((mpid,))
        all_extras = set(all_extras)

        # all_extras = set((mpid,) for mpid in qb.all(flat=True))
        return all_extras

    def _get_structure_from_extras(
        self, extras_values: ty.Tuple[str]
    ) -> orm.StructureData:
        """Get a structure from the values of the extras."""
        qbuild = orm.QueryBuilder()
        qbuild.append(
            orm.Group, filters={"label": self._structure_group.label}, tag="group"
        )
        qbuild.append(
            orm.StructureData,
            project="*",
            tag="structure",
            with_group="group",
            filters={"extras.mpid": extras_values[0]},
        )
        structure = qbuild.all(flat=True)[0]
        return structure

    def get_inputs_and_processclass_from_extras(self, extras_values: ty.Tuple[str]):
        """Construct the inputs and get the process class from the values of the uniquely identifying extras."""
        structure = self._get_structure_from_extras(extras_values)
        pseudos = self._pseudo_family.get_pseudos(structure=structure)
        ecutwfc, ecutrho = self._pseudo_family.get_recommended_cutoffs(
            structure=structure
        )
        metadata = {
            "options": {
                "resources": {"num_machines": 1, "num_mpiprocs_per_machine": 1},
                "max_wallclock_seconds": 2 * 60,
                "withmpi": True,
            }
        }

        inputs = {
            "clean_workdir": orm.Bool(True),
            "kpoints_distance": orm.Float(0.25),
            "pw": {
                "structure": structure,
                "metadata": metadata,
                "code": self._code,
                "pseudos": pseudos,
                "parameters": orm.Dict(
                    dict={
                        "CONTROL": {"calculation": "scf", "verbosity": "low"},
                        "SYSTEM": {
                            "ecutwfc": ecutwfc,
                            "ecutrho": ecutrho,
                            "nosym": False,
                            "occupations": "smearing",
                            "smearing": "gaussian",
                            "degauss": 0.5 / CONSTANTS.ry_to_ev,
                        },
                        "ELECTRONS": {
                            "conv_thr": 1e-8,
                            "mixing_beta": 4e-1,
                            "electron_maxstep": 80,
                        },
                    }
                ),
            },
        }

        return inputs, self._process_class


def main():
    """Main execution when called as a script."""
    warnings.filterwarnings("ignore")

    profile = "asc"
    pseudo_family_id = "SSSP/1.1/PBE/efficiency"

    load_profile(profile)

    controller = PwBaseSubmissionController(
        pw_code_id="pw-6.7MaX_conda",
        structure_group_id="structures/mp/2018_10_18",
        structure_filters={
            "attributes.sites": {"longer": 0, "shorter": 3},
        },
        pseudo_family_id=pseudo_family_id,
        group_label="tests/pw_base",
        max_concurrent=2,
    )

    print("Max concurrent :", controller.max_concurrent)
    print("Active slots   :", controller.num_active_slots)
    print("Available slots:", controller.num_available_slots)
    print("Already run    :", controller.num_already_run)
    print("Still to run   :", controller.num_to_run)
    print()

    print("Submitting...")
    run_processes = controller.submit_new_batch(dry_run=False)
    for run_process_extras, run_process in run_processes.items():
        print(f"{run_process_extras} --> <{run_process}>")
    print("Done.")


if __name__ == "__main__":
    main()

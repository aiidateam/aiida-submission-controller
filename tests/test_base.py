# -*- coding: utf-8 -*-
"""Tests for the `BaseSubmissionController`."""
import pytest
from aiida import orm

from aiida_submission_controller.base import BaseSubmissionController, get_extras_dict

pytest_plugins = ["aiida.manage.tests.pytest_fixtures"]


class ExampleSubmissionController(BaseSubmissionController):
    """Dummy submission controller implementation to test the general logic."""

    n_submissions: int
    "Number of submissions to be made."

    def get_all_extras_to_submit(self):
        """Return a list of all the extras to be submitted."""
        all_extras = [(str(i),) for i in range(self.n_submissions)]
        return set(all_extras)

    def get_inputs_and_processclass_from_extras(self, extras_values):
        """Skip this method."""
        pass


@pytest.fixture
def get_group():
    """Generate a group containing a set of nodes.

    If necessary, seal the first `n_sealed` nodes.
    """

    def _get_group(label="test_group", n_nodes=0, n_sealed=0):
        group = orm.Group(label=label).store()
        if n_nodes == 0:
            return group

        extras = [str(i) for i in range(n_nodes)]
        active_processes = _generate_processes("idx", extras[n_sealed:])
        group.add_nodes(active_processes)

        sealed_processes = _generate_processes("idx", extras[:n_sealed], seal=True)
        group.add_nodes(sealed_processes)

        return group

    return _get_group


def _generate_processes(extra_key, extra_values, seal=False, group=None):
    """Generate a list of `WorkChainNode`s with the given extras."""
    processes = []
    for extra in extra_values:
        process = orm.WorkChainNode().store()
        process.set_extra(extra_key, extra)
        if seal:
            process.seal()
        if group:
            group.add_nodes(process)

        processes.append(process)

    return processes


def _seal_processes(extra_key, extra_values):
    """Seal the active processes specified via the extras."""
    qb = orm.QueryBuilder().append(orm.WorkChainNode, filters={f"extras.{extra_key}": {"in": extra_values}})
    for w in qb.all(flat=True):
        w.seal()


def test_get_extras_dict():
    """Test the `get_extras_dict` function."""
    extra_keys = ["a.1.1", "a.1.2", "b.1", "c.d"]
    workchain_extras = [1, 2, 3, 4]

    extras_dict = get_extras_dict(extra_keys, workchain_extras)

    assert extras_dict == {"a": {"1": {"1": 1, "2": 2}}, "b": {"1": 3}, "c": {"d": 4}}


@pytest.mark.usefixtures("aiida_profile_clean")
def test_get_query(get_group):
    """Test the `get_query method."""
    _ = get_group(n_nodes=5, n_sealed=3)

    submission_controller = ExampleSubmissionController(
        group_label="test_group", max_concurrent=1, unique_extra_keys=("idx",), n_submissions=5
    )

    # Check `get_process_extra_projections`
    process_projections = submission_controller.get_process_extra_projections()
    assert process_projections == ["extras.idx"]

    qb_all = submission_controller.get_query(process_projections=process_projections)
    qb_active = submission_controller.get_query(process_projections=process_projections, only_active=True)

    assert qb_all.count() == 5
    assert qb_active.count() == 2


@pytest.mark.usefixtures("aiida_profile_clean")
def test_get_all_submitted_pks(get_group):
    """Test the `get_all_submitted_pks` method."""
    group = get_group(n_nodes=5)

    submission_controller = ExampleSubmissionController(
        group_label="test_group", max_concurrent=1, unique_extra_keys=("idx",), n_submissions=5
    )

    submitted_pks = submission_controller.get_all_submitted_pks()

    assert len(submitted_pks) == 5
    # test `_check_submitted_extras` as well
    submitted_extras = submission_controller._check_submitted_extras()
    assert len(submitted_extras) == 5
    assert submitted_extras == set([("0",), ("1",), ("2",), ("3",), ("4",)])

    # Check that a node without the required extra is ignored
    test_node = orm.WorkChainNode().store()
    group.add_nodes(test_node)

    submitted_pks = submission_controller.get_all_submitted_pks()
    assert len(submitted_pks) == 5

    # Add the required extra
    test_node.set_extra("idx", 6)
    submitted_pks = submission_controller.get_all_submitted_pks()
    assert len(submitted_pks) == 6


@pytest.mark.usefixtures("aiida_profile_clean")
def test_get_all_submitted_processes(get_group):
    """Test the `get_all_submitted_processes` method."""
    _ = get_group(n_nodes=5, n_sealed=3)

    submission_controller = ExampleSubmissionController(
        group_label="test_group", max_concurrent=1, unique_extra_keys=("idx",), n_submissions=5
    )

    submitted_processes_all = submission_controller.get_all_submitted_processes()
    assert len(submitted_processes_all) == 5
    assert set(submitted_processes_all.keys()) == set([("0",), ("1",), ("2",), ("3",), ("4",)])

    # Only active processes
    submitted_processes_active = submission_controller.get_all_submitted_processes(only_active=True)
    assert len(submitted_processes_active) == 2
    assert set(submitted_processes_active.keys()) == set([("3",), ("4",)])

    # Use this setup to also test `_count_active_in_group`
    assert submission_controller._count_active_in_group() == 2


@pytest.mark.usefixtures("aiida_profile_clean")
def test_submit_new_batch(get_group):
    """Test the `submit_new_batch` method."""
    group = get_group(n_nodes=5, n_sealed=3)

    submission_controller = ExampleSubmissionController(
        group_label="test_group", max_concurrent=2, unique_extra_keys=("idx",), n_submissions=10
    )

    # Initial state: 2 active processes, 3 sealed processes and 5 to run
    assert submission_controller.num_active_slots == 2
    assert submission_controller.num_available_slots == 0
    assert submission_controller.num_to_run == 5
    assert submission_controller.num_already_run == 5

    submitted = submission_controller.submit_new_batch(dry_run=True, sort=True)
    # No available slots
    assert len(submitted) == 0

    # Seal one active processes
    _seal_processes("idx", ["3"])

    assert submission_controller.num_active_slots == 1

    submitted = submission_controller.submit_new_batch(dry_run=True, sort=True)
    assert len(submitted) == 1
    assert set(submitted.keys()) == set([("5",)])

    # Generate dummy process and seal the active ones
    _seal_processes("idx", ["4"])
    _generate_processes("idx", ["5"], seal=True, group=group)

    # Submit new batches until all are submitted
    submitted = submission_controller.submit_new_batch(dry_run=True, sort=True)
    assert len(submitted) == 2
    _generate_processes("idx", ["6", "7"], seal=True, group=group)

    submitted = submission_controller.submit_new_batch(dry_run=True, sort=True)
    assert len(submitted) == 2
    _generate_processes("idx", ["8", "9"], seal=True, group=group)

    # Check final state --> all processes are submitted
    submitted = submission_controller.submit_new_batch(dry_run=True, sort=True)
    assert len(submitted) == 0

    assert submission_controller.num_active_slots == 0
    assert submission_controller.num_available_slots == 2
    assert submission_controller.num_to_run == 0
    assert submission_controller.num_already_run == 10

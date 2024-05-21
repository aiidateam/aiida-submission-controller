# -*- coding: utf-8 -*-
"""A prototype class to submit processes in batches, avoiding to submit too many."""
from typing import Optional

from aiida import orm
from pydantic import Field, PrivateAttr, validator

from .base import BaseSubmissionController, validate_group_exists


class FromGroupSubmissionController(BaseSubmissionController):  # pylint: disable=abstract-method
    """SubmissionController implementation getting data to submit from a parent group.

    This is (still) an abstract base class: you need to subclass it
    and define the abstract methods.
    """

    dynamic_extra: dict = Field(default_factory=dict)
    """A dictionary of dynamic extras to be added to the extras of the process."""
    unique_extra_keys: tuple = Field(default_factory=tuple)
    """List of keys defined in the extras that uniquely define each process to be run."""
    parent_group_label: str
    """Label of the parent group from which to construct the process inputs."""
    filters: Optional[dict] = None
    """Filters applied to the query of the nodes in the parent group."""
    order_by: Optional[dict] = None
    """Ordering applied to the query of the nodes in the parent group."""

    _dynamic_extra_keys: tuple = PrivateAttr(default_factory=tuple)
    _dynamic_extra_values: tuple = PrivateAttr(default_factory=tuple)

    _validate_group_exists = validator("parent_group_label", allow_reuse=True)(validate_group_exists)

    def __init__(self, **kwargs):
        """Initialize the instance."""
        super().__init__(**kwargs)

        if self.dynamic_extra:
            self._dynamic_extra_keys, self._dynamic_extra_values = zip(*self.dynamic_extra.items())

    @property
    def parent_group(self):
        """Return the AiiDA ORM Group instance of the parent group."""
        return orm.Group.objects.get(label=self.parent_group_label)

    def get_extra_unique_keys(self):
        """Return a tuple of the keys of the unique extras that will be used to uniquely identify your workchains."""
        # `_parent_uuid` will be replaced by the `uuid` attribute in the queries
        combined_extras = ["_parent_uuid"] + list(self.unique_extra_keys) + list(self._dynamic_extra_keys)
        return tuple(combined_extras)

    def get_parent_node_from_extras(self, extras_values):
        """Return the Node instance (in the parent group) from the `uuid` identifying it."""
        return orm.load_node(extras_values[0])

    def get_all_extras_to_submit(self):
        """Return a *set* of the values of all extras uniquely identifying all simulations that you want to submit.

        Each entry of the set must be a tuple, in same order as the keys returned by get_extra_unique_keys().

        They are taken from the extra_unique_keys from the group.
        Note: the extra_unique_keys must actually form a unique set;
        if this is not the case, an AssertionError will be raised.
        """
        extras_projections = self.get_process_extra_projections()

        # Use only the unique extras (and the parent uuid) to identify the processes to be submitted
        if self._dynamic_extra_keys:
            extras_projections = extras_projections[: -len(self._dynamic_extra_keys)]

        qbuild = orm.QueryBuilder()
        qbuild.append(orm.Group, filters={"id": self.parent_group.pk}, tag="group")
        qbuild.append(
            orm.Node,
            project=["uuid"] + extras_projections[1:],  # Replace `_parent_uuid` with `uuid`
            filters=self.filters,
            tag="process",
            with_group="group",
        )

        if self.order_by is not None:
            qbuild.order_by(self.order_by)

        results = qbuild.all()

        # I return a set of results as required by the API
        # First, however, convert to a list of tuples otherwise
        # the inner lists are not hashable
        results = [tuple(_) for _ in results]
        for i, res in enumerate(results):
            assert all(
                extra is not None for extra in res
            ), "There is at least one of the nodes in the parent group that does not define one of the required extras."
            results[i] = (*res, *self._dynamic_extra_values)  # Add the dynamic extras to the results

        results_set = set(results)

        assert len(results) == len(results_set), "There are duplicate extras in the parent group"
        return results

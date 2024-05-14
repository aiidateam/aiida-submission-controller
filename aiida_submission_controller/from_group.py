# -*- coding: utf-8 -*-
"""A prototype class to submit processes in batches, avoiding to submit too many."""
from typing import Optional

from aiida import orm
from pydantic import field_validator

from .base import BaseSubmissionController, _validate_group_exists


class FromGroupSubmissionController(BaseSubmissionController):  # pylint: disable=abstract-method
    """SubmissionController implementation getting data to submit from a parent group.

    This is (still) an abstract base class: you need to subclass it
    and define the abstract methods.
    """

    parent_group_label: str
    """Label of the parent group from which to construct the process inputs."""
    filters: Optional[dict] = None
    """Filters applied to the query of the nodes in the parent group."""
    order_by: Optional[dict] = None
    """Ordering applied to the query of the nodes in the parent group."""

    @field_validator('group_label')
    @classmethod
    def validate_group_exists(cls, v: str) -> str:
        return _validate_group_exists(v)

    @property
    def parent_group(self):
        """Return the AiiDA ORM Group instance of the parent group."""
        return orm.Group.collection.get(label=self.parent_group_label)

    def get_parent_node_from_extras(self, extras_values):
        """Return the Node instance (in the parent group) from the (unique) extras identifying it."""
        extras_projections = self.get_process_extra_projections()
        assert len(extras_values) == len(extras_projections), f"The extras must be of length {len(extras_projections)}"
        filters = dict(zip(extras_projections, extras_values))

        qb = orm.QueryBuilder()
        qb.append(orm.Group, filters={"id": self.parent_group.pk}, tag="group")
        qb.append(orm.Node, project="*", filters=filters, tag="process", with_group="group")
        qb.limit(2)
        results = qb.all(flat=True)
        if len(results) != 1:
            raise ValueError(
                "I would have expected only 1 result for extras={extras}, I found {'>1' if len(qb) else '0'}"
            )
        return results[0]

    def get_all_extras_to_submit(self):
        """Return a *set* of the values of all extras uniquely identifying all simulations that you want to submit.

        Each entry of the set must be a tuple, in same order as the keys returned by get_extra_unique_keys().

        They are taken from the extra_unique_keys from the group.
        Note: the extra_unique_keys must actually form a unique set;
        if this is not the case, an AssertionError will be raised.
        """
        extras_projections = self.get_process_extra_projections()

        qb = orm.QueryBuilder()
        qb.append(orm.Group, filters={"id": self.parent_group.pk}, tag="group")
        qb.append(
            orm.Node,
            project=extras_projections,
            filters=self.filters,
            tag="process",
            with_group="group",
        )

        if self.order_by is not None:
            qb.order_by(self.order_by)

        results = qb.all()

        # I return a set of results as required by the API
        # First, however, convert to a list of tuples otherwise
        # the inner lists are not hashable
        results = [tuple(_) for _ in results]
        for res in results:
            assert all(
                extra is not None for extra in res
            ), "There is at least one of the nodes in the parent group that does not define one of the required extras."
        results_set = set(results)

        assert len(results) == len(results_set), "There are duplicate extras in the parent group"
        return results

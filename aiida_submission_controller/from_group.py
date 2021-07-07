# -*- coding: utf-8 -*-
"""A prototype class to submit processes in batches, avoiding to submit too many."""
from aiida import orm
from .base import BaseSubmissionController


class FromGroupSubmissionController(BaseSubmissionController):  # pylint: disable=abstract-method
    """SubmissionController implementation getting data to submit from a parent group.

    This is (still) an abstract base class: you need to subclass it
    and define the abstract methods.
    """
    def __init__(self, parent_group_label, *args, **kwargs):
        """Create a new controller to manage (and limit) concurrent submissions.

        :param parent_group_label: a group label: the group will be used to decide
          which submissions to use. The group must already exist. Extras (in the method
          `get_all_extras_to_submit`) will be returned from all extras in that group
          (you need to make sure they are unique).

        For all other parameters, see the docstring of ``BaseSubmissionController.__init__``.
        """
        super().__init__(*args, **kwargs)
        self._parent_group_label = parent_group_label
        # Load the group (this also ensures it exists)
        self._parent_group = orm.Group.objects.get(
            label=self.parent_group_label)

    @property
    def parent_group_label(self):
        """Return the label of the parent group that is used as a reference."""
        return self._parent_group_label

    @property
    def parent_group(self):
        """Return the AiiDA ORM Group instance of the parent group."""
        return self._parent_group

    def get_parent_node_from_extras(self, extras_values):
        """Return the Node instance (in the parent group) from the (unique) extras identifying it."""
        extras_projections = self.get_process_extra_projections()
        assert len(extras_values) == len(
            extras_projections
        ), f'The extras must be of length {len(extras_projections)}'
        filters = dict(zip(extras_projections, extras_values))

        qbuild = orm.QueryBuilder()
        qbuild.append(orm.Group,
                      filters={'id': self.parent_group.pk},
                      tag='group')
        qbuild.append(orm.Node,
                      project='*',
                      filters=filters,
                      tag='process',
                      with_group='group')
        qbuild.limit(2)
        results = qbuild.all(flat=True)
        if len(results) != 1:
            raise ValueError(
                "I would have expected only 1 result for extras={extras}, I found {'>1' if len(qbuild) else '0'}"
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

        qbuild = orm.QueryBuilder()
        qbuild.append(orm.Group,
                      filters={'id': self.parent_group.pk},
                      tag='group')
        qbuild.append(orm.Node,
                      project=extras_projections,
                      tag='process',
                      with_group='group')
        results = qbuild.all()

        # I return a set of results as required by the API
        # First, however, convert to a list of tuples otherwise
        # the inner lists are not hashable
        results = [tuple(_) for _ in results]
        for res in results:
            assert all(
                extra is not None for extra in res
            ), 'There is at least one of the nodes in the parent group that does not define one of the required extras.'
        results_set = set(results)

        assert len(results) == len(
            results_set), 'There are duplicate extras in the parent group'
        return results_set

# -*- coding: utf-8 -*-
"""A prototype package with tools to submit processes in batches, avoiding to submit too many."""
__version__ = '0.1.2'

__author__ = 'Giovanni Pizzi, Austin Zadoks'

from .base import BaseSubmissionController
from .from_group import FromGroupSubmissionController

__all__ = ('BaseSubmissionController', 'FromGroupSubmissionController')

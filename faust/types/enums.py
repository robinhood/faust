from enum import Enum


class ProcessingGuarantee(Enum):
    AT_LEAST_ONCE = 'at_least_once'
    EXACTLY_ONCE = 'exactly_once'

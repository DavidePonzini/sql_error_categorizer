'''Base classes for SQL error detectors.'''

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable

from ..sql_errors import SqlErrors
from ..query import Query

@dataclass(repr=False)
class DetectedError:
    '''Represents a detected SQL error with its type and associated data.'''

    error: SqlErrors
    data: tuple[Any, ...] = field(default_factory=tuple)

    def __repr__(self):
        return f"DetectedError({self.error.value} - {self.error.name}: {self.data})"

    def __str__(self) -> str:
        if self.data:
            return f'[{self.error.value:3}] {self.error.name}: {self.data}'
        return f'[{self.error.value:3}] {self.error.name}'
    
    def __hash__(self) -> int:
        return hash((self.error, self.data))

class BaseDetector(ABC):
    '''Abstract base class for SQL error detectors.'''

    def __init__(self, *,
                 query: Query,
                 solutions: list[Query] = [],
                 update_query: Callable[[str, str | None], None],
        ):        
        self.query = query
        self.solutions = solutions
        self.update_query = update_query

    @abstractmethod
    def run(self) -> list[DetectedError]:
        '''Run the detector and return a list of detected errors with their descriptions'''
        return []
    
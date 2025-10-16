from ...catalog import Table, Column

from abc import ABC, abstractmethod

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .select import Select


class SetOperation(ABC):
    '''
    Abstract base class for SQL set operations (i.e., SELECT, UNION, INTERSECT, EXCEPT).
    '''

    def __init__(self, sql: str, subquery_level: int = 0):
        self.sql = sql
        '''The SQL string representing the operation.'''
        
        self.subquery_level = subquery_level
        '''The level of subquery nesting.'''

    @property
    @abstractmethod
    def output(self) -> Table:
        '''Returns the output table schema of the set operation.'''
        return Table('')
    
    def __repr__(self, pre: str = '') -> str:
        return f'{pre}{self.__class__.__name__}'

    
    @abstractmethod
    def print_tree(self, pre: str = '') -> None:
        pass

    @property
    @abstractmethod
    def selects(self) -> list['Select']:
        '''Returns a list of all Select nodes in the tree.'''
        return []
    
    

    


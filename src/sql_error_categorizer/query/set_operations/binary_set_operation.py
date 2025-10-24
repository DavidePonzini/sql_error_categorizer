from ...catalog import Table, Column
from ...util import *
from .set_operation import SetOperation

from abc import ABC

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .select import Select


class BinarySetOperation(SetOperation, ABC):
    '''Represents a binary set operation (e.g., UNION, INTERSECT, EXCEPT).'''
    def __init__(self, sql: str, left: SetOperation, right: SetOperation, all: bool | None = None):
        super().__init__(sql)
        self.left = left
        self.right = right
        self.all = all
        '''Indicates whether the operation is ALL (duplicates allowed) or DISTINCT (duplicates removed).'''

    def __repr__(self, pre: str = '') -> str:
        modifiers = []

        if self.all is True:
            modifiers.append('ALL=True')
        elif self.all is False:
            modifiers.append('ALL=False')

        if self.order_by:
            modifiers.append(f'ORDER_BY={[col.name for col in self.order_by]}')
        if self.limit is not None:
            modifiers.append(f'LIMIT={self.limit}')
        if self.offset is not None:
            modifiers.append(f'OFFSET={self.offset}')

        result = f'{pre}{self.__class__.__name__}{"(" + ", ".join(modifiers) + ")" if modifiers else ""}\n'
        result +=  self.left.__repr__(pre + '|- ') + '\n'
        result += self.right.__repr__(pre + '`- ')

        return result

    @property
    def output(self) -> Table:
        # Assuming both sides have the same schema for simplicity
        return self.left.output
    
    def print_tree(self, pre: str = '') -> None:
        print(f'{pre}{self.__class__.__name__} (ALL={self.all})')
        print(                      f'{pre}|- Left:')
        self.left.print_tree(pre=   f'{pre}|  ')
        print(                      f'{pre}`- Right:')
        self.right.print_tree(pre=  f'{pre}   ')

    # TODO: Implement
    @property
    def limit(self) -> int | None:
        return None
    
    # TODO: Implement
    @property
    def offset(self) -> int | None:
        return None

    # TODO: Implement
    @property
    def order_by(self) -> list[Column]:
        return []
    
    @property
    def selects(self) -> list['Select']:
        return self.left.selects + self.right.selects

class Union(BinarySetOperation):
    '''Represents a SQL UNION operation.'''
    def __init__(self, sql: str, left: SetOperation, right: SetOperation, all: bool = False):
        super().__init__(sql, left, right, all=all)

class Intersect(BinarySetOperation):
    '''Represents a SQL INTERSECT operation.'''
    def __init__(self, sql: str, left: SetOperation, right: SetOperation, all: bool = False):
        super().__init__(sql, left, right, all=all)

class Except(BinarySetOperation):
    '''Represents a SQL EXCEPT operation.'''
    def __init__(self, sql: str, left: SetOperation, right: SetOperation, all: bool = False):
        super().__init__(sql, left, right, all=all)

    


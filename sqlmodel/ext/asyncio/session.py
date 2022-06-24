from typing import Any, Mapping, Optional, Sequence, TypeVar, Union, overload

from sqlalchemy import util
from sqlalchemy.ext.asyncio import AsyncSession as _AsyncSession
from sqlalchemy.ext.asyncio.engine import AsyncConnection, AsyncEngine
from sqlmodel.sql.base import Executable

from ...engine.result import Result, ScalarResult
from ...orm.session import Session
from ...sql.expression import Select, SelectOfScalar

_T = TypeVar("_T")


class AsyncSession(_AsyncSession):
    def __init__(
        self,
        bind: Optional[Union[AsyncConnection, AsyncEngine]] = None,
        binds: Optional[Mapping[object, Union[AsyncConnection, AsyncEngine]]] = None,
        **kw: Any,
    ):
        opts = dict(expire_on_commit=False)
        super().__init__(bind, binds, sync_session_class=Session, **{**opts, **kw})

    @overload
    async def exec(
        self,
        statement: Select[_T],
        *,
        params: Optional[Union[Mapping[str, Any], Sequence[Mapping[str, Any]]]] = None,
        execution_options: Mapping[str, Any] = util.EMPTY_DICT,
        bind_arguments: Optional[Mapping[str, Any]] = None,
        _parent_execute_state: Optional[Any] = None,
        _add_event: Optional[Any] = None,
        **kw: Any,
    ) -> Result[_T]:
        ...

    @overload
    async def exec(
        self,
        statement: SelectOfScalar[_T],
        *,
        params: Optional[Union[Mapping[str, Any], Sequence[Mapping[str, Any]]]] = None,
        execution_options: Mapping[str, Any] = util.EMPTY_DICT,
        bind_arguments: Optional[Mapping[str, Any]] = None,
        _parent_execute_state: Optional[Any] = None,
        _add_event: Optional[Any] = None,
        **kw: Any,
    ) -> ScalarResult[_T]:
        ...

    async def exec(
        self,
        statement: Union[
            Select[_T],
            SelectOfScalar[_T],
            Executable[_T],
        ],
        *,
        params: Optional[Union[Mapping[str, Any], Sequence[Mapping[str, Any]]]] = None,
        execution_options: Mapping[str, Any] = util.EMPTY_DICT,
        bind_arguments: Optional[Mapping[str, Any]] = None,
        _parent_execute_state: Optional[Any] = None,
        _add_event: Optional[Any] = None,
        **kw: Any,
    ) -> Union[Result[_T], ScalarResult[_T]]:
        results = await super().execute(
            statement,
            params=params,
            execution_options=execution_options,
            bind_arguments=bind_arguments,
            _parent_execute_state=_parent_execute_state,
            _add_event=_add_event,
            **kw,
        )
        if isinstance(statement, SelectOfScalar):
            return results.scalars()  # type: ignore
        return results  # type: ignore

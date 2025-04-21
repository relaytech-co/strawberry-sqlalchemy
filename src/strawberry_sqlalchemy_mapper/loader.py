import logging
from collections import defaultdict
from typing import (
    Any,
    AsyncContextManager,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

from sqlalchemy import select, tuple_
from sqlalchemy.engine.base import Connection
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession
from sqlalchemy.orm import RelationshipProperty, Session
from strawberry.dataloader import DataLoader


class StrawberrySQLAlchemyLoader:
    """
    Creates DataLoader instances on-the-fly for SQLAlchemy relationships
    """

    _loaders: Dict[RelationshipProperty, DataLoader]

    def __init__(
        self,
        bind: Union[Session, Connection, None] = None,
        async_bind_factory: Optional[
            Union[
                Callable[[], AsyncContextManager[AsyncSession]],
                Callable[[], AsyncContextManager[AsyncConnection]],
            ]
        ] = None,
    ) -> None:
        self._loaders = {}
        self._bind = bind
        self._async_bind_factory = async_bind_factory
        self._logger = logging.getLogger("strawberry_sqlalchemy_mapper")
        if bind is None and async_bind_factory is None:
            self._logger.warning(
                "One of bind or async_bind_factory must be set for loader to function properly."
            )

    async def _execute(self, *args, **kwargs):
        if self._async_bind_factory:
            async with self._async_bind_factory() as bind:
                return await bind.execute(*args, **kwargs)
        else:
            assert self._bind is not None
            return self._bind.execute(*args, **kwargs)

    def loader_for(self, relationship: RelationshipProperty) -> DataLoader:
        """
        Retrieve or create a DataLoader for the given relationship
        """
        try:
            return self._loaders[relationship]
        except KeyError:
            related_model = relationship.entity.entity

            async def load_fn(keys: List[Tuple]) -> List[Any]:
                # To handle both, we:
                # 1. Filter on remote columns in the relationship which are the other side of any local columns
                #       i.e. The join in the relationship from the source table
                # 2. Return the values of the

                # We get the other side of the first jump in the relationship - i.e. the join from the local table columns
                # If there are any other relationships, we join on those
                # This covers us for the cases where there is a many to many relationship

                # We filter on remote columns in the relationship which are the other side of any local columns
                #   i.e. The join in the relationship from the source table
                remote_cols_for_where = [
                    remote
                    for local, remote in relationship.local_remote_pairs or []
                    if local in relationship.local_columns and remote.key
                ]

                # We join on any other pairs in the relationship which aren't associated with the local columns
                # This covers the many to many case
                non_local_pairs = [
                    (local, remote)
                    for local, remote in relationship.local_remote_pairs or []
                    if local not in relationship.local_columns
                ]

                # For each row we return both the target model and the values of the columns which we filtered on
                query = select(related_model, *remote_cols_for_where).filter(
                    tuple_(*remote_cols_for_where).in_(keys)
                )

                for local, remote in non_local_pairs:
                    query = query.join(remote.table, local == remote)

                if relationship.order_by:
                    query = query.order_by(*relationship.order_by)
                result = await self._execute(query)

                rows = result.tuples().all()

                grouped_keys: Mapping[Tuple, List[Any]] = defaultdict(list)
                for target_entity, *cols_filtered_on in rows:
                    grouped_keys[tuple(cols_filtered_on)].append(target_entity)
                if relationship.uselist:
                    return [grouped_keys[key] for key in keys]
                else:
                    return [
                        grouped_keys[key][0] if grouped_keys[key] else None
                        for key in keys
                    ]

            self._loaders[relationship] = DataLoader(load_fn=load_fn)
            return self._loaders[relationship]

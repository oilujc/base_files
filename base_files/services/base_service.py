from abc import ABC, abstractclassmethod, abstractmethod

from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession


class AbstractBaseService(ABC):

	@abstractclassmethod
	async def get_queryset(cls, db: AsyncSession):
		pass


class BaseService(AbstractBaseService):
	model = None

	@classmethod
	async def get_queryset(cls, db: AsyncSession, **kwargs):
		if not cls.model:
			raise Exception('Model not defined')

		query = kwargs.get('query', select(cls.model))

		if 'offset' in kwargs:
			query = query.offset(kwargs.get('offset'))

		if 'limit' in kwargs:
			query = query.limit(kwargs.get('limit'))

		result = await db.execute(query)
		
		return result.scalars().all()

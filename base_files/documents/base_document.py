from abc import ABC, abstractclassmethod, abstractmethod
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy.future import select


class AbstractBaseDocument(ABC):

	@abstractclassmethod
	async def get_data(cls, db: AsyncSession):
		pass

	@abstractclassmethod
	async def gen_data(cls, data):
		pass

	@abstractclassmethod
	def map_query_item(cls, item):
		pass

	@abstractclassmethod
	async def get_query(cls, *args):
		pass

	@abstractclassmethod
	def map_data(cls, data):
		pass


class BaseDocument(AbstractBaseDocument):

	model = None
	_index = None
	_type = '_doc'
	_index_keys = None
	
	@classmethod
	async def get_data(cls, db: AsyncSession, **kwargs):

		if not cls.model:
			raise Exception('Model not defined')
			
		query = kwargs.get('query', select(cls.model))
		result = await db.execute(query)

		data = list(map(cls.map_data, result.scalars().all()))
		
		return data

	@classmethod
	async def gen_data(cls, data):

		if not cls._index:
			raise Exception('Index not defined')

		for source in data:
			yield {
				"_index": cls._index,
				"_type": cls._type,
				"_source": source,
			}

	@classmethod
	def map_query_item(cls, item):

		if not cls._index_keys:
			raise Exception('Index keys not defined')

		if not item:
			return None

		filter = 'match'
		bool_type = 'filter'

		for key , value in item.items():

			if key not in cls._index_keys:
				continue
						
			if type(value) == list:
				value = ' '.join(value)

			if type(value) == dict:
				bool_type = value.get('bool_type', 'filter')
				filter = value.get('filter', 'match')
				value = value.get('query', '')

			return {
				'bool_type': bool_type,
				'data': {
					filter : {
						cls._index_keys[key] : {
							"query" : value
						}
					}
				}
			}

	@classmethod
	def get_query(cls, *args, **kwargs):
		
		items = list(map(cls.map_query_item, args))
		return items

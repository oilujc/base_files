from sqlalchemy.ext.asyncio import AsyncSession
from elasticsearch.helpers import async_bulk

from documents.base_document import BaseDocument
from services.base_service import BaseService

class ElasticService(BaseService):
	document: BaseDocument = None

	@classmethod
	async def gen_data(cls, es, db: AsyncSession):

		if not cls.document:
			raise Exception('Document not defined')

		data = await cls.get_queryset(db)
		data = list(map(cls.map_data, data))

		index = await async_bulk(es, cls.document.gen_data(data=data))

		return index

	@classmethod
	async def get_data(cls, es, *args, **kwargs):

		if not cls.document:
			raise Exception('Document not defined')

		limit = kwargs.get('limit', 10)
		offset = kwargs.get('offset', 0)

		query = {
			'query_string': {
				'query': '*'
			}
		}

		if args and any(args):
			qs = cls.document.get_query(
				*args,
			)

			filters = [ i['data'] for i in qs if i and i['bool_type'] == 'filter']
			filter = {'filter': filters} if filters else {}

			must = [i['data']  for i in qs if i and i['bool_type'] == 'must']
			must = {'must': must} if must else {}

			query = {
				'bool': {
					**filter,
					**must
				}
			}
			

		resp = await es.search(
			index=cls.document._index,
			query=query,
			size=limit,
			from_=offset
		)

		if resp.get('hits', {}).get('total', 0) == 0:
			return []

		resp = list(map(lambda item: item['_source'], resp['hits']['hits']))


		return resp

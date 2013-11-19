from abc import ABCMeta, abstractmethod

class Query(object):
	__metaclass__ = ABCMeta

	def __init__(self, clause):
		self.clause = clause
	@classmethod
	def with_clause(cls, clause):
		return cls({'clause': clause.q})
	def order_by(self, field, descending = False):
		self.field = field
		self.descending = descending
		return self
	def _to_map(self, queryName):
		q = {queryName: self.clause}
		if hasattr(self, 'field') and self.field != None:
			q.get(queryName)['orderBy'] = self.field
			q.get(queryName)['descending'] = self.descending
		return q
	@abstractmethod
	def to_map(self):
		pass

class C(object):
	def __init__(self, q):
		self.q = q
	@classmethod
	def cAll(cls):
		return cls({'type': 'all'})
	@classmethod
	def cEq(cls, field, value):
		return cls({'type': 'eq', 'field': field, 'value': value})
	@classmethod
	def cRange(cls, field, upper_limit, lower_limit, upper_included, lower_included):
		return cls({'type': 'range', 'field': field, 'upperLimit': upper_limit, 'lowerLimit': lower_limit,
			'upperIncluded': upper_included, 'lowerIncluded': lower_limit})
	@classmethod
	def cStartsWith(cls, field, prefix):
		return cls({'type': 'prefix', 'field': field, 'prefix': prefix})
	@classmethod
	def cIn(cls, field, values):
		return cls({'type': 'in', 'field': field, 'values': values})
	@classmethod
	def cWithInBox(cls, field, sw, ne):
		return cls({'type': 'geobox', 'field': field, 'box': {'sw': sw, 'ne': ne}})
	@classmethod
	def cWithInDistance(cls, field, center, radius, result_field = None):
		return cls({'type': 'geodistance', 'field': field, 'center': center, 
			'radius': radius, 'resultField': result_field })
	@classmethod
	def cAnd(cls, *clauses):
		return cls({'type': 'and', 'clauses': [c.q for c in clauses]})
	@classmethod
	def cOr(cls, *clauses):
		return cls({'type': 'or', 'clauses': [c.q for c in clauses]})
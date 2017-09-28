from collections import OrderedDict

import more_itertools

from . import database
from .augment import augment
from .utils import export
from .utils.custom_lists import *
from .utils.mtqdm import mtqdm


def createIndexArrayForDB(dbResult, listCtor=None, nameColumn="name"):
	if not listCtor:
		listCtor = CustomBaseList
	index = listCtor()
	nameIndex = {}
	for r in dbResult:
		index[r["id"]] = r
		nameIndex[r[nameColumn]] = r
	return index, nameIndex


def normalizeModelName(vendor, model):
	n = model.lower()
	vnpos = n.find(vendor.lower())
	if vnpos > -1:
		n = n[vnpos + len(vendor) :].strip()
	tokens = n.split(" ")
	return max(tokens, key=len)


class Dataset:
	indexes = ("drives", "models", "brands", "vendors")

	def normalizeNameInModelDict(self, m):
		m["name"] = normalizeModelName(
			self.brands[
				m["brand_id"]
			]["name"],
			m["name"]
		).upper()
		return m

	def __init__(self, dbPath=None):
		with database.DB(dbPath) as db:
			(self.vendors, self.vendorsByName) = createIndexArrayForDB(db.getVendors())
			(self.brands, self.brandsByName) = createIndexArrayForDB(db.getBrands())
			(self.drives, self.drivesBySerial) = createIndexArrayForDB(db.getDrives(), nameColumn="serial_number")
			(self.models, self.modelsByName) = createIndexArrayForDB(map(self.normalizeNameInModelDict, db.getModels()))
			self.reduced = __class__._isReduced(db)

	def augment(self):
		augment(self)

	def getAvailableAttrs(self):
		return {k for k in more_itertools.flatten((m.keys() for m in self.models))}

	def export(self, fileName=None, format=None, what=("vendors", "brands", "models", "drives")):
		return export(OrderedDict(  ( (propName, getattr(self, propName)) for propName in what)  ), fileName, format)

	@staticmethod
	def _isReduced(db):
		return database.TableName.fromStr(database.tablesNames["smart"]) not in set(db.getTables())

	@staticmethod
	def isReduced(dbPath=None):
		with database.DB(dbPath) as db:
			return _isReduced(db)

	@staticmethod
	def stats(dbPath=None, reduced=None):
		with database.DBAnalyser(dbPath) as db:
			if reduced is None:
				reduced = __class__._isReduced(db)
			if reduced:
				res = db.getDrivesStatsDenormReduced()
			else:
				res = db.getDrivesStatsDenorm()  # damn slow
		return res

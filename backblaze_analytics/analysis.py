import typing
import warnings
from functools import partial
from pathlib import Path

#from Chassis import Chassis
from lazily import lazyImport, pandas
from lazily import scipy as np

from . import database

from .core import *
from .core.Aggregator import *

from .dataset import Dataset
#from .datasetDescription import attrsSpec
from .fitters.XGBoostWeibullFitter import WeibullAggregator

from .utils.mtqdm import mtqdm
from .utils.PickleCache import PickleCache

plt = lazyImport("matplotlib.pyplot")


def genDomainsCustomGeneratorMapping(attrName):
	def func(ds):
		ar = getattr(ds, attrName)
		return range(ar.base, len(ar) + 1)

	return (attrName[:-1] + "_id", func)  # models -> model_id


specialDomainsMapping = dict((genDomainsCustomGeneratorMapping(attrName) for attrName in Dataset.indexes))


class Analysis:
	"""A class to make analysis. Call its methods in a Jupyter notebook"""

	def getAvailableKeys(self):
		return self.ds.getAvailableAttrs() - {"id"}

	def loadAndAugmentDataset(self):
		print("Using the DB " + str(self.dbPath) + "...")
		print("Getting index....")
		ds = Dataset(self.dbPath)
		print("Augmenting...")
		ds.augment()
		return ds

	CACHE_NAMESPACE = "analysis"

	def __init__(self, dbFilePath):
		self.dbPath = dbFilePath

		usual = {"duration_col": "duration_worked", "event_col": "failed"}
		learningTasksCreators = {
			#"taskName": (Aggregator, params to fit),
			"unaggregated": (None, usual),
			"aggregated": (MeanAggregator, usual),
			"Weibull": (WeibullAggregator, {"lambda_col": "weibullLambda", "rho_col": "weibullRho"}),
		}
		#print(learningTasksCreators)

		def generateDataSetConstructor(aggr, paramsToFit):
			return LearningTask(self.createDataFrame(self.getAvailableKeys(), aggregator=aggr), paramsToFit)

		tasksPchConfig = {k + "Task": partial(generateDataSetConstructor, aggr, paramsToFit) for k, (aggr, paramsToFit) in learningTasksCreators.items()}
		# print(tasksPchConfig)

		self.pch = PickleCache({"ds": self.loadAndAugmentDataset, **tasksPchConfig}, self.__class__.CACHE_NAMESPACE)
		self.domains = {}

	@property
	def ds(self):
		return self.pch.ds

	#@property
	#def domains(self):
	#	return self.pch.domains

	def getSetOfValues(self, modelAttr):
		"""Gives a set of values of the attribute with passed name. Use it with categorial attributes."""
		res = set(filter(lambda x: x is not None, {m[modelAttr] for m in self.ds.models if modelAttr in m}))
		try:
			res = sorted(res)
		except TypeError:
			pass
		return set(res)

	def computeDomains(self, *attrs):
		for attrName in attrs:
			if attrName in specialDomainsMapping:
				self.domains[attrName] = specialDomainsMapping[attrName](self.ds)
			else:
				self.domains[attrName] = self.getSetOfValues(attrName)
			#print("domains", attrName, self.domains[attrName])

	def loadStats(self):
		print("The database contains " + ("reduced" if self.ds.reduced else "full") + " dataset")
		print("Getting stats from dataset....")
		statz = Dataset.stats(self.dbPath, reduced=self.ds.reduced)

		print("Creating a dataframe from stats....")
		pds = pandas.DataFrame.from_records(statz, index="id")
		del statz
		try:
			pds["model_id"] = pds.index.map(lambda id: self.ds.drives[id]["model_id"])
		except IndexError as ex:
			warnings.warn("The drive present in statistic data is not present in tables with info on models. Make sure that you use the right version of the DB. For example you may need to export drives information with `export drives`")
			raise

		if not self.ds.reduced:
			pds["failed"] = pds.loc[:, "failure_worked_days_smart"].notnull()
			pds["duration_worked"] = pds.loc[:, "failure_worked_days_smart"].where(pds.loc[:, "failed"], pds.loc[:, "total_worked_days_smart"])
		else:
			pds["failed"] = pds.loc[:, "days_in_dataset_failure"].notnull()
			pds["duration_worked"] = pds.loc[:, "days_in_dataset_failure"].where(pds.loc[:, "failed"], pds.loc[:, "days_in_dataset"])

		return pds

	def loadModelsDataFrame(self):
		pds = pandas.DataFrame.from_records(self.ds.models, index="id")
		pds["vendor_id"] = pds.loc[:, "brand_id"].apply(lambda id: self.ds.brands[id]["vendor_id"])
		return pds

	def createDataFrame(self, additionalAttrs, *, aggregator: Aggregator = None):
		"""Initializes PandasDataFrame with the data from dataset and does some other additional operations."""
		self.computeDomains(*additionalAttrs)

		pds = self.loadStats()
		models = self.loadModelsDataFrame()

		if aggregator is not None:
			pds = aggregator.aggregate(pds)

		missingAttrColumns = additionalAttrs - set(models.columns)
		if missingAttrColumns:
			warnings.warn("Following columns are missing: " + repr(missingAttrColumns))
		pds = pds.merge(models.loc[:, additionalAttrs], how="inner", left_on="model_id", right_index=True, suffixes=("", "_model"), copy=False)
		#print(pds["vendor_id"])

		for attrName in additionalAttrs:
			if attrName not in self.domains:
				self.computeDomains(attrName)

		if "duration_worked" in pds:
			pds.sort_values(by="duration_worked", inplace=True)
		return pds

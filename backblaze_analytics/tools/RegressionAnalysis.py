import math
import typing
import warnings
from collections import OrderedDict
from gc import collect
from pathlib import Path

import lazily.lifelines.utils
from lazily import lifelines
from lazily import numpy as np
from lazily import pandas
from NoSuspend import NoSuspend

from .. import augment, database
from ..analysis import Analysis, LearningTask
from ..datasetDescription import spec
from ..fitters.XGBoostCoxPHFitter import XGBoostCoxPHFitter
from ..fitters.XGBoostWeibullFitter import XGBoostWeibullFitter
from ..utils.PickleCache import PickleCache
from ..utils.reorderPandasDataframeColumns import reorderPandasDataframeColumns


def explainItemUsingSHAP(shapValues: "pandas.Series", thresholdRatio=50.0):
	normConst = shapValues.sum()
	shapValues /= normConst
	shapValsSignificance = shapValues.abs().sort_values(ascending=False)
	minThresh = shapValsSignificance[0] / thresholdRatio
	selector = shapValsSignificance > minThresh
	shapValsSignificance = shapValsSignificance[selector]
	significantShaps = shapValues[shapValsSignificance.index]
	resDict = significantShaps.to_dict()
	resDict["$other"] = shapValues[~selector].sum()
	return resDict


def explainItemsUsingSHAP(shapValues: "pandas.DataFrame", thresholdRatio=50.0):
	res = []
	for idx, el in shapValues.iterrows():
		res.append(explainItemUsingSHAP(el, thresholdRatio))
	return res


class RegressionAnalysis(Analysis):
	"""A class to make analysis. Call its methods in a Jupyter notebook"""

	fitterClass = XGBoostCoxPHFitter

	def smartSample(self, frac: float) -> LearningTask:
		"""XGBoost may fail if all the records are used, presumably because of memory or overflow. Here we try to sample the rows in the way keep the most informative samples:
			* the one that have failed.
			* ~~the one having the longest life~~ (No, this way we can miss the info about medium-lived drives)"""
		assert frac <= 1.0 and frac > 0.0

		if frac == 1.0:
			print("frac=1., keeping all the drives")
			len(self.task.pds)  # to trigger unlazing and unwrapping. TODO: Should we use __wrapped__ here?
			return self.task

		failedSelector = self.task.pds.loc[:, "failed"]
		pdsFailed = self.task.pds.loc[failedSelector]
		pdsAlive = self.task.pds.loc[~failedSelector]

		totalCount = len(self.task.pds)
		drivesToKeep = int(np.floor(totalCount * frac))
		failedCount = len(pdsFailed)
		aliveDrivesToKeep = drivesToKeep - failedCount

		if aliveDrivesToKeep > 0:
			print("All the failed drives (", failedCount / totalCount, " of the dataset) fit into the frac, using", aliveDrivesToKeep / len(pdsAlive), "of censored drives")
			keptAliveDrives = pdsAlive.sample(n=aliveDrivesToKeep)
			pds = pandas.concat([pdsFailed, keptAliveDrives], axis=0)
		else:
			print("Not all the failed drives (", failedCount / totalCount, " of the dataset) fit into the frac, using", (aliveDrivesToKeep - failedCount) / failedCount, " of failed drives")
			pds = pdsFailed.sample(n=failedCount + aliveDrivesToKeep, axis=0)
		return LearningTask(pds, self.task.learnedVars)

	taskName = "unaggregatedTask"

	@property
	def task(self):
		"""To save memory we remove the non-preprocessed data and replace them with the preprocessed ones. Raw data we evict from memory."""
		if self.pch is not None:  # if not evicted
			return getattr(self.pch, self.__class__.taskName)
		else:
			return self._task  # if evicted

	def __init__(self, dbFilePath: Path, frac: float = 1.0, prefix="./Survival_XGBoost_Models"):
		self.spec = None
		super().__init__(dbFilePath)
		self._task = self.smartSample(frac)
		#self.pch = None  # to conserve memory

		self.engineerFeatures(self.task)

		collect()
		self.f = self.__class__.fitterClass(self.spec, prefix=prefix)

	def engineerFeatures(self, task):
		n = "numerical"
		c = "categorical"
		if self.spec is None:
			self.spec = type(spec)(spec)
			#self.spec["brand"] = c
			#self.spec["vendor"] = c
			#self.spec["form_factor_crossection_side"] = n
			#self.spec["form_factor_crossection_front"] = n
			#self.spec["form_factor_volume"] = n
			#self.spec["form_factor_crossection_top"] = n
			#self.spec["platter_linear_speed"] = n
			#self.spec["platter_separation"] = n

		#task.pds.loc[:, "platter_linear_speed"] = task.pds.loc[:, ["form_factor_width", "form_factor_depth"]].min(axis=1) / 2 * task.pds.loc[:, "rpm"] ** 2
		#task.pds.loc[:, "platter_separation"] = task.pds.loc[:, "form_factor_height"] / task.pds.loc[:, "platters"] ** 2
		collect()
		#task.pds.loc[:, "form_factor_volume"] = task.pds.loc[:, ["form_factor_width", "form_factor_height", "form_factor_depth"]].product(1)
		#task.pds.loc[:, "form_factor_crossection_front"] = task.pds.loc[:, ["form_factor_width", "form_factor_height"]].product(1)
		#task.pds.loc[:, "form_factor_crossection_side"] = task.pds.loc[:, ["form_factor_height", "form_factor_depth"]].product(1)
		#task.pds.loc[:, "form_factor_crossection_top"] = task.pds.loc[:, ["form_factor_width", "form_factor_depth"]].product(1)

		#task.pds.loc[:, "interface"] = task.pds.loc[:, "interface"].apply(lambda e: e if e != "600" else "SATA")
		#task.pds.loc[:, "brand"] = task.pds.loc[:, "brand_id"].map(lambda id: self.ds.brands[id]["name"])
		#task.pds.loc[task.pds.loc[:, "variable_rpm"] == True, "variable_rpm"] = 1.0
		#task.pds.loc[task.pds.loc[:, "variable_rpm"] == False, "variable_rpm"] = 0.0
		#task.pds.loc[task.pds.loc[:, "variable_rpm"].isnull(), "variable_rpm"] = math.nan

	def getDefaultHyperparams(self):
		raise NotImplementedError()

	def _selfTest(self):
		"""Redefine this method to return something from XGBoost model in order to check that XGBoost works as needed"""
		raise NotImplementedError()

	def selfTest(self, predicted=None):
		print("Testing that XGBoost is not broken on this count of rows...")
		if predicted is None:
			predicted = self._selfTest()

		countOfNans = predicted.isna().sum()[0]
		if countOfNans:
			raise Exception("Fucking shit. For this fraction of rows XGBoost fails. The result contains " + str(countOfNans) + " nans")

		print("Self-test has passed successfully")

	def optimizeHyperparams(self, iters=10000, optimizer=None, pointsStorageFileName: Path = None, finalCvFolds=2):
		with NoSuspend():
			#self.selfTest()
			self.f.optimizeHyperparams(self.task.pds, *self.task.learnedVars.values(), iters=iters, optimizer=optimizer)
			print("Concordance via cv: ", self.evaluateModel(finalCvFolds))

	def evaluateModel(self, folds: int = 2):
		pds = self.task.pds
		d = self.f.crossvalidate(pds, folds)
		return (np.mean(d), np.std(d))

	def trainModel(self, format="binary"):
		with NoSuspend():
			#self.f.fit(self.task.pds, weights_col="$aggregateWeight", saveLoadModel=False, format=format, **self.task.learnedVars)  # Aggregator.weightCol
			self.f.fit(self.task.pds, saveLoadModel=False, format=format, **self.task.learnedVars)  # Aggregator.weightCol
			#predicted = self.f.predict_log_partial_hazard(self.task.pds)
			#self.selfTest(predicted)

	def loadModel(self, format="binary"):
		self.f.fit(self.task.pds, saveLoadModel=True, format=format, **self.task.learnedVars)

	def preparePredictionOfUnknownModelsSurvival(self, models):
		models = augment.augment(models)
		msdf = pandas.DataFrame.from_records(models)
		self.engineerFeatures(msdf)
		return msdf

	def predictUnknownModelsSurvival(self, models, explain: float = 10.0):
		msdf = self.preparePredictionOfUnknownModelsSurvival(models)
		predictionRes = self.f.predictExpectation(msdf, SHAPInteractions=(False if explain else None))
		assert "predicted_survival" in predictionRes.columns
		msdf = pandas.concat([msdf.loc[:, :], predictionRes], axis=1)

		res = msdf.sort_values("predicted_survival", ascending=False)
		explainations = None

		if explain:
			if self.f.explainations is not None:
				usedSHAPValues = self.f.explainations[0]
				usedSHAPValues = usedSHAPValues.iloc[res.index]

				meanShapMeasure = np.sqrt((usedSHAPValues ** 2).mean(axis=0)) * np.sign(usedSHAPValues.mean(axis=0))
				meanExpls = explainItemUsingSHAP(meanShapMeasure, np.inf)

				explainations = explainItemsUsingSHAP(usedSHAPValues)
				res = reorderPandasDataframeColumns(res, meanExpls.keys())
			else:
				warnings.warn("Explainations are None. Probably unimplemented.")
				explainations = None
				meanExpls = None

		return res, explainations, meanExpls


class CoxAnalysis(RegressionAnalysis):
	fitterClass = XGBoostCoxPHFitter
	#taskName = "aggregatedTask"
	taskName = "unaggregatedTask"

	def _selfTest(self):
		self.f.hyperparams = self.getDefaultHyperparams()
		self.f.fit(self.task.pds, saveLoadModel=None, **self.task.learnedVars)
		return self.f.predict_log_partial_hazard(self.task.pds)

	def getDefaultHyperparams(self):
		return {
			"colsample_bytree": 0.852646728688084,
			"learning_rate": 0.44999999970000004,
			"max_depth": 5,
			"min_child_weight": 0.00036131818682314773,
			"min_split_loss": 1.7193612317506807e-05,
			"reg_alpha": 0.002685834045538217,
			"subsample": 0.99999999881,
			"num_boost_round": 34
		}


class WeibullAnalysis(RegressionAnalysis):
	fitterClass = XGBoostWeibullFitter
	taskName = "WeibullTask"

	def _selfTest(self):
		return
		self.f.hyperparams = self.getDefaultHyperparams()
		self.f.fit(self.task, saveLoadModel=None, **self.task.learnedVars)
		return self.f.predict_log_partial_hazard(self.task)

	def getDefaultHyperparams(self):
		return {
			"colsample_bytree": 0.852646728688084,
			"learning_rate": 0.44999999970000004,
			"max_depth": 5,
			"min_child_weight": 0.00036131818682314773,
			"min_split_loss": 1.7193612317506807e-05,
			"reg_alpha": 0.002685834045538217,
			"subsample": 0.99999999881,
			"num_boost_round": 34
		}

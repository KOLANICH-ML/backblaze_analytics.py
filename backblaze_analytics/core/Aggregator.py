import typing
import warnings

import numpy as np
import pandas
from lazily.scipy.stats import gmean


class AggregationException(Exception):
	pass


AggregatorFuncT = typing.Callable[[pandas.DataFrame, float], dict]


class Aggregator:
	columnsToGroupBy = None
	columnsToAggregate = None
	weightCol = "$aggregateWeight"
	aggregationFunc = None

	@classmethod
	def _aggregate(cls, pds: pandas.DataFrame, modelAggregator: AggregatorFuncT, laplaceEstimatorZeroFix: float = 0.25):
		def a(pds):
			#print(len(pds))
			try:
				#res = pandas.DataFrame.from_records([{**pds.loc[:, cls.columnsToGroupBy].iloc[0].to_dict(), **modelAggregator(pds, laplaceEstimatorZeroFix)}])
				aggrV = modelAggregator(pds, laplaceEstimatorZeroFix)
				aggrV[cls.weightCol] = len(pds)
				res = pandas.DataFrame.from_records([aggrV])
				#print(res)
				return res
			except AggregationException as ex:
				warnings.warn("Item with group ID " + repr(pds.loc[:, cls.columnsToGroupBy].iloc[0].to_dict()) + " was not imported because " + str(ex))
			#except Exception as ex:
			#	print(len(pds))
			#	print(pds.loc[:, cls.columnsToGroupBy])
			#	print("Exception in aggregator, fucked up group id is ", repr(pds.loc[:, cls.columnsToGroupBy].iloc[0].to_dict()))
			#	raise

		#pds = pds.loc[:, [*cls.columnsToGroupBy, *cls.columnsToAggregate]]  # the rest of stuff makes no sense when averaged
		assert set(pds.columns) & set(cls.columnsToGroupBy)
		#print("cls.columnsToGroupBy 2", cls.columnsToGroupBy)
		res = pds.groupby(cls.columnsToGroupBy).apply(a)
		res = res.reset_index(-1, True)
		res = res.reset_index()
		return res

	@classmethod
	def aggregate(cls, pds, laplaceEstimatorZeroFix: float = 0.25):
		assert cls.aggregationFunc is not None, cls.__name__ + " doesn't contain an `aggregationFunc`, use `_aggregate` instead"
		return cls._aggregate(pds, cls.aggregationFunc, laplaceEstimatorZeroFix)


class StrataAggregator(Aggregator):
	columnsToGroupBy = ["model_id", "failed"]
	columnsToAggregate = ["duration_worked"]


class ParametricAggregator(Aggregator):
	columnsToGroupBy = ["model_id"]
	columnsToAggregate = ["duration_worked", "failed"]


class MeanAggregator(StrataAggregator):
	@staticmethod
	def aggregationFunc(pds, laplaceEstimatorZeroFix):
		durations = pds.loc[:, "duration_worked"].values
		durations[durations == 0.0] = laplaceEstimatorZeroFix
		return {"duration_worked": np.mean(durations)}


class GMeanAggregator(StrataAggregator):
	@staticmethod
	def aggregationFunc(pds, laplaceEstimatorZeroFix):
		durations = pds.loc[:, "duration_worked"].values
		durations[durations == 0.0] = laplaceEstimatorZeroFix
		return {"duration_worked": gmean(durations)}

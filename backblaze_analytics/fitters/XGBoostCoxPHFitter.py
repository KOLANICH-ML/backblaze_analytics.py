from lifelines.fitters.coxph_fitter import CoxPHFitter
from lifelines.utils import _get_index, k_fold_cross_validation

__author__ = "KOLANICH"

__license__ = "MIT"
__copyright__ = """MIT License

Copyright (c) 2017 Cameron Davidson-Pilon

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import typing

import numpy as np
import pandas
from AutoXGBoost import AutoXGBoost
from Chassis import Chassis


class XGBoostCoxPHFitter(CoxPHFitter):

	"""
	This class implements fitting Cox's proportional hazard model using XGBoost `cox:survival` objective contributed by @slundberg.
	This module uses some libraries like Chassis and AutoXGBoost"""

	def __init__(self, spec, hyperparams=None, alpha=0.95, durationColPostfix="_prep", prefix=None):
		if not (0.0 < alpha <= 1.0):
			raise ValueError("alpha parameter must be between 0 and 1.")

		self.alpha = alpha
		self.initialSpec = spec
		self.hyperparams = hyperparams
		self._defaultDurationCol = None
		self._SHAPExplaination = None  # a workaround of missing argument
		self.explainations = None
		self.strata = None  # to make CoxPHFitter happy
		self.prefix = prefix
		for k, v in spec.items():
			if v == "survival":
				self._defaultDurationCol = k
				break

	def prepareFitting(self, df, duration_col=None, event_col=None, weights_col=None):
		self.spec = type(self.initialSpec)(self.initialSpec)

		df = df.copy()
		if duration_col:
			#df = df.sort_values(by=duration_col)
			pass

		duration_col_transformed = None
		#print("\x1b[31m", "event_col", event_col, "\x1b[0m")
		if event_col is not None:
			#print("\x1b[31m", "duration_col", duration_col, "\x1b[0m")
			if duration_col is None:
				if self._defaultDurationCol:
					duration_col_transformed = self._defaultDurationCol
			else:
				if self.spec[duration_col] == "survival":  # the shit is already supplied transformed
					duration_col_transformed = duration_col
				elif self.spec[duration_col] in {"numerical", "stop"}:
					duration_col_transformed = duration_col + "_prep"

			#print("\x1b[31m", "duration_col_transformed not in df.columns", duration_col_transformed not in df.columns, "\x1b[0m")
			if duration_col_transformed not in df.columns:
				df.loc[:, duration_col_transformed] = df.loc[:, duration_col] * (df.loc[:, event_col] * 2 - 1)
				#print("\x1b[31m", "df.loc[:, duration_col_transformed]", df.loc[:, duration_col_transformed], "\x1b[0m")

			self.spec[duration_col] = "stop"
			self.spec[event_col] = "stop"
		else:
			assert duration_col is not None
			duration_col_transformed = duration_col

		self.duration_col_transformed = duration_col_transformed
		self.spec[self.duration_col_transformed] = "survival"

		if weights_col:
			self.spec[weights_col] = "weight"

		#print(df)
		return AutoXGBoost(self.spec, df, prefix=self.prefix)

	def optimizeHyperparams(self, df, duration_col=None, event_col=None, weights_col=None, show_progress=False, autoSave: bool = True, folds: int = 10, iters: int = 1000, jobs: int = None, optimizer: "UniOpt.core.Optimizer" = None, force: typing.Optional[bool] = None, *args, **kwargs):
		print(df)
		self.axg = self.prepareFitting(df, duration_col=duration_col, event_col=event_col, weights_col=weights_col)

		self.axg.optimizeHyperparams(columns={self.duration_col_transformed}, autoSave=autoSave, folds=folds, iters=iters, jobs=jobs, optimizer=optimizer, force=force, *args, **kwargs)

	def _preprocess_dataframe(self, duration_col, event_col, weights_col):
		E = self.axg.select(columns={event_col})[event_col]
		T = self.axg.select(columns={duration_col})[duration_col]
		X = self.axg.prepareCovariates(self.duration_col_transformed)
		W = self.axg.weights if weights_col is not None else pandas.Series(np.ones((len(X),)), index=X.index, name="$aggregateWeight")

		return X, T, E, W, None, None

	def fit(self, df, duration_col=None, event_col=None, show_progress=True, initial_point=None, weights_col=None, saveLoadModel=None, format="binary"):
		"""
		Fit the XGBoost Cox Propertional Hazard model to a dataset.

		Parameters
		----------
		df: DataFrame
			a Pandas DataFrame with necessary columns `duration_col` and `event_col` (see below), covariates columns, and special columns (weights, strata).
			`duration_col` refers to the lifetimes of the subjects. `event_col` refers to whether the 'death' events was observed: 1 if observed, 0 else (censored).

		duration_col: string
			the name of the column in DataFrame that contains the subjects' lifetimes.

		event_col: string, optional
			the  name of thecolumn in DataFrame that contains the subjects' death observation. If left as None, assume all individuals are uncensored.

		weights_col: string, optional
			an optional column in the DataFrame, df, that denotes the weight per subject.
			This column is expelled and not used as a covariate, but as a weight in the final regression. Default weight is 1.
			This can be used for case-weights. For example, a weight of 2 means there were two subjects with identical observations.
			This can be used for sampling weights. In that case, use `robust=True` to get more accurate standard errors.

		show_progress: boolean, optional (default=False)
			since the fitter is iterative, show convergence diagnostics. Useful if convergence is failing.

		initial_point: (d,) numpy array, optional
			initialize the starting point of the iterative algorithm. Default is the zero vector.

		Returns
		-------
		self: CoxPHFitter
			self with additional new properties: ``print_summary``, ``hazards_``, ``confidence_intervals_``, ``baseline_survival_``, etc."""
		self.axg = self.prepareFitting(df, duration_col=duration_col, event_col=event_col, weights_col=weights_col)
		assert self.duration_col_transformed

		if self.hyperparams is not None:
			self.axg.bestHyperparams = self.hyperparams
		else:
			self.axg.loadHyperparams()

		if saveLoadModel is True:
			self.axg.loadModel(cn=self.duration_col_transformed, format=format)
		else:
			#print(df[self.duration_col_transformed])
			self.axg.trainModels((self.duration_col_transformed,))

			if saveLoadModel is False:
				self.axg.models[self.duration_col_transformed].save(format=format)

		#self.confidence_intervals_ = self._compute_confidence_intervals()

		X, T, E, W, original_index, _clusters = self._preprocess_dataframe(duration_col, event_col, weights_col)

		self._predicted_partial_hazards_ = (
			self.predict_partial_hazard(X)
			.rename(columns={0: "P"})
			.assign(T=T, E=E, W=W)
			.set_index(X.index)
		)

		self.baseline_hazard_ = self._compute_baseline_hazards()
		self.baseline_cumulative_hazard_ = self._compute_baseline_cumulative_hazard()
		#self.baseline_survival_ = self._compute_baseline_survival()
		#self.score_ = concordance_index(self.durations, -self.baseline_survival_, self.event_observed)
		return self

	def _SHAPExplainationMissingArgumentWorkaround(self, SHAPInteractions):
		if SHAPInteractions is not None:
			assert self._SHAPExplaination is None
			self._SHAPExplaination = SHAPInteractions

	def predict_partial_hazard(self, X, SHAPInteractions=None):
		self._SHAPExplainationMissingArgumentWorkaround(SHAPInteractions)
		return super().predict_partial_hazard(X)

	def predict_log_hazard_relative_to_mean(self, X, SHAPInteractions=None):
		self._SHAPExplainationMissingArgumentWorkaround(SHAPInteractions)
		return super().predict_log_hazard_relative_to_mean(X)

	def predict_expectation(self, X, SHAPInteractions=None):
		"""lifelines-expected function to predict expectation"""
		self._SHAPExplainationMissingArgumentWorkaround(SHAPInteractions)
		return super().predict_expectation(X)

	def predictExpectation(self, X, SHAPInteractions=None):
		"""our function to predict expectation"""
		res = self.predict_expectation(X, SHAPInteractions)[0]
		res.name = "predicted_survival"
		return pandas.DataFrame(res)

	def crossvalidate(self, pds, folds: int):
		return k_fold_cross_validation(self, pds, duration_col="duration_worked", event_col="failed", k=folds)

	def predict_log_partial_hazard(self, X, SHAPInteractions=None):
		if not isinstance(X, Chassis):
			dmat = AutoXGBoost(self.spec, X)
		else:
			dmat = X

		shouldDelete = self.duration_col_transformed not in X
		#print("\x1b[31m", "shouldDelete", shouldDelete, "\x1b[0m")
		#print("\x1b[31m", "dmat.pds.loc[:, self.duration_col_transformed]", dmat.pds.loc[:, self.duration_col_transformed], "\x1b[0m")
		#from traceback import print_stack

		#print_stack()

		if SHAPInteractions is not None:
			assert self._SHAPExplaination is None
		else:
			SHAPInteractions = self._SHAPExplaination
			self._SHAPExplaination = None

		res = self.axg.predict(self.duration_col_transformed, dmat, returnPandas=True, SHAPInteractions=SHAPInteractions)

		if SHAPInteractions is None:
			self.explainations = None
		else:
			res, self.explainations = res

		if shouldDelete and self.duration_col_transformed in X:
			del X[self.duration_col_transformed]

		res.name = 0
		fRes = pandas.DataFrame(res)
		#print(fRes)
		return fRes

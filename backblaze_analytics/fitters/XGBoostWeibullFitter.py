from lifelines.fitters.weibull_fitter import WeibullFitter
from lifelines.utils import _get_index, concordance_index

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
import scipy.special as spcl
from AutoXGBoost import AutoXGBoost, MultiColumnModelType
from Chassis import Chassis
from lazy_object_proxy import Proxy
from lifelines.fitters.weibull_fitter import WeibullFitter
from lifelines.utils import ConvergenceError
from lifelines.utils.sklearn_adapter import LifelinesSKLearnAdapter
from sklearn.model_selection import cross_validate

from ..core.Aggregator import AggregationException, ParametricAggregator

# Λ(x) = (T/λ)**ρ
# λ(x) = (T/λ)**(ρ−1)*(ρ/λ)
# F(x) = 1-exp(-(T/λ)**ρ)
# f(x) = (ρ/λ)*(T/λ)**(ρ-1)*exp(−(T/λ)**ρ)
# log f = E*(ρ·log(T/λ) + log(ρ)−(T/λ)**ρ)
# discrete log f = E·log( exp( λ**−ρ * ( (T+1)**ρ - T**ρ ) ) - 1 ) − λ**-ρ * (T+1)**ρ
# Kullback-Leibler divergence(λ, ρ, λ', ρ') = log(ρ'/λ'**ρ') - log(ρ/λ**ρ) + (ρ' - ρ)*(log(λ') - np.euler_gamma/ρ') + (λ'/λ)**ρ * spcl.gamma(ρ/ρ' + 1) - 1 # https://arxiv.org/pdf/1310.3713.pdf


def weibullExpectation(lam, rho):
	return lam * spcl.gamma(1 + 1 / rho)


def WeibullKL_divergence(λ, ρ, λ_true, ρ_true):
	"""Kullback-Leibler divergence according to https://arxiv.org/pdf/1310.3713.pdf"""
	return np.log(ρ_true / λ_true ** ρ_true) - np.log(ρ / λ ** ρ) + (ρ_true - ρ) * (np.log(λ_true) - np.euler_gamma / ρ_true) + (λ_true / λ) ** ρ * spcl.gamma(ρ / ρ_true + 1) - 1


def WeibullKL_divergence_rho(ρ, ρ_true):
	"""Kullback-Leibler divergence according to https://arxiv.org/pdf/1310.3713.pdf"""
	rr = ρ / ρ_true
	return -np.log(rr) - (1 - rr) * np.euler_gamma + spcl.gamma(rr + 1) - 1


def WeibullKL_divergence_lambda(λ, λ_true, ρ_true):
	"""Kullback-Leibler divergence according to https://arxiv.org/pdf/1310.3713.pdf"""
	lr = λ_true / λ
	return lr ** ρ_true - ρ_true * np.log(lr) - 1


lambdas_margin = rhos_margin = 1


def WeibullKLReg(rho_lambda, rho_lambda_true):
	lambdas = np.array(rho_lambda[:, 1], dtype=np.complex128)
	rhos = rho_lambda[:, 0]
	lambdas_true = rho_lambda_true[:, 1]
	rhos_true = rho_lambda_true[:, 0]
	return WeibullKL_divergence(lambdas, rhos, lambdas_true, rhos_true) + np.exp(lambdas_margin - lambdas) + np.exp(rhos_margin - rhos)


def WeibullKLGradHess(lambdas, rhos, lambdas_true, rhos_true):
	"""Gradient of Kullback-Leibler divergence with exp regularization to prevent from going into forbidden regions (<0)"""

	rr = rhos / rhos_true
	rg = spcl.gamma(rr + 1)
	rpg = spcl.polygamma(0, rr + 1)
	rpg1 = spcl.polygamma(1, rr + 1)
	rpgprt = rpg / rhos_true

	lr = lambdas_true / lambdas
	ll = np.log(rhos)
	lll = np.log(lr)
	llt = np.log(rhos_true)
	lrr = lr ** rhos * rg

	klreg_rhos = np.euler_gamma / rhos_true + ll - llt + rg * rpgprt * (np.e * lr) ** rhos * np.exp(-rhos) + rg * (np.e * lr) ** rhos * np.exp(-rhos) * np.log(lr) - np.exp(lambdas_margin - rhos) - 1.0 / rhos

	klreg_lambdas = (-lrr * rhos - lambdas * np.exp(-lambdas + rhos_margin + np.exp(-lambdas + rhos_margin)) + rhos) / lambdas

	klreg_rhorho = (rg * rhos ** 2 * rhos_true ** 2 * (lr * np.exp(2)) ** rhos * (rpgprt * (np.log(lr) + 1) + rpgprt * np.log(lr) - rpgprt + (np.log(lr) + 1) * np.log(lr) - np.log(lr)) + rg * rhos ** 2 * (lr * np.exp(2)) ** rhos * (rpg ** 2 + rpg1) + rhos ** 2 * rhos_true ** 2 * np.exp(lambdas_margin + rhos) + rhos_true ** 2 * np.exp(2 * rhos)) * np.exp(-2 * rhos) / (rhos ** 2 * rhos_true ** 2)

	klreg_lambdarho = klreg_rholambda = -(lrr * rpg * rhos + rhos_true * (lrr * (1.0 + rhos * np.log(lr)) - 1.0)) / (lambdas * rhos_true)

	klreg_lambdalambda = (-1.0 + np.exp(-rhos)) * np.exp(-lambdas) + rhos * ((rhos + 1.0) * lr ** rhos - 1.0) / lambdas ** 2

	g = np.real(
		np.array(
			[klreg_rhos, klreg_lambdas],
		)
	)  # 1, 2, item_no
	h = np.real(np.array([
		[klreg_rhorho, klreg_rholambda],
		[klreg_lambdarho, klreg_lambdalambda]
	]))  # 1, 2, item_no 
	h = np.transpose(h, (2, 0, 1))  # item_no, 1, 2
	g = np.transpose(g, (1, 0))
	return g, h


def WeibullKLGradHess_rho(rhos, rhos_true):
	rr = rhos / rhos_true
	rg = spcl.gamma(rr + 1)
	rpg = spcl.polygamma(0, rr + 1)
	rpg1 = spcl.polygamma(1, rr + 1)
	rpgprt = rpg / rhos_true
	e2r = np.exp(2 * rhos)

	ll = np.log(rhos)
	llt = np.log(rhos_true)

	klreg_rhos = np.euler_gamma / rhos_true + ll - llt + rg * rpgprt * (np.e * 1) ** rhos * np.exp(-rhos) - np.exp(rhos_margin - rhos) - 1.0 / rhos
	klreg_rhorho = (rg * rhos ** 2 * e2r * (rpg ** 2 + rpg1) + rhos ** 2 * rhos_true ** 2 * np.exp(rhos_margin + rhos) + rhos_true ** 2 * e2r) * np.exp(-2 * rhos) / (rhos ** 2 * rhos_true ** 2)

	return klreg_rhos, klreg_rhorho


pg02 = spcl.polygamma(0, 1 + 1)
pg12 = spcl.polygamma(1, 1 + 1)


def WeibullKLGradHess_lambda(lambdas, lambdas_true, rhos_true):
	rpg = pg02
	rpg1 = pg12
	lr = lambdas_true / lambdas

	klreg_lambdas = ((1.0 - lr ** rhos_true) * rhos_true - lambdas * np.exp(-lambdas + rhos_margin + np.exp(-lambdas + rhos_margin))) / lambdas
	klreg_lambdalambda = (-1.0 + np.exp(-rhos_true)) * np.exp(-lambdas) + rhos_true * ((rhos_true + 1.0) * lr ** rhos_true - 1.0) / lambdas ** 2
	return klreg_lambdas, klreg_lambdalambda


class WeibullFitterWithMean(WeibullFitter):
	def predict_expectation(self):
		return weibullExpectation(self._lambda, self._rho)


defaultRhoColName = "weibullRho"
defaultLambdaColName = "weibullLambda"


def getWeibulAggregator():
	f = WeibullFitterWithMean()

	class WeibullAggregator(ParametricAggregator):
		@classmethod
		def aggregationFunc(cls, pds, laplaceEstimatorZeroFix):
			print("cls.columnsToAggregate", cls.columnsToAggregate)
			durationColName = cls.columnsToAggregate[0]
			eventColName = cls.columnsToAggregate[1]

			durations = pds.loc[:, durationColName].values
			durations[durations == 0.0] = laplaceEstimatorZeroFix

			if len(durations) > 2:
				try:
					f.fit(durations, pds.loc[:, eventColName])
				except ConvergenceError as err:
					raise AggregationException(err)
				if f.rho_ < 1.0e-2:
					raise AggregationException("rho < than 1.e-2, expectation is infinite")

				return {defaultRhoColName: f.rho_, defaultLambdaColName: f.lambda_}
			else:
				raise AggregationException("less than 2 drives histories are available, which is unsuitable for Weibull fitter")

	return WeibullAggregator


WeibullAggregator = Proxy(getWeibulAggregator)


class XGBoostWeibullFitter:

	"""
	This class implements fitting parametric distributions, currently only Weibull one"""

	def __init__(self, spec, hyperparams=None, prefix=None):
		self.initialSpec = spec
		self.spec = None
		self.hyperparams = hyperparams
		self.prefix = prefix
		self._SHAPExplaination = None  # a workaround of missing argument
		self.explainations = None
		self.fitter = WeibullFitterWithMean()
		self.axg = None
		self._rho_col = None
		self.rho_col = defaultRhoColName
		self.lambda_col = defaultLambdaColName

	@property
	def rho_col(self):
		return self._rho_col

	@rho_col.setter
	def rho_col(self, v):
		if v == "weibullLambda":
			import traceback

			traceback.print_stack()
		self._rho_col = v

	def prepareFitting(self, df, lambda_col=None, rho_col=None, weights_col=None, duration_col=None, event_col=None):
		self.spec = type(self.initialSpec)(self.initialSpec)
		if weights_col is None:
			weights_col = WeibullAggregator.weightCol

		if lambda_col is None or rho_col is None:
			# WeibullAggregator assummes columns names to be the ones defined in its class
			assert duration_col is None or duration_col == WeibullAggregator.columnsToAggregate[0], duration_col
			assert event_col is None or event_col == WeibullAggregator.columnsToAggregate[1], event_col

			print("lambda, rho are None, creating")

			df = WeibullAggregator.aggregate(df)
			assert defaultRhoColName in df.columns, tuple(df.columns)
			assert defaultLambdaColName in df.columns, tuple(df.columns)

			rho_col = defaultRhoColName
			lambda_col = defaultLambdaColName

		#print(df)
		assert duration_col not in df.columns
		assert event_col not in df.columns

		self.lambda_col = lambda_col
		self.rho_col = rho_col
		self.spec[lambda_col] = "numerical"
		self.spec[rho_col] = "numerical"

		if weights_col:
			self.spec[weights_col] = "weight"

		return AutoXGBoost(self.spec, df, prefix=self.prefix)

	def optimizeHyperparams(self, df, lambda_col=None, rho_col=None, weights_col=None, show_progress=False, autoSave: bool = True, folds: int = 10, iters: int = 1000, jobs: int = None, optimizer: "UniOpt.core.Optimizer" = None, force: typing.Optional[bool] = None, *args, **kwargs):
		self.axg = self.prepareFitting(df, lambda_col=lambda_col, rho_col=rho_col, weights_col=weights_col)

		(weibullRhoObj, weibullRhoError), (weibullLambdaObj, weibullLambdaError) = self.prepareObjectives(self.axg.pds)

		self.axg.optimizeHyperparams(columns={self.rho_col}, autoSave=autoSave, folds=folds, iters=iters, jobs=jobs, optimizer=optimizer, excludeColumns={self.lambda_col}, force=force, additionalArgsToCV={"obj": weibullRhoObj, "feval": weibullRhoError}, *args, **kwargs)
		self.axg.optimizeHyperparams(columns={self.lambda_col}, autoSave=autoSave, folds=folds, iters=iters, jobs=jobs, optimizer=optimizer, excludeColumns={self.rho_col}, force=force, additionalArgsToCV={"obj": weibullLambdaObj, "feval": weibullLambdaError}, *args, **kwargs)

	def prepareObjectives(self, X):
		trueRhos = X.loc[:, self.rho_col].values
		trueLambdas = X.loc[:, self.lambda_col].values

		def weibullRhoObj(rhos, dtrain):
			trueRhos = dtrain.get_label()
			g, h = WeibullKLGradHess_rho(rhos, trueRhos)
			#print("r g", g)
			#print("r h", h)
			return g, h

		#def weibullRhoError(rhos, dtrain):
		#	trueRhos = dtrain.get_label()
		#	return "weibull-rho-exp-error", np.sqrt(np.mean((weibullExpectation(trueLambdas, rhos) - weibullExpectation(trueLambdas, trueRhos)) ** 2.0))

		def weibullRhoError(rhos, dtrain):
			trueRhos = dtrain.get_label()
			return "weibull-rho-kl-error", np.mean(WeibullKL_divergence_rho(rhos, trueRhos))

		def weibullLambdaObj(lambdas, dtrain):
			trueLambdas = dtrain.get_label()
			g, h = WeibullKLGradHess_lambda(lambdas, trueRhos, trueLambdas)

			print("l g", g)
			print("l h", h)

			finInd = ~np.isfinite(g)
			print(finInd)
			g[finInd] = lambdas[finInd] - trueLambdas[finInd]
			h[finInd] = 1.0

			finInd = ~np.isfinite(h)
			h[finInd] = 1.0

			print("g", g)
			print("h", h)
			return g, h

		#def weibullLambdaError(lambdas, dtrain):
		#	trueLambdas = dtrain.get_label()
		#	return "weibull-lambda-exp-error", np.sqrt(np.mean((weibullExpectation(lambdas, trueRhos) - weibullExpectation(trueLambdas, trueRhos)) ** 2.0))

		def weibullLambdaError(lambdas, dtrain):
			trueLambdas = dtrain.get_label()
			return "weibull-lambda-kl-error", np.mean(WeibullKL_divergence_lambda(lambdas, trueLambdas, trueRhos))

		return (weibullRhoObj, weibullRhoError), (weibullLambdaObj, weibullLambdaError)

	def fit(self, df, lambda_col=None, rho_col=None, show_progress=True, initial_point=None, weights_col=None, saveLoadModel=None, format="binary", *, duration_col=None, event_col=None):
		#print(df)
		self.axg = self.prepareFitting(df, lambda_col=lambda_col, rho_col=rho_col, weights_col=weights_col, duration_col=duration_col, event_col=event_col)

		if self.hyperparams is not None:
			self.axg.bestHyperparams = self.hyperparams
		else:
			self.axg.loadHyperparams()

		if saveLoadModel is True:
			for mn in (self.lambda_col, self.rho_col):
				self.axg.loadModel(cn=mn, format=format)
		else:
			#print(df[(self.lambda_col, self.rho_col)])
			(weibullRhoObj, weibullRhoError), (weibullLambdaObj, weibullLambdaError) = self.prepareObjectives(self.axg.pds)

			self.axg.trainModels(self.rho_col, excludeColumns={self.lambda_col}, obj=weibullRhoObj, feval=weibullRhoError)
			self.axg.trainModels(self.lambda_col, excludeColumns={self.rho_col}, obj=weibullLambdaObj, feval=weibullLambdaError)

			if saveLoadModel is False:
				for mn in (self.lambda_col, self.rho_col):
					self.axg.models[mn].save(format=format)

		return self

	def predict_expectation(self, X, SHAPInteractions=None):
		"""lifelines-expected function to predict expectation"""
		res = self.predictExpectation(X, SHAPInteractions).loc[:, "predicted_survival"]
		res.name = 0
		res = pandas.DataFrame(res)
		return res

	def crossvalidate(self, pds, folds: int):
		a = LifelinesSKLearnAdapter(self, params={"lambda_col": self.lambda_col, "rho_col": self.rho_col}, yArgNames=["lambda_col", "rho_col"], eventArgName=None, etalonFunc=lambda sself, Y: weibullExpectation(Y[sself.fitter.rho_col], Y[sself.fitter.lambda_col]))
		return cross_validate(a, pds, cv=folds, n_jobs=1)["test_score"]  # FUCK PICKLE

	def predictExpectation(self, X, SHAPInteractions=None):
		"""our function to predict expectation"""
		if not isinstance(X, Chassis):
			dmat = AutoXGBoost(self.initialSpec, X)
		else:
			dmat = X

		#if self.axg is None:
		#	self.axg = AutoXGBoost(self.initialSpec, dmat, prefix=self.prefix)
		#	self.axg.loadHyperparams()
		#	for cn in (self.lambda_col, self.rho_col):
		#		self.axg.loadModel(cn=cn)

		res = self.axg.predict((self.lambda_col, self.rho_col), dmat, returnPandas=True, SHAPInteractions=SHAPInteractions)

		if SHAPInteractions is None:
			explainations = None
		else:
			res, explainations = res

		res["predicted_survival"] = weibullExpectation(res[self.rho_col], res[self.lambda_col])

		if SHAPInteractions is not None:
			# TODO: combine
			self.explainations = None

		return res

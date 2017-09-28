import json
import math
from functools import partial
from pprint import pprint

import more_itertools
from lazily import AutoXGBoost, NoSuspend, lazyImport
from lazily import numpy as np
from lazily import pandas
from plumbum import cli

from ..dataset import *
from ..datasetDescription import attrsSpec
from ..utils.PickleCache import PickleCache

plt = lazyImport("matplotlib.pyplot")

#pandas.set_option("display.max_columns", 500)


def modelsManualFix(ds, pds):
	wdId = ds.brandsByName["Western Digital"]["id"]
	pds.loc[pds["brand_id"] != wdId, "Caviar"] = 0  # WD
	pds.loc[pds["brand_id"] != wdId, "Scorpio"] = 0  # WD
	pds.loc[pds["brand_id"] != wdId, "GP"] = 0  # WD
	pds.loc[(pds["brand_id"] == wdId) & (pds.loc[:, "form_factor"] == 3.5) & (pds.loc[:, "Scorpio"].isna()), "Scorpio"] = 0  # WD
	pds.loc[pds["brand_id"] == ds.brandsByName["Hitachi"]["id"], "product_code"] = "N/A"
	pds.loc[pds["brand_id"] == ds.brandsByName["HGST"]["id"], "product_code"] = "N/A"


def prepareModelsForTrainingImputter(dataBaseFileName=None):
	ds = Dataset(dataBaseFileName)
	ds.augment()
	availableKeys = {k for k in more_itertools.flatten((m.keys() for m in ds.models))}
	pds = pandas.DataFrame.from_dict(ds.models)
	modelsManualFix(ds, pds)
	return pds


features = type(attrsSpec)(attrsSpec)
# features.update({'interface_NCQ': "B", 'region': "S"}) # what is it?
params = {
	"booster": "dart",
	"nthread": 4,
	"learning_rate": 0.1,
	"min_split_loss": 0.0,
	"max_depth": 10,
	"max_delta_step": 1,
	"silent": True,
	#"metric":"error"
}


def makeImputer(dataBaseFileName):
	pch = PickleCache({"pds": partial(prepareModelsForTrainingImputter, dataBaseFileName)}, "models")
	ai = AutoXGBoost.AutoXGBoostImputer(features, pch.pds, params)
	columns = set(ai.columns) - {"Caviar", "Scorpio", "GP", "name", "brand_id"}
	return (ai, columns)


class Imputer(cli.Application):
	"""Optimizes hyperparams, trains imputting models and imputs missing data using these models"""

	pass


class ImputerCommand(cli.Application):
	dbPath = cli.SwitchAttr("--db-path", cli.ExistingFile, default="./drives.sqlite", help="Path to the SQLite database")


@Imputer.subcommand("imput")
class ImputCommand(ImputerCommand):
	"""Creates a json file with models with missing attributess imputted"""

	imputedJSONFileName = cli.SwitchAttr("--imputedJSONFileName", cli.ExistingFile, default="./cache/models_imputed.json", help="Path to the resulting json file")

	def main(self):
		(ai, columns) = makeImputer(self.dbPath)
		ai.loadHyperparams()
		ai.loadModels()
		ai.imput()
		res = ai.reverse()
		res = res.to_dict("records")
		res = {r["name"]: r for r in res}
		for kk in res:
			res[kk] = {k: v for k, v in res[kk].items() if (not isinstance(v, float) or not math.isnan(v))}
		with open(self.imputedJSONFileName, "wt", encoding="utf-8") as f:
			json.dump(res, f, indent="\t")


@Imputer.subcommand("train")
class TrainCommand(ImputerCommand):
	"""Trains models"""

	def main(self):
		(ai, columns) = makeImputer(self.dbPath)
		ai.loadHyperparams()
		ai.trainModels(columns=columns)
		ai.scoreModels()
		ai.saveModels()
		pprint(ai.scores)


@Imputer.subcommand("optimize_hyperparams")
class OptimizeHyperparamsCommand(ImputerCommand):
	"""Optimizes hyperparams. Usually takes very long"""

	iterations = cli.SwitchAttr("--iterations", int, default=10000, help="Number of iterations. More iterations - higher the probability to get into better ones.")
	folds = cli.SwitchAttr("--folds", int, default=8, help="number of folds in crossvalidation to evaluate hyperparams")
	jobs = cli.SwitchAttr("--jobs", int, default=2, help="number of processes")
	method = cli.SwitchAttr("--method", cli.switches.Set("hyperopt", "RandomizedSearchCV", case_sensitive=True), default="hyperopt", help="number of processes")
	resume = cli.Flag("--no-resume", default=True, help="do not resume interrupted optimization")

	def main(self):
		(ai, columns) = makeImputer(self.dbPath)
		if self.resume:
			ai.loadHyperparams()
		with NoSuspend():
			ai.optimizeHyperparams(autoSave=True, folds=self.folds, n_iter=self.iterations, n_jobs=self.jobs, method=self.method, columns=columns)
		pprint(ai.scores)


if __name__ == "__main__":
	Imputer.run()

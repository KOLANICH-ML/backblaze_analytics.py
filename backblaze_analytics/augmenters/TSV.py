from datetime import datetime

import dateutil.parser

from ..utils.myTSV import myTSV
from .Augmenter import Augmenter

baseDate = datetime(2000, 1, 1)

whitelist = {"first_known_date"}


def loadModels(additionalDataPath):
	models = {}
	for m in myTSV(additionalDataPath):
		m["model"] = m["model"].upper()
		if "first_known_date" in m:
			m["first_known_date"] = dateutil.parser.parse(m["first_known_date"])
			m["first_known_date"] = (m["first_known_date"] - baseDate).days
		#m["db_id"] = int(m["db_id"])
		models[m["model"]] = type(m)((p for p in m.items() if p[0] in whitelist))
	return models


models = loadModels("./additionalData/models.tsv")


class TSV(Augmenter):
	priority = 1

	def __call__(self, model, vendorName):
		if model["name"] in models:
			model.update(models[model["name"]])
			return True
		else:
			return False

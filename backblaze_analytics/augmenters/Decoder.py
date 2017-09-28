import HDDModelDecoder

from ..utils import flattenDict
from .Augmenter import Augmenter

boolleanOptional = {"variable_rpm"}


class Decoder(Augmenter):
	priority = 0

	def __call__(self, model, vendorName):
		try:
			res = HDDModelDecoder.decodeModel(model["name"], True)
			if res:
				for bName in boolleanOptional - set(res.keys()):
					res[bName] = False
				model.update(flattenDict(res))
				return True
			else:
				return False
		except BaseException:
			return False

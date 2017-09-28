__all__ = ("PickleCache",)

import lzma
import os
from pathlib import Path

from lazy_object_proxy import Proxy

#try:
#	import joblib
#except BaseException:
#	import pickle as joblib
try:
	import _pickle as joblib
except BaseException:
	import pickle as joblib
cacheDir = Path("./cache/pickles")


def makeProxyFunc(name, prefix, creatorFunc):
	prefixDir = cacheDir / prefix
	pickleFileName = prefixDir / (name + ".pickle.xz")

	def proxyFunc():
		if pickleFileName.exists():
			with lzma.open(pickleFileName, "rb") as f:
				res = joblib.load(f)
		else:
			os.makedirs(str(prefixDir), exist_ok=True)
			res = creatorFunc()
			with lzma.open(pickleFileName, "wb") as f:
				joblib.dump(res, f, protocol=-1)
		return res

	return proxyFunc


class PickleCache:
	"""If the object is not cached, creates it and pickles, otherwise loads it from pickle"""

	def __init__(self, creators: dict, prefix=""):
		for name, creator in creators.items():
			setattr(self, name, Proxy(makeProxyFunc(name, prefix, creator)))

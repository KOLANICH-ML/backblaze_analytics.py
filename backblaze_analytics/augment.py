import enum
from collections import Iterable, OrderedDict, defaultdict

from . import augmenters as augmenterModules
from .utils.mtqdm import mtqdm


class Augmenter:
	"""
	A base augmenter class.
	Augmenter is a class adding some data to a drive model."""

	pass


augmenters = OrderedDict()
#print(repr(augmenterModules.__all__))
for modName in augmenterModules.__all__:
	candidate = getattr(getattr(augmenterModules, modName), modName)
	#print(modName, candidate, isinstance(candidate, augmenterModules.Augmenter.Augmenter))
	#if isinstance(candidate, augmenterModules.Augmenter.Augmenter):  # doesn't work by unknown reason
	augmenters[modName] = candidate()
augmenters = type(augmenters)(sorted(augmenters.items(), key=lambda it: it[1].priority))

NotFound = enum.IntFlag("NotFound", list(augmenters.keys()))


def augmentModelDict(model, vendorName):
	"""Augments a model dict"""
	notFound = 0
	r = type(model)(model)
	for name, augmenter in augmenters.items():
		success = augmenter(r, vendorName)
		if not success:
			notFound |= getattr(NotFound, name)
		else:
			model = r
	return (model, notFound)


def augmentDataset(dataset):
	"""Augments data. Returns a report about augmentation."""
	notFound = defaultdict(int)
	progress = mtqdm(dataset.models)
	for m in progress:
		vendorName = dataset.vendors[dataset.brands[m["brand_id"]]["vendor_id"]]["name"]
		(dataset.models[m["id"]], nf) = augmentModelDict(m, vendorName)
		notFound[m["name"]] |= nf
		if notFound[m["name"]]:
			progress.write(m["name"] + ": " + str(notFound[m["name"]]))
			pass
	return notFound


def augmentStandaloneCollection(coll):
	progress = mtqdm(coll)
	for m in progress:
		vendorName = None
		if "brand" in m:
			vendorName = m["brand"]
		(mm, nf) = augmentModelDict(m, vendorName)
		yield (mm)
		if nf:
			progress.write(m["name"] + ": " + str(nf))
			pass


def augment(o):
	if isinstance(o, Iterable) or hasattr(o, "__iter__"):
		return augmentStandaloneCollection(o)
	else:
		return augmentDataset(o)

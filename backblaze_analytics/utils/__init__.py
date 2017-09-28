import collections
import importlib
import platform
import sys
from datetime import datetime, timedelta
from pathlib import Path

import _io
from dateutil.relativedelta import relativedelta
from psutil import virtual_memory

__all__ = ("pathRes", "find7z", "nearestPowerOf2", "flattenDict", "getInterpreterCommand", "fancyTimeDelta")


def fancyTimeDelta(d: timedelta):
	a = datetime.now()
	return relativedelta(a + d, a).normalized()


def pathRes(p: Path):
	"""Finds min length path for a file"""
	p = Path(p)
	return min((f(p) for f in (Path.absolute, lambda p: p.absolute().relative_to(Path(".").absolute()))))


def find7z(winNotFoundPath="C:\\Program Files\\7-Zip\\7z.exe"):
	"""Finds 7z executable"""
	if platform.system() == "Windows":
		try:
			import winreg

			with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\7-Zip", 0, winreg.KEY_READ) as reg:
				sevenZipPath = winreg.QueryValueEx(reg, "Path")[0] + "\\7z.exe"
		except BaseException:
			sevenZipPath = winNotFoundPath
	else:
		sevenZipPath = "7z"
	return sevenZipPath


def getInterpreterCommand():
	return Path(sys.executable).stem


def nearestPowerOf2(num: int):
	return 1 << (num - 1).bit_length()


def flattenDictGen(d):
	for k, v in d.items():
		if isinstance(v, dict):
			yield from ((k + "_" + kk if k != kk else k, vv) for kk, vv in flattenDictGen(v))
		else:
			yield (k, v)


def flattenDict(d):
	return dict(flattenDictGen(d))


def flattenIter(it: collections.Iterable):
	for v in it:
		if not isinstance(v, str) and isinstance(v, collections.Iterable):
			yield from flattenIter(v)
		else:
			yield v


def flattenIter1Lvl(it):
	for subIt in it:
		yield from subIt


def getExt(filePath: str):
	filePath = Path(filePath)
	return filePath.suffix[1:]


allowedFormats = {"json": "t", "json5": "t", "yaml": "t", "bson": "b"}


NoneType = type(None)


def makeSerializeable(obj):
	if isinstance(obj, list):
		return type(obj)((makeSerializeable(el) for el in obj))
	if isinstance(obj, dict):
		return type(obj)(((k, makeSerializeable(v)) for k, v in obj.items()))
	if not isinstance(obj, (int, str, float, NoneType)):
		return str(obj)
	else:
		return obj


def export(data, file: (Path, str, _io._IOBase) = None, format: str = None):
	if isinstance(file, str):
		file = Path(file)

	if format is None:
		if isinstance(file, Path):
			format = getExt(file)
		else:
			format = "json"

	if format not in allowedFormats:
		raise AttributeError("Valid formats are: " + ", ".join(allowedFormats.keys()) + " but you have passed " + str(format), "format")

	exporter = importlib.import_module(format)
	data = makeSerializeable(data)

	if isinstance(file, _io._IOBase):
		return exporter.dump(file, data)
	elif file is None:
		return exporter.dumps(data)
	elif isinstance(file, str):
		mode = "w" + allowedFormats[format]
		with file.open(mode) as file:
			return exporter.dump(data, file)
	else:
		raise ValueError("file argument is of wrong type")


def getDBMmapSize(fileName: Path, initialSize: int = 1024 * 1024 * 1024, maxSize: int = None, leave: (int, float) = 0.2, emptyFileSize: int = 1024):
	fileName = Path(fileName)
	fSize = fileName.stat().st_size
	vmem = virtual_memory().available

	if fSize > emptyFileSize:
		mmapSize = nearestPowerOf2(fSize)
	else:
		mmapSize = initialSize
	if maxSize:
		mmapSize = min(mmapSize, maxSize)

	if isinstance(leave, int):
		vmemToConsume = vmem - leave
	elif isinstance(leave, float):
		vmemToConsume = int(vmem * (1 - leave))

	mmapSize = min(mmapSize, vmemToConsume)
	return mmapSize

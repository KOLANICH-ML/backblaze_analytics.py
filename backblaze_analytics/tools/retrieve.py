import os
import re
import shlex
import sys
import typing
from datetime import datetime, timedelta, timezone

import lazy_object_proxy
from lazily import bs4, lazyImport
from plumbum import cli

from .CommandsGenerator import commandGen
from .DatabaseCommand import DatabaseCommand
from .NeedingOutputDirCommand import NeedingOutputDirCommand

try:
	import ujson as json
except ImportError:
	import json

database = lazyImport("..database")


def makeRequestsSession():
	import requests

	reqSess = requests.Session()
	try:
		import hyper
		from hyper.contrib import HTTP20Adapter

		ad = HTTP20Adapter()
		reqSess.mount("https://", ad)
		reqSess.mount("http://", ad)
	except BaseException:
		pass
	return reqSess


reqSess = lazy_object_proxy.Proxy(makeRequestsSession)

dsListUri = "https://www.backblaze.com/b2/hard-drive-test-data.html"

yearRx = re.compile("20[12]\\d")
typeRx = re.compile("data|docs")
quartalRx = re.compile("Q([1-4])")

quartalMonthLength = 3
yearMonthLength = 12


def decodeTimespan(year, quartal=None):
	lowerBoundMonth = 1
	lowerMonthDelta = 0
	if quartal:
		lowerMonthDelta = quartalMonthLength * (quartal - 1)
		higherMonthDelta = lowerMonthDelta + quartalMonthLength
	else:
		higherMonthDelta = 12

	yearDelta = higherMonthDelta // yearMonthLength
	higherMonthDelta %= yearMonthLength

	return (
		datetime(
			year=year, month=lowerBoundMonth + lowerMonthDelta, day=1, tzinfo=timezone.utc
		),
		datetime(
			year=year + yearDelta,
			month=lowerBoundMonth + higherMonthDelta,
			day=1,
			tzinfo=timezone.utc,
		),
	)


sizeRxText = "(\\d+(?:\\.\\d+)?)\\s+([MG])B\\s+"
comprSizeTypeRx = re.compile(sizeRxText + "((?:ZIP|zip|Zip)\\s+)?[fF]ile")
decomprSizeRx = re.compile(sizeRxText + "on\\s+[dD]isk")
countOfFilesRx = re.compile("(\d+) files")

sizeMults = {None: 1 / 1024 / 1024, "K": 1 / 1024, "M": 1, "G": 1024}


def parseSize(sizeStr: str, sizeMul: str) -> float:
	return float(sizeStr.strip()) * sizeMults[sizeMul.strip()]


class BackblazeDatasetDownload:
	"""A piece of Backblaze dataset available for download"""

	__slots__ = ("name", "title", "year", "type", "quartal", "uri", "comprSize", "decomprSize", "countOfFiles", "format", "timespan",)

	def __init__(self, downloadDataDict: typing.Mapping[str, str]):
		uri = downloadDataDict["dataURL"]

		name = os.path.basename(uri)
		qM = quartalRx.search(name)
		if qM:
			quartal = int(qM.group(1))
		else:
			quartal = None

		compressedInfo = comprSizeTypeRx.search(downloadDataDict["metaData"])
		if compressedInfo:
			comprSize, sizeMult, format = compressedInfo.groups()
			if comprSize:
				comprSize = parseSize(comprSize, sizeMult)
			else:
				raise ValueError("No compr size is known", downloadDataDict["metaData"])
			format = format.lower()
		else:
			raise ValueError("No compressedInfo is known", downloadDataDict["metaData"])

		decomprSize = decomprSizeRx.search(downloadDataDict["metaData"])
		if decomprSize:
			decomprSize = parseSize(*decomprSize.groups())
		else:
			raise ValueError("No decompr size is known", downloadDataDict["metaData"])

		countOfFiles = countOfFilesRx.search(downloadDataDict["metaData"])
		if countOfFiles:
			countOfFiles = int(countOfFiles.group(1))
		else:
			raise ValueError("No #files is known", downloadDataDict["metaData"])

		format = format.strip()

		self.name = name
		self.title = downloadDataDict["title"]
		self.year = int(downloadDataDict["year"])
		self.type = typeRx.search(name).group(0)
		self.quartal = quartal
		self.uri = uri
		self.countOfFiles = countOfFiles
		self.comprSize = comprSize
		self.decomprSize = decomprSize
		self.format = format
		self.timespan = decodeTimespan(self.year, self.quartal)

	def __repr__(self):
		return str(self.__dict__)


kvPairRx = re.compile("(?:var|let)\\s+(\\w+)\\s*=\\s*new\\s+(Article|Data)Table\\s*\\(([^\\)]+)\\)\\s*;")
commentRx = re.compile("/\\*\\w+\\*/")
jsonFixExpr = re.compile("'([^']+)'")


def parseScriptTag(scriptTagContents, res):
	for k, kind, v in kvPairRx.findall(scriptTagContents):
		v = commentRx.subn("", v)[0]
		v = jsonFixExpr.subn('"\\1"', v)[0]
		v = "[" + v.strip() + "]"
		fs, iF, articles = json.loads(v)
		res[kind.lower()][k] = articles


def downloadIter():
	"""Downloads and parser list of datasets"""
	resp = reqSess.get(dsListUri)
	resp.raise_for_status()
	doc = bs4.BeautifulSoup(resp.text, "html5lib")
	res = {"data": {}, "article": {}}
	for sEl in doc.select("script"):
		parseScriptTag(sEl.text, res)

	for el in res["data"]["rawHardDriveTestData"]:
		yield BackblazeDatasetDownload(el)


def datasetsListIntoTree(dList):
	"""Transforms list of datasets into a neested dict where keys are year and quartal"""
	d = {}
	for rec in dList:
		if rec.quartal:
			if rec.year not in d:
				d[rec.year] = [None] * 4
			d[rec.year][rec.quartal - 1] = rec
		else:
			d[rec.year] = rec
	return d


class DatasetRetriever(DatabaseCommand):
	__doc__ = (
		"""Creates a script to download the dataset from BackBlaze website using aria2c.
	Links to datasets are extracted from """
		+ dsListUri
		+ " ."
	)  # without __doc__ dynamic docstring won't work

	streamsCount = cli.SwitchAttr("--streamsCount", int, default=32, help="Max count of streams")
	incremental = cli.Flag("--incremental", default=None, help="Check db, download only the ones not in DB")
	destFolder = cli.SwitchAttr("--destFolder", cli.switches.MakeDirectory, default="./dataset", help="A dir to save dataset. Must be large enough.")

	def main(self):
		downloads = downloadIter()
		if self.incremental:
			with database.DBAnalyser() as db:
				lastDate = db.findLastDateTimeInAnalytics()
			print("the last date in the DB is " + str(lastDate), file=sys.stderr)
			downloads = [d for d in downloads if d.timespan[0] > lastDate]
			if not downloads:
				print("Good job, nothing to download, everything is in the base!", file=sys.stderr)
				return 0

			minDDate = min(downloads, key=lambda d: d.timespan[0]).timespan[0]
			maxDDate = max(downloads, key=lambda d: d.timespan[1]).timespan[1]
			print("The files to be downloaded will cover [" + str(minDDate) + ", " + str(maxDDate) + "] interval (" + str(maxDDate - minDDate) + ")", file=sys.stderr)

		print(__class__.genDownloadCommand((d.uri for d in downloads), self.destFolder, self.streamsCount))

	def genDownloadCommand(uris, destFolder, streamsCount=32, type="aria2"):
		streamsCount = str(streamsCount)
		if type == "aria2":
			return " ".join((commandGen.multilineEcho(uris), "|", "aria2c", "--continue=true", "--enable-mmap=true", "--optimize-concurrent-downloads=true", "-j", streamsCount, "-x 16", "-d", destFolder, "--input-file=-"))
		else:
			args = ["curl", "-C", "-", "--location", "--remote-name", "--remote-name-all", "--xattr"]
			args.extend(uris)
			return " ".join(args)


if __name__ == "__main__":
	DatasetRetriever.run()

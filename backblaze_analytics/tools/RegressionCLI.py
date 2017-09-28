import asyncio
import math
import re
import sys
import typing
import warnings
from collections import OrderedDict
from datetime import timedelta
from os import isatty
from pathlib import Path

import pandas._libs.json as json  # automatically handles nans, infs and other shit
from plumbum import cli

from .. import database
from ..analysis import Analysis
from ..utils import fancyTimeDelta
from ..utils.reorderPandasDataframeColumns import reorderPandasDataframeColumns
from .DatabaseCommand import DatabaseCommand
from .RegressionAnalysis import CoxAnalysis, WeibullAnalysis


class class2dictMeta(type):
	def __new__(cls, className: str, parents, attrs: typing.Dict[str, typing.Any], *args, **kwargs):
		newAttrs = type(attrs)(attrs)
		return {k: v for k, v in newAttrs.items() if k[0] != "_"}


class modelsTypes(metaclass=class2dictMeta):
	Weibull = WeibullAnalysis
	Cox = CoxAnalysis


class RegressionCLI(cli.Application):
	pass


class AnalysisCLI(cli.Application):
	"""Tools for regression - the main purpose of this project"""

	modelType = cli.SwitchAttr("--model", cli.Set(*modelsTypes, case_sensitive=True), default="cox", help="(Default) model to use")
	modelsPrefix = cli.SwitchAttr("--prefix", str, default="./Survival_XGBoost_Models", help="The directory where fitted XGBoost models and their metadata would reside.")


class FittingCLI(AnalysisCLI):
	reduced = cli.Flag(("r", "reduced"), help="do not use the info from S.M.A.R.T.. The numbers of days may be LESS ACCURATE since the time before appearing in the dataset is not counted (but it's available in S.M.A.R.T.).", default=True)
	dbPath = cli.SwitchAttr("--db-path", cli.ExistingFile, default=None, help="Path to the SQLite database")
	#dsFrac = cli.SwitchAttr("--ds-frac", float, default=1.0, help="Fraction of the dataset used to train the XGBoost model. XGBoost may eat all the memory in the system and crash, if all the dataset is used, if this happens, reduce the fraction.")
	dsFrac = cli.SwitchAttr("--ds-frac", float, default=0.2, help="Fraction of the dataset used to train the XGBoost model. XGBoost may eat all the memory in the system and crash, if all the dataset is used, if this happens, reduce the fraction.")
	modelFormat = cli.SwitchAttr("--modelFormat", str, default="binary", help="which format of XGBoost models to use")  # currently pyxgboost doesn't support survival:cox


optimizeHyperparamsCommandName = "optimize-hyperparams"

try:
	import UniOpt

	@RegressionCLI.subcommand(optimizeHyperparamsCommandName)
	class OptimizeHyperparamsCLI(FittingCLI):
		"""Fits hyperparams for XGBoost Cox regression using UniOpt"""

		iters = cli.SwitchAttr("--iters", int, default=10000, help="Count of iters")

		optimizer = cli.SwitchAttr("--optimizer", cli.Set(*UniOpt.__all__, case_sensitive=True), default="MSRSM", help="select a UniOpt-supported hyperparams optimizer.")

		def main(self, *attrs):
			if self.dbPath is None:
				self.dbPath = "./drives.sqlite" if self.reduced else database.databaseDefaultFileName

			a = modelsTypes[self.modelType](self.dbPath, self.dsFrac, prefix=self.modelsPrefix)
			a.optimizeHyperparams(optimizer=getattr(UniOpt, self.optimizer) if self.optimizer else None, iters=self.iters)  # `NoSuspend`ed  internally


except ImportError:
	warnings.warn(optimizeHyperparamsCommandName + " is not available, install UniOpt @ git+https://gitlab.com/KOLANICH/UniOpt.py.git ")


@RegressionCLI.subcommand("evaluate")
class EvaluateCLI(FittingCLI):
	"""Does crossvalidation estimating Harrel's concordance on the current hyperparams."""

	folds = cli.SwitchAttr("--folds", int, default=4, help="Count of folds in concordance crossvlidation")

	def main(self, *attrs):
		import pandas

		pandas.set_option("display.max_columns", None)
		if self.dbPath is None:
			self.dbPath = "./drives.sqlite" if self.reduced else database.databaseDefaultFileName

		a = modelsTypes[self.modelType](self.dbPath, self.dsFrac, prefix=self.modelsPrefix)
		print(a.evaluateModel(folds=self.folds))


@RegressionCLI.subcommand("fit")
class TrainModelCLI(FittingCLI):
	"""Fits and saves XGBoost Cox model. Need to do it in order to use the model."""

	def main(self, *attrs):
		if self.dbPath is None:
			self.dbPath = "./drives.sqlite" if self.reduced else database.databaseDefaultFileName
		a = modelsTypes[self.modelType](self.dbPath, self.dsFrac)
		a.trainModel(format=self.modelFormat)


jsonProbeRx = re.compile('^\\s*\\[\\s*\\{\\s*"?')


def preprocessModelDescriptors(models):
	if isinstance(models, str):
		try:
			models = json.loads(models)
			if isinstance(models, dict):
				models = [models]
		except json.ValueError:

			def dumbText():
				nonlocal models
				models = str.splitlines(models)

			if jsonProbeRx.match(models):
				try:
					import json5

					models = json5.loads(models)
					warnings.warn("Incorrect JSON is used. Parsed with a DAMN SLOW json5 lib. Fix the fucking shit!")
				except ImportError as ex:
					raise Exception("Incorrect string. Begins like JSON, but it is not. Unable to test if it is JSON5 - the lib is not present.") from ex

			else:
				dumbText()

	ms = []
	for m in models:
		if isinstance(m, str):
			try:
				m = json.loads(m)
			except json.ValueError:
				m = {"name": m}

		ms.append(m)
	return ms


wsRx = re.compile("\\s")


def splitByWhitespaces(file):
	for l in file:
		yield from wsRx.split(l)


def pred2obj(res, explainations, meanExpls, err=None):
	resp = {}
	if res is not None:
		resp["res"] = res.to_dict(orient="records")
	if explainations is not None:
		resp["explainations"] = explainations
	if meanExpls is not None:
		resp["meanExplainations"] = meanExpls

	if err is not None:
		resp["error"] = str(err)
	return resp


class info2formatsMeta(class2dictMeta):
	def __new__(cls, className: str, parents, attrs: typing.Dict[str, typing.Any], *args, **kwargs):
		newAttrs = type(attrs)(attrs)
		try:
			from functools import partial

			from pytablewriter import JavaScriptTableWriter, MarkdownTableWriter, MediaWikiTableWriter, PythonCodeTableWriter, RstGridTableWriter, TomlTableWriter

			pytablewriterFormats = {
				"markdown": (MarkdownTableWriter, "text/markdown", "md"),
				"rst": (RstGridTableWriter, "text/x-rst", "rst"),
				"mediawiki": (MediaWikiTableWriter, "text/plain", "mediawiki"),
				"js": (JavaScriptTableWriter, "application/javascript", "js"),
				"py": (PythonCodeTableWriter, "application/x-python-code", "py"),
				"toml": (TomlTableWriter, "application/x-toml", "toml")
			}

			def pytablewriterFormatWriter(writerType, mime, ext, res, explainations, err=None):
				if res is not None:
					w = writerType()
					if hasattr(w, "table_name"):
						w.table_name = "Regression data"
					w.from_dataframe(res)
					return w.dumps(), mime, ext
				else:
					return str(err), "text/plain", "txt"

			for k, descriptor in pytablewriterFormats.items():
				newAttrs[k] = partial(pytablewriterFormatWriter, *descriptor)
		except ImportError:
			pass

		return super().__new__(cls, className, parents, newAttrs, *args, **kwargs)


def SHAPPercent2Color(v: float) -> str:
	return "hsl(" + str(150 if v > 0 else 0) + ", " + str(abs(v) * 100) + "%, 50%)"


def SHAPPercent2style(v: float, prop="background-color"):
	return prop + ": " + SHAPPercent2Color(v) + ";"


class info2formats(metaclass=info2formatsMeta):
	def json(res, explainations, meanExpls, err=None):
		return json.dumps(pred2obj(res, explainations, meanExpls, err)), "application/json", "json"

	def json_pretty(res, explainations, meanExpls, err=None):
		# FUCK, have to use these ugly hacks because I am too lazy to write an own impl converting to serializable
		import json as nativeJSONSerializer

		res = info2formats["json"](res, explainations, meanExpls, err)
		o = nativeJSONSerializer.loads(res[0])
		return nativeJSONSerializer.dumps(o, indent="\t"), res[1], res[2]

	def html(res, explainations, meanExpls, err=None):
		if res is not None:
			boolSelector = res.dtypes == bool
			res.loc[:, boolSelector] = res.loc[:, boolSelector].replace(True, "✅")
			res.loc[:, boolSelector] = res.loc[:, boolSelector].replace(False, "❌")

			hres = res.to_html(na_rep="❔")
			try:
				import bs4

				hres = bs4.BeautifulSoup(hres, "lxml")
				head = hres.select_one("thead")
				headEls = head.select("th")
				mapping = {}
				for i, el in enumerate(headEls):
					hn = el.text.strip()
					mapping[hn] = i - 1  # the first one is `th`
					if hn in meanExpls:
						el["style"] = SHAPPercent2style(meanExpls[hn])

				rows = hres.select_one("tbody").select("tr")
				for r, es in zip(rows, explainations):
					attrsCells = r.select("td")
					for k, v in es.items():
						if k in mapping:
							attrsCells[mapping[k]]["style"] = SHAPPercent2style(v)
				hres = str(hres)
			except ImportError:
				pass

			return str(hres), "text/html", "html"
		else:
			return str(err), "text/html", "html"

	def latex(res, explainations, meanExpls, err=None):
		if res is not None:
			return res.to_latex(), "application/x-latex", "tex"
		else:
			return str(err), "text/plain", "txt"

	def tsv(res, explainations, meanExpls, err=None):
		if res is not None:
			return res.to_csv(sep="\t"), "text/tab-separated-values", "tsv"
		else:
			return str(err), "text/plain", "txt"

	def csv(res, explainations, meanExpls, err=None):
		if res is not None:
			return res.to_csv(), "text/csv", "csv"
		else:
			return str(err), "text/plain", "txt"


class PredictModelCLI(FittingCLI):
	"""Fits and saves XGBoost Cox model. Need to do it in order to use the model."""

	format = cli.SwitchAttr("--format", cli.Set(*info2formats, case_sensitive=True), default="json", help="Format to output the prediction")

	def prepare(self):
		if self.dbPath is None:
			self.dbPath = "./drives.sqlite" if self.reduced else database.databaseDefaultFileName

		import pandas

		pandas.set_option("display.max_columns", None)
		self.analysis = modelsTypes[self.modelType](self.dbPath, self.dsFrac)  # aggregation doesn't make any sense here - we don't train anything
		self.analysis.loadModel()

	def computeAndRenderResults(self, modelDescriptors, format=None, raises: bool = True):
		if format is None:
			format = self.format

		if format in info2formats:
			try:
				modelDescriptors = preprocessModelDescriptors(modelDescriptors)
				res, explainations, meanExpls = self.analysis.predictUnknownModelsSurvival(modelDescriptors)

				def fancifyTime(s):
					if s and not math.isnan(s):
						ft = fancyTimeDelta(timedelta(days=s))
						return (str(ft.years) + " y, " if ft.years else "") + (str(ft.months) + " m, " if ft.months else "") + str(ft.days) + " d"
					else:
						return s

				res.loc[:, "$fancyTime"] = res.loc[:, "predicted_survival"].map(fancifyTime)

				res = reorderPandasDataframeColumns(res, ["vendor", "name", "predicted_survival", "$fancyTime"])

				ex = None
			except BaseException as ex1:
				res, explainations, meanExpls = (None, None, None)
				ex = ex1
				if raises:
					raise
			return info2formats[format](res, explainations, meanExpls, ex)
		else:
			return "Format `" + format + "` is not available, use one of " + str(availableFormats), "text/plain", "txt"


@RegressionCLI.subcommand("predict")
class PredictModelFromCLI(PredictModelCLI):
	"""Predicts survival for the drives which descriptors (just namebers or JSON) are passed via CLI"""

	def main(self, *modelDescriptors):
		if not modelDescriptors:
			if not isatty(sys.stdin.fileno()):
				modelDescriptors = sys.stdin.read()
			else:
				self.help()
				return -1

		self.prepare()

		print(self.computeAndRenderResults(modelDescriptors)[0])


def check12700(ip: str) -> bool:
	ip = ip.split(".")
	return len(ip) == 4 and ip[0] == 127 and ip[1] == 0 and ip[2] == 0


@RegressionCLI.subcommand("server")
class CoxPredictionServer(PredictModelCLI):
	startBrowser = cli.Flag(("-B", "--start-browser"), help="Starts a web browser automatically", default=False)
	CORS = cli.Flag(("-C", "--CORS"), help="Enables CORS", default=True)
	ip = cli.SwitchAttr("--ip", str, default="127.0.0.1", help="IP address to start server on. If 127.0.0.*, restricts accesses to localhost")
	port = cli.SwitchAttr("--port", int, default=0xbb85, help="Port to start server on")

	def main(self, *args):
		self.prepare()

		from aiohttp import ClientSession, TCPConnector, web

		app = web.Application()

		routes = web.RouteTableDef()

		@routes.get("/")
		@asyncio.coroutine
		def help(request):
			return web.Response(text="Send a POST to /predict.<br/><form action='predict' method='POST'><select name='format'>" + "".join(("<option>" + fId + "</option>") for fId in info2formats._available) + "</select><textarea name='models' width='100%' height='100%'></textarea><input type='submit'></form>", content_type="text/html")

		@routes.get("/status")
		@asyncio.coroutine
		def status(request):
			f = self.analysis.f
			axg = f.axg
			return web.json_response({"columns": list(axg.columns), "bestHyperparams": axg.bestHyperparams})

		def predictForData(modelDescriptors, format):
			resContent, mimeType, ext = self.computeAndRenderResults(modelDescriptors, format, raises=False)
			resp = web.Response(text=resContent, content_type=mimeType)
			resp.headers["Content-Disposition"] = 'inline; filename="prediction.' + ext + '"'
			return resp

		@routes.get("/predict/{models}")
		@asyncio.coroutine
		def predictGet(request):
			return predictForData(request.match_info["models"], None)

		@routes.get("/predict/{models}/{format}")
		@asyncio.coroutine
		def predictGetFormat(request):
			return predictForData(request.match_info["models"], request.match_info["format"])

		@asyncio.coroutine
		def predictPost_(request, format):
			formData = yield from request.post()
			if formData:
				if formData["models"]:
					data = formData["models"]
				else:
					data = yield from request.text()

				if formData["format"]:
					format = formData["format"]
			else:
				data = yield from request.text()

			return predictForData(data, format)

		@routes.post("/predict")
		@asyncio.coroutine
		def predictPost(request):
			return (yield from predictPost_(request, None))

		@routes.post("/predict/{format}")
		@asyncio.coroutine
		def predictPostFormat(request):
			return (yield from predictPost_(request, request.match_info["models"]))

		if self.CORS:
			import aiohttp_cors

			cors = aiohttp_cors.setup(app)
			predictPostResource = cors.add(app.router.add_resource("/predict"))
			route = cors.add(
				predictPostResource.add_route("POST", predictPost),
				{
					"*": aiohttp_cors.ResourceOptions(allow_headers="*")
				}
			)
		else:
			app.add_routes([web.post("/", predictPost)])

		@web.middleware
		@asyncio.coroutine
		def logger(request, handler):
			peerName = request.transport.get_extra_info("peername")
			print("Connection from ", *peerName, "to", request.path)
			return (yield from handler(request))

		app.middlewares.append(logger)

		if check12700(self.ip):

			@web.middleware
			@asyncio.coroutine
			def accessRestricter(request, handler):
				peerName = request.transport.get_extra_info("peername")
				if not check12700(peerName[0]):
					raise web.HTTPForbidden("Fuck off!")

				return (yield from handler(request))

			app.middlewares.append(accessRestricter)

		@asyncio.coroutine
		def initApp(app):
			serverURI = "http://localhost:" + str(self.port)
			if self.startBrowser:
				import webbrowser

				try:
					webbrowser.open(serverURI)
				except BaseException:
					print("Cannot start browser")
			# Don't use aiohttp_remotes, it trusts a remote user blindly

		#resp.headers["Server"] = "BackBlaze_Analytics_Cox_Regression"
		app.on_startup.append(initApp)
		app.add_routes(routes)
		web.run_app(app, host=self.ip, port=self.port)


if __name__ == "__main__":
	AnalysisCLI.run()

from pathlib import Path
from types import FunctionType

from lazily import lazyImport, lifelines, more_itertools, pandas
from lazily import scipy as np
from plumbum import cli

from .. import database
from ..analysis import Analysis
from ..datasetDescription import targetAttrsSpec
from ..utils.mtqdm import mtqdm
from ..utils.PickleCache import PickleCache
from .DatabaseCommand import DatabaseCommand
from .ImageOutputCommand import ImageOutputCommand

plt = lazyImport("matplotlib.pyplot")


# from Chassis import Chassis


class CustomSplitters:
	def vendor(self):
		"""Plots a survival plot, where differrent lines correspond to vendors"""
		return self.attrOverall("vendor_id", fancyNameLambda=lambda vId: self.ds.vendors[vId]["name"], title="vendors overall")

	def model(self):
		"""Plots a survival plot, where differrent lines correspond to models"""
		return self.attrOverall("model_id", fancyNameLambda=lambda mId: self.ds.models[mId]["name"], title="models overall")

	def brand(self):
		"""Plots multiple survival plots (each one for each brand in dataset), where differrent lines correspond to models"""
		return self.splitTwice("brand_id", "model_id", lambda bId: self.ds.brands[bId]["name"], lambda mId: self.ds.models[mId]["name"], overall=True)

	def series(self):
		return self.splitTwice("brand_id", "series", lambda bId: self.ds.brands[bId]["name"], overall=True)

	def family(self):
		return self.splitTwice("brand_id", "family", lambda bId: self.ds.brands[bId]["name"])

	def capacity(self):
		return self.splitTwiceHistWithAppensionToFancyNameFromModel("capacity", overall=True)


def refineHist(hist, bin_edges):
	newEdges = [bin_edges[0]]
	newHist = []
	lastNonZero = 0
	for i in range(1, len(hist)):
		if hist[i] != 0:
			if lastNonZero is not None and bin_edges[i] == 0:
				newEdges.append((bin_edges[lastNonZero] + bin_edges[i]) / 2)
			newEdges.append(bin_edges[i])
			lastNonZero = i
	newEdges = list(more_itertools.pairwise(newEdges))
	return newEdges


class LifeLinesAnalysis(Analysis):
	"""A class to make analysis. Call its methods in a Jupyter notebook"""

	def __init__(self, dbFilePath: Path):
		super().__init__(dbFilePath)
		self.kmf = lifelines.KaplanMeierFitter()

	def plotDrivesSurvivalLine(self, cur, label: str, ax=None):
		"""Plots lifelines for a subset of drives."""
		if not cur.empty:
			self.kmf.fit(cur["duration_worked"], event_observed=cur["failed"], label=label)
			return self.kmf.plot(ax=ax, grid=True)
		else:
			return ax

	def splitTwice(self, primary: str, secondary: str, fancyNameLambdaPrimary: FunctionType = None, fancyNameLambdaSecondary: FunctionType = None, overall: bool = False):
		"""Plots multiple survival plots (each one for each primary attr value in a view), where differrent lines correspond to secondary attr value"""
		primaryPartsToPlot = list(self.splitTwice_(self.pds, primary, secondary))
		return self.plotSplitedTwice_(primaryPartsToPlot, primary, secondary, fancyNameLambdaPrimary, fancyNameLambdaSecondary, overall)

	def splitTwice_(self, view, primary: str, secondary: str):
		"""Plots multiple survival plots (each one for each primary attr value in a view), where differrent lines correspond to secondary attr value"""
		if primary not in self.pds.columns:
			self.insertModelAttrIntoPandasDataset(self.pds, primary)
		if primary not in self.domains:
			self.computeDomains(primary)

		with mtqdm(self.domains[primary], desc="Splitting by " + primary + ":" + secondary + "s...") as pb:
			for i, bId in enumerate(pb):
				#pb.write(str(bId))
				#pb.write(str(primary))
				#pb.write(repr(view.loc[:, primary]))
				currentPDSView = view.loc[view.loc[:, primary] == bId]
				currentPDSView = currentPDSView.loc[currentPDSView.loc[:, secondary].notna()]
				if not currentPDSView.empty:
					yield (bId, currentPDSView)

	def splitTwiceHist_(self, view, primary: str, secondary: str):
		# if you see all nulls, populate models with scraped data and rebuild dataframe
		columns = list(targetAttrsSpec.keys())
		columns.extend([primary, secondary])
		view = view.loc[view.loc[:, primary].notna(), columns]
		newEdges = refineHist(*np.histogram(view.loc[:, primary], bins="doane"))

		for i, (lb, ub) in enumerate(mtqdm(newEdges, desc="Binning by " + primary + "...")):
			currentSelector = view.loc[:, primary].between(lb, ub)
			currentPDSView = view[currentSelector]
			#print(currentPDSView)
			yield ((lb, ub), currentPDSView)

	def plotSplitedTwice_(self, primaryPartsToPlot: list, primary: str, secondary: str, fancyNameLambdaPrimary: FunctionType = None, fancyNameLambdaSecondary: FunctionType = None, overall: bool = False):
		"""Plots multiple survival plots (each one for each primary attr value in a view), where differrent lines correspond to secondary attr value"""

		countOfSubplots = len(primaryPartsToPlot) + int(overall)
		fig, sp = plt.subplots(countOfSubplots, 1, sharex=True, sharey=True, figsize=(plt.rcParams["figure.figsize"][0], plt.rcParams["figure.figsize"][1] * countOfSubplots))
		if overall:
			overallAx = sp[0]
			sp = sp[1:]

		with mtqdm(primaryPartsToPlot, desc="Plotting " + primary + ":" + secondary + "s...") as pb:
			for i, (bId, currentPDSView) in enumerate(pb):
				self.attrOverall(
					secondary,
					view=currentPDSView,
					domain=set(currentPDSView[secondary]),
					ax=sp[i],
					fancyNameLambda=fancyNameLambdaSecondary, title=fancyNameLambdaPrimary(bId)
				)
				if overall:
					self.plotDrivesSurvivalLine(currentPDSView, fancyNameLambdaPrimary(bId), overallAx)
		if overall:
			overallAx.set_title("overall")
		return (fig, "by_" + primary + "_and_" + secondary)

	def splitTwiceHist(self, primary: str, secondary: str, fancyNameLambdaPrimary: FunctionType = None, fancyNameLambdaSecondary: FunctionType = None, overall: bool = False):
		"""Plots  a series of plots where ech plot is a bin of primary attr"""
		if fancyNameLambdaPrimary is None:

			def fancyNameLambdaPrimary(r):
				return "[" + str(r[0]) + ", " + str(r[1]) + ")"

		primaryPartsToPlot = list(self.splitTwiceHist_(self.pds, primary, secondary))
		return self.plotSplitedTwice_(primaryPartsToPlot, primary, secondary, fancyNameLambdaPrimary, fancyNameLambdaSecondary, overall)

	def splitTwiceHistWithAppensionToFancyNameFromModel(self, primary: str, fancyNameLambdaPrimary: FunctionType = None, fancyNameLambdaSecondary: FunctionType = None, overall: bool = False):
		if fancyNameLambdaSecondary is None:

			def fancyNameLambdaSecondary(mId):
				return self.ds.models[mId]["name"]

		def decoratedFancyNameLambdaSecondary(mId):
			return fancyNameLambdaSecondary(mId) + " : " + str(self.ds.models[mId][primary])

		return self.splitTwiceHist(primary, "model_id", fancyNameLambdaPrimary, decoratedFancyNameLambdaSecondary, overall)

	def attrOverall(self, attrName: str, view=None, domain=None, ax=None, fancyNameLambda=None, title=None):
		"""Plots a survival plot, where differrent lines correspond to diferrent values of a model attribute with passed name"""
		if view is None:
			view = self.pds

		if attrName not in self.pds.columns:
			if view is self.pds:
				self.insertModelAttrIntoPandasDataset(self.pds, attrName)
			else:
				raise Exception("We are not inserting into a view, insert in the original DataFrame insterad")

		if domain is None:
			if view is self.pds:
				if attrName not in self.domains:
					self.computeDomains(attrName)
				domain = self.domains[attrName]
			else:
				raise Exception("Domains are computed for the original DataFrame!")

		if ax is None:
			fig, ax = plt.subplots(1, 1)
		else:
			fig = None

		if title is None:
			title = "overall " + attrName

		if fancyNameLambda is None:
			fancyNameLambda = str

		for a in mtqdm(domain, desc=("Splitting by " + attrName + "... ")):
			ax = self.plotDrivesSurvivalLine(view.loc[view.loc[:, attrName] == a], fancyNameLambda(a), ax)
		ax.set_title(title)
		return (fig, title)

	def represent(self, fig, name: str, saveDir: Path = False, imageExt="svg"):
		if not saveDir:
			fig.show()
		else:
			saveDir = Path(saveDir)
			fig.savefig(str(saveDir / (name + "." + imageExt)))

	def __call__(self, attrs, saveDir=None, imageExt="svg"):
		"""Do analysis."""
		additionalAttrs = set(attrs) - {"brand", "vendor", "model"}

		# self-check, defensive programming to find error
		#brokenRecords = self.pds.loc[self.pds.loc[:, "failed"].isna()]
		#print(brokenRecords)
		#assert len(brokenRecords) == 0

		plt.rcParams["svg.fonttype"] = "none"

		for atr in attrs:
			if hasattr(CustomSplitters, atr):
				imgs = getattr(CustomSplitters, atr)(self)
			else:
				imgs = self.attrOverall(atr)
			self.represent(*imgs, saveDir=saveDir, imageExt=imageExt)


class AnalysisCLI(ImageOutputCommand):
	"""Plots lifelines in Kaplan-Meier model"""

	reduced = cli.Flag(("r", "reduced"), help="do not use the info from S.M.A.R.T.. The numbers of days may be LESS ACCURATE since the time before appearring in the dataset is not counted (but it's available in S.M.A.R.T.).", default=True)
	showAvailableAttrs = cli.Flag(("S", "show-available-attrs"), help="Loads and augments dataset to show available attrs")
	dbPath = cli.SwitchAttr("--db-path", cli.ExistingFile, default=None, help="Path to the SQLite database")

	def main(self, *attrs):
		if self.dbPath is None:
			self.dbPath = "./drives.sqlite" if self.reduced else database.databaseDefaultFileName

		attrs = set(attrs)
		if not attrs:
			attrs |= {"rpm", "interface_speed", "form_factor_height", "capacity", "top_capacity", "first_known_date"}
			attrs |= {"brand", "vendor"}

		a = LifeLinesAnalysis(self.dbPath)
		if self.showAvailableAttrs:
			print("The following attrs are available: ")
			print(" ".join(a.ds.getAvailableAttrs()))
			return 0

		a(attrs=attrs, saveDir=self.destFolder, imageExt=self.imageExt)


if __name__ == "__main__":
	AnalysisCLI.run()

import importlib
import itertools
from collections import OrderedDict, defaultdict

from NoSuspend import *
from plumbum import cli

from .. import database
from ..datasetDescription import *
from ..utils import *
from ..utils.mtqdm import mtqdm
from .DatabaseCommand import DatabaseCommand


def detectMultipleFailures(failures):
	acc = defaultdict(list)
	for s in mtqdm(failures):
		if s["failure_date"]:
			acc[s["id"]].append(s["failure_date"])
	return {k: v for k, v in acc.items() if len(v) > 1}


def detectAnomalies(db, failures):
	print("detecting drives with multiple failures")
	multipleFailures = detectMultipleFailures(failures)
	print("detected " + str(len(multipleFailures)) + " with multiple failures")

	print("detecting drives used after a failure")
	postfailureUsed = {d["id"]: d for d in db.getPostfailureUsedDrives()}
	print("detected " + str(len(postfailureUsed)) + " drives used after a failure")

	anomalies = dict(postfailureUsed)
	for i, d in multipleFailures.items():
		if i in anomalies:
			anomalies[i]["failure_date"] = d
		else:
			anomalies[i] = {"failure_date": d}
	for dr in db.getDrivesWithUnknownModel():
		anomalies[dr["id"]] = dr
		anomalies[dr["id"]]["unknown"] = True
	return anomalies


class Preprocesser(DatabaseCommand):
	"""find first and last occurences and failures for every drive in dataset"""

	# when default=true it is true by default and using it inverts it
	nonevaluated = cli.Flag("no-nonevaluated", help="finds and computes stats for the drives not having them", default=True)
	outdated = cli.Flag("no-outdated", help="updates stats for the drives having them", default=True)
	failed = cli.Flag("no-failed", help="finds which drives have failed", default=True)
	anomalies = cli.Flag(
		"no-anomalies",
		help=r"""finds anomalyous drives:
		* the ones not removed after failure
		* the ones having multiple failures""",
		requires=["no-failed"],  # remember, in fact it is "failed", plumbum is shit and I have to do perversions, and it is definitely a bug
		default=True,
	)

	def main(self):
		with NoSuspend():
			with database.DBAnalyser(self.dbPath) as db:
				if self.failed:
					print("searching for failure records (both new and old ones)....")
					failures = db.findFailureRecords()  # TODO: do it smart
					print(len(failures), "records failed")
					failures.sort(key=lambda x: x["id"])
				elif self.anomalies:
					print("searching for failure records of known failed drives....")
					#failures = db.getKnownFailedDrivesFailureRecords()  # damn it, no better than the full scan
					failures = list()
					for r in db.getKnownFailedDrivesDates():
						if r["failure"]:
							del r["failure"]
							failures.append(r)
					print(len(failures), "records failed")
					failures.sort(key=lambda x: x["id"])

				if self.nonevaluated:
					print("searching for nonevaluated drives....")
					nonevaluated = db.findNonevaluatedDrives()
					print(len(nonevaluated), "drives without stats")

				if self.outdated:
					print("searching for drives with outdated stats....")
					outdated = db.findOutdatedCandidatesStatsRecords()  # TODO: do it smart
					print(len(outdated), "drives with possibly outdated stats")

				stats = []
				if self.outdated:
					stats.extend(db.recomputeStatsForDrives(mtqdm(outdated, desc="recomputing outdated")))
				if self.failed:
					stats.extend(db.computeStatsForDrives(mtqdm(failures, desc="computing failed")))
				if self.nonevaluated:
					stats.extend(db.computeStatsForDrives(mtqdm(nonevaluated, desc="computing nonevaluated")))

				db.saveStatsForDrives(mtqdm(stats, desc="saving stats"))

				if self.anomalies:
					anomalies = detectAnomalies(db, failures)
					print(len(anomalies), "anomalious drives")

					db.saveAnomalies(mtqdm(anomalies.items(), desc="saving anomalies"))


if __name__ == "__main__":
	Preprocesser.run()

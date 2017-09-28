from pathlib import Path

from plumbum import cli

from ..database import DB
from ..dataset import Dataset
from ..utils import getExt
from .CommandsGenerator import *
from .DatabaseCommand import DatabaseCommand
from .NeedingOutputDirCommand import NeedingOutputDirCommand
from .SevenZipCommand import SevenZipCommand


class DatasetExporter(cli.Application):
	"""Tools to export data"""

	pass


@DatasetExporter.subcommand("dataset")
class DatasetArchiver(DatabaseCommand, SevenZipCommand, NeedingOutputDirCommand):
	"""Creates a script to archive the processed dataset"""

	def main(self):
		dbfn = pathRes(self.dbPath)
		#print(commandGen.wrapNoSuspend(commandGen.sqliteWrap(dbfn, "vacuum;")))
		print(commandGen.wrapNoSuspend(commandGen.pack7z(self.sevenZipPath, dbfn, str(dbfn) + ".xz")))


@DatasetExporter.subcommand("toy")
class ToyExporter(DatabaseCommand):
	"""Exports subset of dataset to test this scripts on fast"""

	def main(self, outputFilePath: (Path, str) = None):
		if outputFilePath is not None:
			outputFilePath = Path(outputFilePath)
		with DB(self.dbPath) as db:
			db.exportToyDB(outputFilePath)


@DatasetExporter.subcommand("drives")
class DrivesExporter(DatabaseCommand):
	"""Exports info about vendors, brands and models into a separate file. Useful when wanna use preprocessed dataset only."""

	augment = cli.Flag(("A", "augment"), help="Augment the data before exporting")

	def main(self, outputFilePath: Path = "./drives.sqlite", *what):
		format = None
		if outputFilePath == "-":
			outputFilePath = None
		else:
			outputFilePath = Path(outputFilePath)
			format = getExt(outputFilePath)

		if format == "sqlite":
			if self.augment:
				print("For now we cannot export augmented data into a DB")
			with DB(self.dbPath) as db:
				db.exportSomeTables(outputFilePath, what)
		else:
			if not what:
				what = ("vendors", "brands", "models")
			ds = Dataset(self.dbPath)
			if self.augment:
				ds.augment()
			ds.export(outputFilePath, what=what)


if __name__ == "__main__":
	DatasetExporter.run()

import csv
import zipfile
from glob import glob
from pathlib import Path, PurePath

from NoSuspend import *
from plumbum import cli

from .. import database
from ..database import *
from ..datasetDescription import *
from ..SMARTAttrsNames import SMARTAttrsNames
from ..utils import pathRes
from ..utils.mtqdm import mtqdm
from .CommandsGenerator import *
from .DatabaseCommand import DatabaseCommand
from .nativeImporterCodeGen import CPPSchemaGen
from .SevenZipCommand import SevenZipCommand


def genImportDatasetScript(sevenZipPath: Path, dbPath: Path, tempDir: Path, archiveName: Path, fileNames):
	"""Generates a script unpacking and importing a CSV file from archived dataset"""
	tempDir = Path(tempDir)
	cmds = []
	for fileName in fileNames:
		unpackedName = pathRes(tempDir / fileName.name)
		cmds.append(commandGen.unpack7z(sevenZipPath, tempDir, archiveName, fileName))
		cmds.append(commandGen.sqliteWrap(pathRes(dbPath), '.import "' + str(unpackedName).replace("\\", "/") + '" ' + database.tablesNames["csvImportTemp"].replace("`", "")))
		cmds.append(commandGen.delete(unpackedName))
	return "\n".join(cmds)


def doesFileNameLookSuitable(fileName):
	fileName = Path(fileName)
	return fileName.suffix == ".csv" and fileName.parts[0].find("MACOSX") == -1


def genImportDatasetsScript(sevenZipPath: Path, dbPath: Path, archivesDir: Path = "./dataset/", tempDir: Path = None, isRamDisk=False):
	"""Generates a script importing all the archived datasets from the folder"""
	if not tempDir:
		tempDir = archivesDir  # this is the temp dir for unpacking csv files, they are not very large.

	yield from commandGen.setEnvs(database.getTempDirEnvDict())  # this is a temp directory for DB journal, it may be very large

	yield commandGen.backblazeAnalytics(("import", Importer._subcommand_CreateTables.name), dbPath)
	archivesDir = Path(archivesDir)
	fileNames = archivesDir.glob("*.zip")
	for archName in fileNames:
		# unpack every file from dataset
		if isRamDisk:
			commandGen.copy(archName, tempDir)
			archName = tempDir

		z = zipfile.ZipFile(archName)
		files = sorted(PurePath(f.filename) for f in z.filelist if doesFileNameLookSuitable(f.filename))

		yield genImportDatasetScript(sevenZipPath, dbPath, tempDir, archName, files)

		if isRamDisk:
			yield commandGen.delete(archName)

	yield commandGen.sqliteWrap(pathRes(dbPath), "delete from " + database.tablesNames["csvImportTemp"] + " where `model` = 'model';", wrap=commandGen.wrapNoSuspend)
	yield commandGen.backblazeAnalytics(("import", Importer._subcommand_ModelsNormalizer.name), dbPath)
	yield commandGen.backblazeAnalytics(("import", Importer._subcommand_RecordsNormalizer.name), dbPath)
	yield commandGen.wrapNoSuspend(commandGen.sqliteFastVacuum(dbPath))


def genImportScript(sevenZipPath, dbPath, archivesDir="./dataset/", tempDir="./dataset/", isRamDisk=False):
	return "\n".join(genImportDatasetsScript(sevenZipPath, dbPath, archivesDir, tempDir, isRamDisk))


class Importer(cli.Application):
	"""Contains the tools to import the dataset and prepare it for analysis"""

	pass


@Importer.subcommand("genScript")
class GenScript(DatabaseCommand, SevenZipCommand):
	"""Generates a shell script to import BackBlaze data to a DB"""

	tempDir = cli.SwitchAttr("--tempDir", cli.ExistingDirectory, default="./dataset/", help="A dir to unpack csv files. Must be large enough.")
	isRamDisk = cli.Flag("--RAMDisk", default=False, help="tempDir is a RAM Disk enough to fit both zip and a csv extracted from it")
	#useFuse = cli.Flag("--fuse", default=False, help="use a fuse FS to access CSV files in archive instead of extracting")

	archivesDir = cli.SwitchAttr("--archivesDir", cli.ExistingDirectory, default="./dataset/", help="The dir where archives with csv files are situated.")

	def main(self):
		print(genImportScript(self.sevenZipPath, self.dbPath, self.archivesDir, self.tempDir, self.isRamDisk))


@Importer.subcommand("createTables")
class CreateTables(DatabaseCommand):
	"""Creates the necessary tables"""

	def main(self):
		with DBNormalizer(self.dbPath) as db:
			db.createTables()


@Importer.subcommand("normalizeModels")
class ModelsNormalizer(DatabaseCommand):
	"""Does normalization of database structure to get the lower size, better speeds and convenient edits: moves everything specific to a model into a separate table"""

	def main(self):
		with NoSuspend():
			with DBNormalizer(self.dbPath) as db:
				db.normalizeModels()


@Importer.subcommand("normalizeRecords")
class RecordsNormalizer(DatabaseCommand):
	"""Does normalization of database structure: transforms the rows imported from CSV. Usually takes long."""

	batchSize = cli.SwitchAttr("--batch-size", int, default=100, help="The size of batches. Lesser the size - less free disk space needed for journal, less the work wasted on interrupt or failure and faster the recovery from interrupt (less journal must be processed in order not to break the DB). More the size - less the speed overhead (see `throughput_from_batch_size_dependence.ipynb`), but on interrupt more the work wasted and longer the recovery.")

	def main(self):
		with NoSuspend():
			with DBNormalizer(self.dbPath) as db:
				(size, iter) = db.normalizeRecords(batchSize=self.batchSize)
				pr = 0
				with mtqdm(total=size, desc="Normalizing records") as bar:
					for progress in iter:
						bar.update(progress[0] - pr)
						pr = progress[0]
						bar.write(progress[1])


@Importer.subcommand("upgradeSchema")
class SchemaUpgrader(DatabaseCommand):
	"""Migrates data to a new schema"""

	def main(self):
		with NoSuspend():
			with DBNormalizer(self.dbPath) as db:
				db.upgradeSchema()


Importer.subcommand("generateC++Schema")(CPPSchemaGen)

if __name__ == "__main__":
	Importer.run()

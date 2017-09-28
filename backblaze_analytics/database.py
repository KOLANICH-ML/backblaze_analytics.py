__all__ = ("DB", "DBNormalizer", "DBAnalyser", "databaseDefaultFileName")
import itertools
import json
import os
import platform
import re
import sqlite3
import sys
from collections import OrderedDict
from functools import lru_cache
from pathlib import Path

from .datasetDescription import *
from .rowidHacks import *
from .utils import flattenIter1Lvl, getDBMmapSize, pathRes

# DO NOT TOUCH WITHOUT MODIFIING *.SQL FILES
tablesNames = {
	"vendors": "vendors",
	"brands": "brands",
	"models": "models",
	"drives": "drives",
	"smart": "drive_stats"
}
tablesNames["csvImportTemp"] = tablesNames["smart"] + "_1"
tablesNames = {k: ("`" + v + "`") for k, v in tablesNames.items()}

analysisDBName = "analytics"
analysisDBTablesNames = {
	"drivesAnalytics": "drives_analytics",
	"anomalies": "anomalies",
	"censoredDrives": "censored_drives"
}
analysisDBTablesNames = {k: (analysisDBName + ".`" + v + "`") for k, v in analysisDBTablesNames.items()}

tablesNames.update(analysisDBTablesNames)


class TableName:
	__slots__ = ("dbID", "name")

	def __init__(self, name, dbID=None):
		if not dbID:
			dbID = "main"
		self.name = name
		self.dbID = dbID

	def __str__(self):
		return "".join((self.dbID, ".", "`", self.name, "`"))

	def __repr__(self):
		return self.__class__.__name__ + "(" + ", ".join((repr(self.name), repr(self.dbID))) + ")"

	def __eq__(self, other):
		if isinstance(other, str):
			return self.__class__.fromStr(other) == self
		elif isinstance(other, __class__):
			return self.name == other.name and self.dbID == other.dbID
		else:
			return False

	def __hash__(self):
		return hash(str(self))

	def fromStr(tableName: str):
		parts = tableName.split(".")
		tableName = parts[-1].replace("`", "").replace("'", "").replace('"', "")
		dbId = ".".join(parts[:-1])
		return __class__(tableName, dbId)


databaseDefaultFileName = pathRes("./db.sqlite")
analysisDatabaseDefaultFileName = pathRes("./analytics.sqlite")
toyDatabaseDefaultFileName = pathRes("./toy.sqlite")

thisDir = Path(__file__).parent
sqlFilesDir = thisDir / "sql"


def dateToOrdinal(date="`date`"):
	return "cast(strftime('%s', " + date + ")/(3600*24) as int)"


def dateToISO(date="`date`"):
	return "strftime('%Y-%m-%d', " + date + "*(3600*24), 'unixepoch')"


def createQueryWrapper(query):
	def wrapper(self):
		cur = self.db.cursor()
		cur.row_factory = lambda *r: dict(sqlite3.Row(*r))
		#print(query)
		cur.execute(query)
		res = list(cur)
		cur.close()
		return res

	wrapper.__doc__ = "Returns the result of " + query
	return wrapper


def dumbSelect(name):
	return createQueryWrapper("select * from " + name + ";")


class TableSpecGen:
	def __init__(self, tableName):
		self.tableName = tableName

	def genSpecs(self):
		raise NotImplementedError()

	def genModifiers(self):
		return None

	def __call__(self, tableVarName=None):
		lines = __class__.genTableColumnsSpecsLines(self.genSpecs(), tableVarName)
		modifiers = self.genModifiers()
		return "".join(("CREATE TABLE ", str(self.tableName), "(", ",\n".join(lines) + ((",\n" + modifiers) if modifiers else "") + "\n);"))

	def genTableColumnsSpecsLines(columns, srcTableVarName=None, names=None, dstTableVarName=None):
		if isinstance(columns, dict) and names is None:
			names = columns.values()

		for i, line in enumerate(columns):
			yield "\t\t" + ", ".join(((srcTableVarName + "." if srcTableVarName else "") + "`" + name + "` " + colType + (" as " + (dstTableVarName + "." if dstTableVarName else "") + "`" + names[i][j] + "`" if names else "") for (j, (name, colType)) in enumerate(line)))


class TableSpec(TableSpecGen):
	def __init__(self, tableName, specs, modifiers=None):
		super().__init__(tableName)
		self.specs = list(specs)
		self.modifiers = modifiers

	def genSpecs(self):
		return self.specs

	def genModifiers(self):
		return self.modifiers


class DrivesParamsTableSpec(TableSpecGen):
	def __init__(self, name, smartAttrIDs, resolver):
		self.smartAttrIDs = smartAttrIDs
		self.resolver = resolver
		super().__init__(name)

	def genTableColumnNamePairForASMARTParam(names):
		return (((name + "_normalized", "INTEGER (1)"), (name + "_raw", "INTEGER (8)")) for name in names)

	def genSpecs(self):
		yield from self.__class__.genTableColumnNamePairForASMARTParam(self.resolver(self.smartAttrIDs))


class DrivesStatsTableSpec(DrivesParamsTableSpec):
	def genSpecs(self):
		yield (("capacity_bytes", "INTEGER (8) NOT NULL"),)
		yield (("failure", "INTEGER (1) NOT NULL"),)
		yield from super().genSpecs()


class StatsTableSpec(DrivesStatsTableSpec):
	def genSpecs(self):
		yield (("packed_rowid", "INTEGER NOT NULL PRIMARY KEY"),)
		yield from super().genSpecs()

	def genModifiers(self):
		return "FOREIGN KEY('drive_id') REFERENCES drives('id')"


class TempStatsTableSpec(DrivesStatsTableSpec):
	def genSpecs(self):
		tnn = "TEXT NOT NULL"
		yield from ((("date", tnn), ("serial_number", tnn)), (("model", tnn),))
		yield from super().genSpecs()


tablesSchemas = {
	"csvImportTemp": TempStatsTableSpec(tablesNames["csvImportTemp"], smartAttrIDs, smartAttrsResolvers.basic),
	"smart": StatsTableSpec(tablesNames["smart"], hddAttrsIDs, smartAttrsResolvers.pretty)
}

fictiveSpecRepresentingTheAttrsNeededToBeMovedFromTempRecordsTableToPermanentOne = DrivesStatsTableSpec(tablesNames["csvImportTemp"], tablesSchemas["smart"].smartAttrIDs, tablesSchemas["csvImportTemp"].resolver)


class SQLiteRegexpWrapper:
	"""Adds ```regexp``` function into DB connection to allow sqlie to use regexps"""

	def __init__(self):
		self.regexDic = {}

		def sqlite_regexp(regexText, item):
			if regexText:
				if regexText in self.regexDic:
					regex = self.regexDic[regexText]
				else:
					regex = re.compile(regexText)
					self.regexDic[regexText] = regex
				return regex.search(item) is not None
			else:
				return False

		self.sqlite_regexp = sqlite_regexp

	def attach(self, db):
		db.create_function("regexp", 2, self.sqlite_regexp)


def getTempDirEnvDict(tmpDir="."):
	"""PRAGMA temp_store_directory is deprecated, recommended to be disabled and this recommendation is followed the build in Anaconda. This func generates the dict of env variables needed."""
	tmpDir = str(Path(tmpDir).absolute())
	if platform.system() == "Windows":
		return {"TMP": tmpDir, "TEMP": tmpDir}
	else:
		return {"SQLITE_TMPDIR": tmpDir}


class DB:
	"""The main class to deal with DB. Defines needed function to allow DB work with regexes and some pragms for optimization. Also has some functions to create or delete tables. Use it as a context manager."""

	regexpWrapper = SQLiteRegexpWrapper()

	def __init__(self, fileName: Path = None):
		if fileName is None:
			fileName = databaseDefaultFileName
		fileName = Path(fileName)

		self.db = sqlite3.connect(str(fileName), 0, True)
		__class__.regexpWrapper.attach(self.db)

		for sq in __class__.genSetupQueries(fileName):
			self.db.execute(sq)

		os.environ.update(getTempDirEnvDict())

	def executescript(self, query, *args, **kwargs):
		print(query, *args, kwargs, file=sys.stderr)
		return self.db.executescript(query, *args, **kwargs)

	def execute(self, query, *args, **kwargs):
		print(query, *args, kwargs, file=sys.stderr)
		return self.db.execute(query, *args, **kwargs)

	getAttachedDatabases = createQueryWrapper("PRAGMA database_list;")

	@property
	def attachedDatabases(self):
		res = {}
		for atDb in self.getAttachedDatabases():
			atDb["file"] = Path(atDb["file"])
			res[atDb["name"]] = atDb
		return res

	def getTables(self, dbID="main"):
		return [TableName(td[0], dbID) for td in self.db.execute("select `name` from " + dbID + ".`sqlite_master` where `type` = 'table';")]

	def getColumns(self, tableName: TableName):
		qw = createQueryWrapper("PRAGMA table_info(" + str(tableName) + ");")
		return qw(self)

	def createTables(self):
		with (sqlFilesDir / "create.sql").open("rt", encoding="utf-8") as f:
			query = f.read()
		self.executescript(query)
		self.executescript(tablesSchemas["csvImportTemp"]())
		self.executescript(tablesSchemas["smart"]())
		self.db.commit()

	def genSetupQueries(fileName: Path = None):
		yield "PRAGMA journal_mode=TRUNCATE;"  # WAL is useless
		#yield "PRAGMA locking_mode=EXCLUSIVE;"
		if fileName:
			yield "PRAGMA main.mmap_size=" + str(getDBMmapSize(fileName, 1024 * 1024 * 1024)) + ";"

	def dropTables(self):
		for tableName in tablesNames:
			self.executescript("DROP TABLE " "" + tableName + ";")
		self.db.commit()

	def __enter__(self):
		return self

	def __exit__(self, *args, **kwargs):
		self.db.__exit__(*args, **kwargs)

	getBrandsDenorm = createQueryWrapper("select * from " + tablesNames["brands"] + " b join " + tablesNames["vendors"] + " v on v.`id`=b.`vendor_id`;")
	getModelsDenorm = createQueryWrapper("select m.*, b.`name` as `brand` from " + tablesNames["models"] + " m join " + tablesNames["brands"] + " b on m.`brand_id`=b.`id`;")
	getBrands = dumbSelect(tablesNames["brands"])
	getModels = dumbSelect(tablesNames["models"])
	getVendors = dumbSelect(tablesNames["vendors"])
	getDrives = dumbSelect(tablesNames["drives"])
	getDrivesWithUnknownModel = createQueryWrapper("select * from " + tablesNames["drives"] + " d join " + tablesNames["models"] + " m on d.`model_id`=m.`id` where m.`brand_id` = 0;")

	def exportTablesIntoExternalDBQueriesGen(tableNames, dstDbId):
		for tn in tableNames:
			tnp = TableName.fromStr(tn)
			yield "insert into " + dstDbId + ".`" + tnp.name + "` select * from " + str(tn) + ";"

	def getTableCreationQuery(self, tableName: TableName):
		if isinstance(tableName, str):
			tnp = TableName.fromStr(tableName)
		else:
			tnp = tableName
		return next(self.execute("select `sql` from " + tnp.dbID + ".`sqlite_master` where `name` = :tblName;", {"tblName": tnp.name}))[0]

	def getTablesCreationQuery(self, tableNames):
		for tn in tableNames:
			yield self.getTableCreationQuery(tn)

	def exportTablesIntoExternalDB(self, tableNames, dstDbId):
		for q in self.__class__.exportTablesIntoExternalDBQueriesGen(tableNames, dstDbId):
			self.execute(q)

	def cloneTablesSchema(self, tableNames, dstDbId):
		#!!!! WE CONNECT TO THE FUTURE CLONE IN A SEPARATE CONNECTION TO ISOLATE THE QUERIES FROM THE MAIN DB NOT TO HARM IT !!!
		with sqlite3.connect(str(self.attachedDatabases[dstDbId]["file"]), 0, True) as tmpDb:
			for q in self.getTablesCreationQuery(tableNames):
				print("Extracted table creation query: ", q, file=sys.stderr)
				tmpDb.execute(q)
			tmpDb.commit()

	def cloneTables(self, tableNames, dstDbId):
		tableNames = list(tableNames)
		self.cloneTablesSchema(tableNames, dstDbId)
		self.exportTablesIntoExternalDB(tableNames, dstDbId)
		self.db.commit()  # othervise we get OperationalError: database is locked
		#self.execute("VACUUM " + dstDbId + ";")  # strange - too long if do it from main connection

		with sqlite3.connect(str(self.attachedDatabases[dstDbId]["file"]), 0, True) as tmpDb:
			tmpDb.execute("VACUUM;")

	def cloneTablesIntoForeignDB(self, tableNames, dbFileName: Path, dbID):
		self.db.execute("ATTACH DATABASE ? AS ?;", (str(dbFileName), dbID))
		self.cloneTables(tableNames, dbID)
		self.db.execute("DETACH DATABASE ?;", (dbID,))

	def exportSomeTables(self, dbFileName: Path = "./drives.sqlite", what=None):
		dbFileName = Path(dbFileName)
		if not what:
			what = ("vendors", "brands", "models", "drives")
		self.cloneTablesIntoForeignDB([tablesNames[tn] for tn in what], dbFileName, "drivesDb")

	def exportToyDB(self, dbFileName=None, size=2 * 1024 * 1024):
		if not dbFileName:
			dbFileName = toyDatabaseDefaultFileName
		dbFileName = Path(dbFileName)

		invertPart = pathRes(self.attachedDatabases["main"]["file"]).stat().st_size // size

		print("We are going to take one model from " + str(invertPart), file=sys.stderr)

		dbID = "toy"
		self.db.execute("ATTACH DATABASE ? AS ?;", (str(dbFileName), dbID))
		self.cloneTables(
			(
				tablesNames[tn]
				for tn in (
					"vendors",
					"brands",
					"models",
					"drives",
					#"anomalies"
				)
			),
			dbID,
		)
		toyTableSMARTName = TableName.fromStr(tablesNames["smart"])
		toyTableSMARTName.dbID = dbID
		mainTableSMARTName = TableName.fromStr(tablesNames["smart"])
		print(repr(invertPart), type(invertPart))
		self.execute("CREATE TABLE " + str(toyTableSMARTName) + " AS select * from " + str(mainTableSMARTName) + " where 0;")
		self.execute(
			"insert into " + str(toyTableSMARTName) +
			"select ds.* from " + str(mainTableSMARTName) + " ds "
			+ "join " + tablesNames["drives"] + " dr on" +
			sqlThisDrive(driveId="dr.`id`", oid="ds.`oid`")
			+ " where (dr.`oid` % ?)==0;", (invertPart,))
		self.db.commit()
		self.db.execute("DETACH DATABASE ?;", (dbID,))
		self.db.commit()

	def addColumn(self, tableName: TableName, columnName, columnType=None):
		self.execute("alter table " + tableName + " add column `" + columnName + "`" + ((" " + columnType) if columnType else "") + ";")

	def renameTable(self, old: TableName, new: TableName):
		self.execute("alter table " + old + " rename to " + new + ";")

	def upgradeTableSchema(self, tableName, newColumns):
		newColumns = list(newColumns)
		clmnsNamesSet = {c["name"] for c in self.getColumns(tableName)}
		columnsToAdd = [c for c in newColumns if c[0] not in clmnsNamesSet]
		columnsToDelete = clmnsNamesSet - {c[0] for c in newColumns}

		print("To add:", columnsToAdd, file=sys.stderr)
		print("To delete:", columnsToDelete, file=sys.stderr)
		if columnsToDelete:
			raise NotImplementedError("Deleting columns with ALTER TABLE is not implemented in SQLite! The other method with writable_schema is intentionally not implemented here for safety reasons.")
		for col in columnsToAdd:
			self.addColumn(tableName, *col)

	def upgradeSchema(self):
		for tableId in ("smart",):  # "csvImportTemp" needs columns reordering
			#print(tablesSchemas[tableId]())
			desiredColumns = tablesSchemas[tableId].genSpecs()
			desiredColumns = flattenIter1Lvl(desiredColumns)
			self.upgradeTableSchema(tablesNames[tableId], desiredColumns)
		self.db.commit()


class DBNormalizer(DB):
	"""Contains functions useful for importing and normalization f data"""

	def normalizeModels(self):
		with (sqlFilesDir / "normalize_models.sql").open("rt", encoding="utf-8") as f:
			query = f.read()
		self.executescript(query)
		self.db.commit()

	def getLastDenormalizedRow(self):
		return next(self.db.execute("select `OID`, " + dateToOrdinal("`date`") + " AS `day`, * from " + tablesNames["csvImportTemp"] + " where unlikely(`oid` = (select max(`oid`) from " + tablesNames["csvImportTemp"] + "))"))

	def getLastNormalizedRow(self):
		return next(self.db.execute("select `OID`, " + dateToISO("`date`") + " AS `day`, * from " + tablesNames["smart"] + " where unlikely(`oid` = (select max(`oid`) from " + tablesNames["smart"] + "));"))

	def getMinDenormalizedRowid(self):
		"""Returns min(`oid`) in csvImportTemp"""
		return next(self.db.execute("select min(`oid`) from " + tablesNames["csvImportTemp"] + ";"))[0]

	def getMaxDenormalizedRowid(self):
		"""Returns max(`oid`) in csvImportTemp"""
		return next(self.db.execute("select max(`oid`) from " + tablesNames["csvImportTemp"] + ";"))[0]

	def getDenormalizedCount(self):
		"""Returns count of entries in csvImportTemp"""
		return next(self.db.execute("select count(*) from " + tablesNames["csvImportTemp"] + ";"))[0]

	@lru_cache(maxsize=1, typed=True)
	def genNormalizeRecordsQuery():
		"""Generates a SQL query for normalizeRecords method"""
		specsSmart = [[s[0] for s in r] for r in super(tablesSchemas["smart"].__class__, tablesSchemas["smart"]).genSpecs()]
		attrsToBeMoved = [[(s[0], "") for s in r] for r in fictiveSpecRepresentingTheAttrsNeededToBeMovedFromTempRecordsTableToPermanentOne.genSpecs()]

		print(specsSmart, file=sys.stderr)
		print(attrsToBeMoved, file=sys.stderr)
		query = "insert into " + tablesNames["smart"] + r"""
			select
				""" + sqlToOid("dr.`id`", dateToOrdinal("t2.`date`"), "`packed_rowid`") + """, 
		"""
		query += ",\n".join(TableSpecGen.genTableColumnsSpecsLines(attrsToBeMoved, "t2", specsSmart))
		query += r"""
			from """ + tablesNames["csvImportTemp"] + r""" t2
			INNER JOIN """ + tablesNames["drives"] + " dr on t2.`serial_number`=dr.`serial_number`"
		return query

	def genNormalizeRecordsQueryConstrained(constraint):
		"""Generates a SQL query for normalizeRecords method"""
		return __class__.genNormalizeRecordsQuery() + " where t2.`oid` < " + str(constraint) + ";"

	def normalizeRecords(self, batchSize=100):
		"""
		Normalizes database structure while importing:
			1 finds the id of drive in `drives` table this record belongs by serial number
			2 parses date
			3 packs drive id and date in rowid to optimize search efficiency"""
		constraint = batchSize + self.getMinDenormalizedRowid()
		maxRowid = self.getMaxDenormalizedRowid()
		size = maxRowid - constraint
		cur = self.db.cursor()
		normalizeQuery = __class__.genNormalizeRecordsQueryConstrained("?")
		deleteQuery = "delete from " + tablesNames["csvImportTemp"] + " where `oid`<? ;"

		def generatorOfProgress(constraint):
			processedRows = 1
			while processedRows > 0:
				yield ((size - maxRowid + constraint), normalizeQuery + "\n" + str(constraint))
				cur.execute(normalizeQuery, (constraint,))
				yield ((size - maxRowid + constraint), deleteQuery + "\n" + str(constraint))
				cur.execute(deleteQuery, (constraint,))
				yield ((size - maxRowid + constraint), "committing...")
				self.db.commit()
				constraint += batchSize
				processedRows = cur.rowcount
			cur.close()
			yield ((size - maxRowid + constraint), "finished")

		return (size, generatorOfProgress(constraint))


@lru_cache(maxsize=4, typed=True)
def genDriveStatsDenormQuery(failed=True, reduced=False):
	"""Generates a sql query to get precomputed stats for the drives in a form convenient for analysis"""
	return (
		"select\n"
		+ ("(a.`failure_date` - a.`first_date`)" if failed else "null")
		+ " as `days_in_dataset_failure`,\n"
		+ "(a.`last_date` - a.`first_date`) as `days_in_dataset`, "
		+ (
			(
				("(fa.`power_on_hours_raw`-fi.`power_on_hours_raw`)/24"                      if failed else "null") + " as `days_in_dataset_failure_smart`,\n"
				+ ("fa.`power_on_hours_raw`/24"                                                if failed else "null") + " as `failure_worked_days_smart`,\n"
				+ "(fi.`power_on_hours_raw`/24 + a.`last_date`-a.`first_date`) as `failure_worked_days_synthetic`,"
				+ r"""
				(la.`power_on_hours_raw`-fi.`power_on_hours_raw`)/24 as `days_in_dataset_smart`,
				la.`power_on_hours_raw`/24 as `total_worked_days_smart`,"""
			)
			if not reduced else ""
		)
		+ r"""a.*
		from """ + tablesNames["drivesAnalytics"] + " a "
		+ (
			(
			"join " + tablesNames["smart"] + " fi on unlikely("+sqlToOidUnoffsetted("a.`id`", "a.`first_date`") + "=fi.`oid`) \n" +
			"join " + tablesNames["smart"] + " la on unlikely("+sqlToOidUnoffsetted("a.`id`", "a.`last_date`") + "=la.`oid`) \n"
			)
			if not reduced else ""
		)
		+
		(
			("join " + tablesNames["smart"] + " fa on unlikely("+sqlToOidUnoffsetted("a.`id`", "a.`failure_date`")+"=fa.`oid`)\n" if not reduced else "")+
			"where unlikely(a.`failure_date` is not NULL)"
			if failed else
			"where likely(a.`failure_date` is NULL)"
		)
	)


@lru_cache(maxsize=2, typed=True)
def genDriveStatsDenormQueryUnioned(reduced=False):
	return (
		genDriveStatsDenormQuery(failed=True, reduced=reduced)
		+ "\nUNION\n"
		+ genDriveStatsDenormQuery(failed=False, reduced=reduced)
	)


def genAnomaliesExclusionWrapperQuery(queryText):
	return ("with anomDrives AS (select `id` from `anomalies`),\n" +
		"drivesRecs AS (\n" + queryText + "\n)\n" +
		"select * from drivesRecs where likely(drivesRecs.`id` not in anomDrives);"
	)


class DBAnalyser(DB):
	"""Contains the functions dealing with computing statistics and removing anomalies"""

	def __init__(self, fileName: Path = None, analyticsDBFileName=None):
		super().__init__(fileName)
		if not analyticsDBFileName:
			analyticsDBFileName = analysisDatabaseDefaultFileName
		analyticsDBFileName = Path(analyticsDBFileName)

		self.db.execute("ATTACH DATABASE ? AS ?;", (str(analyticsDBFileName), analysisDBName))
		self.db.execute("PRAGMA " + analysisDBName + ".mmap_size=" + str(getDBMmapSize(analyticsDBFileName, 12 * 1024 * 1024)) + ";")

	def genArgQuery(func, minOrd=0, driveId=":id", date=None, ordinal="`ord`", oid="`oid`"):
		"""generates a SQL query to get info from the rowid to which a function is applied"""
		return "select " + sqlFromOid(oid=func + "(" + oid + ")", driveId="id", date=date, ordinal=ordinal) + r" from " + tablesNames["smart"] + " where " + sqlThisDrive(driveId, minOrd=minOrd, oid=oid)

	@lru_cache(maxsize=1, typed=True)
	def genComputeStatsForDrivesQuery():
		"""generates a SQL query to compute first and last dates the drive with id :id has in the dataset"""
		return (
			"with fApp AS (\n"
			+ __class__.genArgQuery("min")
			+ r"""
			),
			lApp AS (
			"""
			+ __class__.genArgQuery("max", minOrd="(select `ord` from fApp)") + "\n)\n"
			+ "select :id as `id`, "
			+ "(select `ord` from lApp) as `last_date`, "
			+ "(select `ord` from fApp) as `first_date`;"
		)

	@lru_cache(maxsize=1, typed=True)
	def genRecomputeStatsForDrivesQuery():
		"""generates a SQL query to recompute first and last dates the drive with id :id has in the dataset"""
		return __class__.genArgQuery("max", minOrd=sqlDateToOrd(":last_date"), ordinal="`last_date`")

	def updateDriveRecordsWithStats(self, drivesToComputeStats, query):
		cur = self.db.cursor()
		cur.row_factory = lambda *r: dict(sqlite3.Row(*r))

		protoToCreateTheOnesWhichMayBeNotPresent = {k: None for k in ("failure_date",)}

		for d in drivesToComputeStats:
			d = dict(d)
			cur.execute(query, d)
			for ds in cur:
				dt = type(protoToCreateTheOnesWhichMayBeNotPresent)(protoToCreateTheOnesWhichMayBeNotPresent)
				dt.update(d)
				dt.update(ds)
				yield dt
		cur.close()

	def computeStatsForDrives(self, drivesToComputeStats):
		"""finds first and last dates the drivesToComputeStats have in dataset, augment the records, returns augmented records"""
		yield from self.updateDriveRecordsWithStats(drivesToComputeStats, __class__.genComputeStatsForDrivesQuery())

	def recomputeStatsForDrives(self, drivesToComputeStats):
		"""finds last dates the drivesToComputeStats have in dataset, augment the records, returns augmented records"""
		yield from self.updateDriveRecordsWithStats(drivesToComputeStats, __class__.genRecomputeStatsForDrivesQuery())

	def saveStatsForDrives(self, stats):
		"""saves the computed stats into a DB"""
		stats = iter(stats)
		items0 = next(stats)
		cur = self.db.cursor()
		keys = items0.keys()
		q = (
			"INSERT INTO " + tablesNames["drivesAnalytics"] + " (" + ", ".join(("`" + k + "`" for k in keys)) + ") " +
			"VALUES (" + ", ".join((":" + k for k in keys)) + ");"
		)
		cur.execute(q, items0)
		cur.executemany(q, stats)
		#for s in stats:
		#	cur.execute(q, s)
		#	self.db.commit()
		self.db.commit()

	def findLastOrdinalInAnalytics(self):
		return next(self.db.execute("select max(`last_date`) from " + tablesNames["drivesAnalytics"] + ";"))[0]

	def findLastDateTimeInAnalytics(self):
		return dateTimeFromOrd(self.findLastOrdinalInAnalytics())

	findOutdatedCandidatesStatsRecords = createQueryWrapper("select * from " + tablesNames["drivesAnalytics"] + " where likely(`failure_date` is NULL) AND likely(`last_date` < (select max(`last_date`) from " + tablesNames["drivesAnalytics"] + "));")

	def genFindFailureRecordsQuery(driveId=None, minOrd=None):
		# SHIT, we cannot introduce here selection by date because our rowid structure is optimized for selection by drive
		return (
			"select " + sqlFromOid("`id`", date=None, ordinal="`failure_date`") +
			" from " + tablesNames["smart"] +
			" where unlikely(`failure` = 1)"
			+ (sqlThisDrive(driveId, minOrd=minOrd, oid="`id`") if (minOrd is not None and driveId is not None) else "")
			+ ";"
		)

	findFailureRecords = createQueryWrapper(genFindFailureRecordsQuery())

	getKnownFailedDrivesDates = createQueryWrapper("select " + sqlFromOid("`id`", date=None, ordinal="`failure_date`", oid="st.`oid`") + ", st.`failure`" " from " + tablesNames["smart"] + " st" + " join " + tablesNames["drivesAnalytics"] + " an on (" + sqlThisDrive(driveId="an.`id`", oid="st.`oid`", minOrd="an.`first_date`", maxOrd="an.`last_date`") + ") where unlikely(an.`failure_date` is not NULL);")  # we have to do this shit and then filter manually because SQLITE query optimizer is too dumb and eliminates our rowid hacks

	getKnownFailedDrivesFailureRecords = createQueryWrapper(
		"select " + sqlFromOid("`id`", date=None, ordinal="`failure_date`", oid="st.`oid`") +
		" from " + tablesNames["smart"] + " st" +
		" join " + tablesNames["drivesAnalytics"] + " an on (" + sqlThisDrive(driveId="an.`id`", oid="st.`oid`", minOrd="an.`first_date`", maxOrd="an.`last_date`") + ") where unlikely(an.`failure_date` is not NULL) and unlikely(st.`failure` = 1);"
	)  # doesn't work any better than findFailureRecords, creates a covering index

	findNonevaluatedDrives = createQueryWrapper("select `id` from " + tablesNames["drives"] + " where `id` not in (select `id` from " + tablesNames["drivesAnalytics"] + ");")

	getDrivesStats = dumbSelect(tablesNames["drivesAnalytics"])

	getSavedAnomalies = dumbSelect(tablesNames["anomalies"])

	getDrivesStatsDenorm = createQueryWrapper(genAnomaliesExclusionWrapperQuery(genDriveStatsDenormQueryUnioned()))
	getFailedDrivesStatsDenorm = createQueryWrapper(genAnomaliesExclusionWrapperQuery(genDriveStatsDenormQuery(failed=True)))
	getNonFailedDrivesStatsDenorm = createQueryWrapper(genAnomaliesExclusionWrapperQuery(genDriveStatsDenormQuery(failed=False)))

	getDrivesStatsDenormReduced = createQueryWrapper(genAnomaliesExclusionWrapperQuery(genDriveStatsDenormQueryUnioned(reduced=True)))
	getFailedDrivesStatsDenormReduced = createQueryWrapper(genAnomaliesExclusionWrapperQuery(genDriveStatsDenormQuery(failed=True, reduced=True)))
	getNonFailedDrivesStatsDenormReduced = createQueryWrapper(genAnomaliesExclusionWrapperQuery(genDriveStatsDenormQuery(failed=False, reduced=True)))

	getPostfailureUsedDrives = createQueryWrapper(
		r"""select d.id, (d.`last_date` - d.`failure_date`) as `overshoot`
		from """ + tablesNames["drivesAnalytics"] + """ d
		where unlikely(d.`last_date` > d.`failure_date`);"""
	)

	def saveAnomalies(self, anomaliesItems):
		"""Saves anomalied drives into DB to exclude from analysis"""
		cur = self.db.cursor()
		cur.executemany("insert into " + tablesNames["anomalies"] + " values (:id, :info);", ({"id": k, "info": json.dumps(v)} for k, v in anomaliesItems))
		cur.close()
		self.db.commit()

from plumbum import cli

from .. import database


class DatabaseCommand(cli.Application):
	"""A base class for db-related commands"""

	dbPath = cli.SwitchAttr("--db-path", cli.ExistingFile, default=database.databaseDefaultFileName, help="Path to the SQLite database")

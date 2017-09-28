from plumbum import cli

from ..utils import find7z


class SevenZipCommand(cli.Application):
	"""A base class for archiver-related commands"""

	sevenZipPath = cli.SwitchAttr("--7z-path", cli.ExistingFile, default=find7z(), help="Path to 7z executable")

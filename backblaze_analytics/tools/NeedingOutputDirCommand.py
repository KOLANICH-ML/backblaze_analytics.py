from plumbum import cli


class NeedingOutputDirCommand(cli.Application):
	"""A base class for commands requiring output dir"""

	destFolder = cli.SwitchAttr("--destFolder", cli.switches.MakeDirectory, default="./result", help="A dir to save output. Must be large enough.")

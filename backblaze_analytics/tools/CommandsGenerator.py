import platform
import re
import shlex
import string
import typing
from pathlib import Path
from secrets import choice

from ..utils import *

interprCmd = getInterpreterCommand()


class CommandsGenerator:
	def quote(self, input: str):
		return shlex.quote(str(input))

	def delete(self, fileName: Path):
		fileName = Path(fileName)
		return "rm " + self.quote(fileName.absolute())

	def sqliteFastVacuum(self, fileName: Path):
		"""build https://www.sqlite.org/src/file/tool/fast_vacuum.c"""
		fileName = Path(fileName)
		return "fast_vacuum " + self.quote(fileName.absolute())

	def copy(self, src: Path, dst: Path):
		return "cp " + self.quote(Path(src).absolute()) + " " + self.quote(Path(dst).absolute())

	def sqliteWrap(self, fileName: Path, commands, wrap=None):
		from ..database import DB

		if isinstance(commands, str):
			commands = (commands,)
		commands1 = list(DB.genSetupQueries())
		for command in commands:
			commands1 += command.split("\n")
		sqliteCommand = "sqlite3 -csv " + self.quote(fileName)
		if wrap:
			sqliteCommand = wrap(sqliteCommand)
		return self.multilineEcho(commands1) + " | " + sqliteCommand

	def unpack7z(self, sevenZipPath: Path, tempDir: Path, archiveName: Path, fileName: Path):
		return self.quote(sevenZipPath).replace("'", '"') + " e -aos -o" + self.quote(tempDir) + " " + self.quote(pathRes(archiveName)) + " " + self.quote(fileName)

	def pack7z(self, sevenZipPath: Path, fileName: Path, archiveName: Path, outDir: Path = "./", compression=9, blockSize="64M", fb=273):
		return self.quote(sevenZipPath).replace("'", '"') + " a -tXZ -mx=" + str(compression) + " -m0=LZMA2:d" + blockSize + ":fb" + str(fb) + " -o" + self.quote(outDir) + " " + self.quote(pathRes(archiveName)) + " " + self.quote(fileName)

	def wrapNoSuspend(self, command):
		return interprCmd + " -m NoSuspend " + self.quote(command)

	def backblazeAnalytics(self, subcommands: typing.Iterable[str], dbPath: Path = None):
		return interprCmd + " -m backblaze_analytics " + " ".join(subcommands) + ((" --db-path " + str(dbPath)) if dbPath else "")

	def setEnv(self, name, value):
		return "export " + name + "=" + self.quote(str(value))

	def setEnvs(self, envDict: typing.Mapping[str, str]):
		for k, v in envDict.items():
			yield self.setEnv(k, v)

	def echo(self, input: str):
		return "echo " + self.quote(str(input))

	def multilineEcho(self, lines):
		ls = "\n".join(lines)
		EOF = "EOF"
		while EOF in ls:
			EOF += choice(string.ascii_uppercase + string.digits)

		return 'cat <(cat << "' + EOF + '"\n' + ls + "\n" + EOF + "\n)"


specChar = re.compile("[&\\<|>^]")


class CommandsGeneratorWin(CommandsGenerator):
	"""A class to create shell commands. Create another class for other platforms"""

	def quote(self, input: str):
		# shlex.quote works incorrectly on Windows
		return '"' + str(input).replace('"', '""') + '"'

		# windows

	def delete(self, fileName: Path):
		return "del " + self.quote(Path(fileName).absolute())

	def copy(self, src: Path, dst: Path):
		return "copy " + self.quote(Path(src).absolute()) + " " + self.quote(Path(dst).absolute())

	def echo(self, input: str):
		return "echo " + specChar.subn("^\\g<0>", input)[0]

	def multilineEcho(self, lines):
		return "(\n" + "\n".join((self.echo(line) for line in lines)) + "\n)"

	def setEnv(self, name, value):
		return "set " + name + "=" + str(value)


if platform.system() == "Windows":
	commandGen = CommandsGeneratorWin()
else:
	commandGen = CommandsGenerator()

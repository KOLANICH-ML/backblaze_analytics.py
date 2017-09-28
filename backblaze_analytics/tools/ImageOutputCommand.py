from lazily import lazyImport
from plumbum import cli

from ..utils.isNotebook import isNotebook

plt = lazyImport("matplotlib.pyplot")

if isNotebook():
	ipy = get_ipython()
	ipy.run_line_magic("matplotlib", "inline")
	ipy.run_line_magic("config", "InlineBackend.figure_format = 'svg'")


class ImageOutputCommand(cli.Application):
	"""A base class for commands requiring output dir"""

	destFolder = cli.SwitchAttr("destFolder", cli.switches.MakeDirectory, default=(None if isNotebook() else "./result"), help="A dir to save output. Must be large enough.")

	imageExt = cli.SwitchAttr(["t", "image-ext"], str, default="svg", help="An extension of image files to be generated.")

	@cli.switch(["H", "height"], argtype=int)  # todo: add support of extracting from func annotations to plumbum
	def height(self, height: int = 7):
		"""The height of the image"""
		plt.rcParams["figure.figsize"] = (plt.rcParams["figure.figsize"][0], height)

	@cli.switch(["W", "width"], argtype=int)
	def width(self, width: int = 17):
		"""The width of the image"""
		plt.rcParams["figure.figsize"] = (width, plt.rcParams["figure.figsize"][1])


plt.rcParams["figure.figsize"] = (ImageOutputCommand.width.__defaults__[0], ImageOutputCommand.height.__defaults__[0])  # a workaround since plumbum doesn't capture it for now from func signature and doesn't auto-apply

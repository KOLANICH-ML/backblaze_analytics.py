import plumbum.cli


class BackblazeAnalyticsCLI(plumbum.cli.Application):
	"""The main script for convenience"""

	pass

from .tools.plotLifeLines import AnalysisCLI
BackblazeAnalyticsCLI.subcommand("KM")(AnalysisCLI)

from .tools.RegressionCLI import RegressionCLI
BackblazeAnalyticsCLI.subcommand("regression")(RegressionCLI)

from .tools.importer import Importer
BackblazeAnalyticsCLI.subcommand("import")(Importer)

from .tools.preprocess import Preprocesser
BackblazeAnalyticsCLI.subcommand("preprocess")(Preprocesser)

from .tools.retrieve import DatasetRetriever
BackblazeAnalyticsCLI.subcommand("retrieve")(DatasetRetriever)

from .tools.export import DatasetExporter
BackblazeAnalyticsCLI.subcommand("export")(DatasetExporter)

from .tools.imput import Imputer
BackblazeAnalyticsCLI.subcommand("imput")(Imputer)


if __name__ == "__main__":
	BackblazeAnalyticsCLI.run()

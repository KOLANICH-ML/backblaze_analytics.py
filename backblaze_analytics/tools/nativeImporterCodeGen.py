from collections import defaultdict
from pathlib import Path, PurePath

from plumbum import cli

from .. import database
from ..database import DrivesParamsTableSpec, DrivesStatsTableSpec, TableSpecGen, tablesNames, tablesSchemas
from ..SMARTAttrsNames import SMARTAttrsNames


class CPPSchemaGen(cli.Application):
	"""Generates the files needed for native importer."""

	def main(self, generatedHeadersDir="./faster_importer/src/generated"):
		generatedHeadersDir = Path(generatedHeadersDir)

		for hN, hT in self.generateHeaders().items():
			(generatedHeadersDir / hN).write_text(hT)

	def generateHeaders(self):
		specsSmart = super(tablesSchemas["smart"].__class__.__mro__[1], tablesSchemas["smart"]).genSpecs()
		specsTemp = super(tablesSchemas["csvImportTemp"].__class__.__mro__[1], tablesSchemas["csvImportTemp"]).genSpecs()
		from pprint import pprint

		res = defaultdict(list)

		recordsQueryVarName = "recordsQuery"
		rowVarName = "row"
		rowVarName1 = "r"

		res["tableColumnsNamesVars.h"] = defaultdict(list)

		typesRemap = {
			"INTEGER (1)": "uint8_t",
			"INTEGER (2)": "uint16_t",
			"INTEGER (4)": "uint32_t",
			"INTEGER (8)": "int64_t",  # FUCK, SQLite doesn't have unsigned type
		}

		for line in zip(specsSmart, specsTemp):
			fieldsPairsInLine = list(zip(*line))
			for friendly, raw in fieldsPairsInLine:
				#res += [str(friendly)]
				friendlyNoDashes = friendly[0].replace("-", "_")
				res["tableColumnsNamesStrings.h"].append('"' + raw[0] + '"')
				res["tableColumnsNamesVars1.h"].append("{1}.{0}".format(friendlyNoDashes, rowVarName))
				res["tableColumnsNames.h"].append("`" + friendly[0] + "`")
				res["tableColumnsNamesVars.h"][friendly[1]].append(friendlyNoDashes)
				res["tableColumnsPlaceholders.h"].append(":{0}".format(friendlyNoDashes))
				res["variablesIndices.h"].append('auto {0}Idx=sqlite3_bind_parameter_index({1}.mStmtPtr, ":{0}");'.format(friendlyNoDashes, recordsQueryVarName))
				res["variablesBind.h"].append("{1}.bind({0}Idx, {2}.{0});".format(friendlyNoDashes, recordsQueryVarName, rowVarName1))

		res["countOfSMARTColumns.h"] = str(len(res["tableColumnsNamesStrings.h"]))
		res["tableColumnsNamesStrings.h"] = ", ".join(res["tableColumnsNamesStrings.h"])
		res["tableColumnsNamesVars1.h"] = ", ".join(res["tableColumnsNamesVars1.h"])
		res["tableColumnsPlaceholders.h"] = 'R"smartAttrsNames(\n' + ", ".join(res["tableColumnsPlaceholders.h"]) + '\n)smartAttrsNames"'
		res["tableColumnsNames.h"] = 'R"smartAttrsNames(\n' + ", ".join(res["tableColumnsNames.h"]) + '\n)smartAttrsNames"'
		res["variablesIndices.h"] = "\n".join(res["variablesIndices.h"])
		res["variablesBind.h"] = "\n".join(res["variablesBind.h"])

		res["tableColumnsNamesVars.h"] = "\n".join((typesRemap[k] + " " + ", ".join(v) + ";") for k, v in res["tableColumnsNamesVars.h"].items())

		return res

		#print(list(TableSpecGen.genTableColumnsSpecsLines(specsTemp, "t2", specsSmart)))


if __name__ == "__main__":
	CPPSchemaGen.run()

from collections import OrderedDict


def reorderPandasDataframeColumns(pdf, columnsOrder):
	resCols = OrderedDict((k, None) for k in pdf.columns)  # a surrogate for difference of ordered sets
	for k in columnsOrder:
		if k in resCols:
			del resCols[k]

	missingCols = set(columnsOrder) - set(pdf.columns)
	res = pdf.reindex(list(columnsOrder) + list(resCols.keys()), axis=1)
	for cN in missingCols:
		del res[cN]
	return res

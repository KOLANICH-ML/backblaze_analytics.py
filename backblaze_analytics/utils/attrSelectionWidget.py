from functools import partial

import ipywidgets
from more_itertools import chunked


def createAttrSelectionWidget(dmat, inARow=4):
	#print(selected, allAttrs)

	def onChange(dmat, groupName, event):
		#print(event)
		featureName = event["owner"].description
		if event["new"]:
			dmat.groups[groupName].add(featureName)
			dmat.groups["stop"].remove(featureName)
		else:
			dmat.groups["stop"].add(featureName)
			dmat.groups[groupName].remove(featureName)

		#print(selected)

	onChange = partial(onChange, dmat)

	def makeCbxForAtr(groupName, atr):
		#print(atr, atr in selected)
		cbx = ipywidgets.Checkbox(value=(atr in dmat.groups[groupName]), description=atr, disabled=False)
		cbx.observe(partial(onChange, groupName), "value")
		return cbx

	groups = []
	for groupName, group in dmat.groups.items():
		items = []
		for atr in group:
			items.append(makeCbxForAtr(groupName, atr))
		groups.append(ipywidgets.VBox([ipywidgets.Label(groupName), ipywidgets.VBox([ipywidgets.Box(ch) for ch in chunked(items, inARow)])]))
	return ipywidgets.VBox(groups)

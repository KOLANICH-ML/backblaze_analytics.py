def chainSHAP(combinerFuncArgs, combinerSHAP, combineesArgs, combineesSHAPs):
	# combinerSHAP i sample j feature
	# combineesSHAP j function i sample k feature
	# combineesArgs j function i sample k feature
	# combinerFuncArgs

	combineesSHAPs = np.transpose(combineesSHAPs, (1, 0, 2))
	combineesArgs = np.transpose(combineesArgs, (1, 0, 2))
	# combineesSHAP i sample j function k feature
	# combineesArgs i sample j function k feature

	combinerArgsDemeaned = combinerFuncArgs - np.mean(combinerFuncArgs, axis=0)
	combinerM = combinerSHAP / combinerArgsDemeaned  # i, j

	combineesArgsDemeaned = combineesArgs - np.mean(combineesArgs, axis=0)
	combineesM = combineesSHAPs / combineesArgsDemeaned  # i, j, k

	res = np.sum(combineesM * np.expand_dims(combinerM * combinerArgsDemeaned, 1), axis=1)  # i sample k feature
	return res


class ChainedSHAP:
	def __init__(self, func):
		self.func = func

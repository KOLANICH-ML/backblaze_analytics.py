import typing


class LearningTask:
	__slots__ = ("pds", "learnedVars")

	def __init__(self, pds: "pandas.DataFrame", learnedVars: typing.Mapping[str, str]):
		self.pds = pds
		self.learnedVars = learnedVars

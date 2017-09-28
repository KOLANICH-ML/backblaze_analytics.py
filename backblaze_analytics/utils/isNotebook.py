__author__ = "Gustavo Bezerra"


def isNotebook():
	"""https://stackoverflow.com/questions/15411967/how-can-i-check-if-code-is-executed-in-the-ipython-notebook/39662359#39662359"""
	try:
		shell = get_ipython().__class__.__name__
		if shell == "ZMQInteractiveShell":
			return True  # Jupyter notebook or qtconsole
		elif shell == "TerminalInteractiveShell":
			return False  # Terminal running IPython
		else:
			return False  # Other type (?)
	except NameError:
		return False  # Probably standard Python interpreter

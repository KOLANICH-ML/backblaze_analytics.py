from .isNotebook import isNotebook

"""Magic calling needed tqdm depending on the environment"""

if isNotebook():
	from tqdm._tqdm_notebook import tqdm_notebook

	mtqdm = tqdm_notebook
else:
	from tqdm import tqdm

	mtqdm = tqdm

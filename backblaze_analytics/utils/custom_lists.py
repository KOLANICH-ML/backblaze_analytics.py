__license__ = r"""
This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

For more information, please refer to <https://unlicense.org/>
"""


class SilentList(list):
	"""A list automatically extending to fit the key"""

	def __getitem__(self, key):
		if key < len(self):
			return super().__getitem__(key)
		else:
			return None

	def __setitem__(self, key, value):
		maxLenNeeded = key + 1
		count = maxLenNeeded - len(self)
		super().extend(count * [None])
		return super().__setitem__(key, value)


class CustomBaseList(SilentList):
	"""A list which starting index is not zero"""

	base = 1

	def idxSub(self, index, another):
		if isinstance(index, slice):
			start = index.start
			stop = index.stop
			step = index.step
			if start is None:
				start = self.base
			if stop is None:
				stop = -1
			start = self.idxSub(start, another)
			stop = self.idxSub(stop, another)
			index = type(index)(start, stop, step)
		else:
			if index > 0:
				index -= another
		return index

	def __getitem__(self, key):
		return super().__getitem__(self.idxSub(key, self.base))

	def __setitem__(self, key, value):
		return super().__setitem__(self.idxSub(key, self.base), value)

	def __enumerate__(self):
		raise NotImplementedError()

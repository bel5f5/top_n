import sys
import heapq
import os

K  = 100
GB = 1024 * 1024 * 1024
g_index_map = {}

class UrlMap(dict):
	def __init__(self):
		dict.__init__(self)
		self.__heap = []
		self.__size    = 0

	def dump(self, _index):
		# Find and dump the top-K URL in the sub-file.
		while len(self) > 0:
			k,v = self.popitem()
			if len(self.__heap) < K:
				heapq.heappush(self.__heap, (v,k))
			else:
				heapq.heappushpop(self.__heap, (v,k))

		with open("result_%d" %_index, 'a+') as fd:
			while len(self.__heap) > 0:
				v,k = heapq.heappop(self.__heap)
				fd.write("%d:%s\n"%(v,k))

class SubFileInfo(object):
	def __init__(self, _index):
		self.__size     = 0
		self.__index    = _index
		self.__urls     = []

	def __add_size(self, _size):
		self.__size += _size

	def size(self):
		return self.__size

	def add(self, _url):
		# url is accumulated in the memory for later flush.
		if len(_url) + self.__size >= GB/K:
			self.flush()
		self.__add_size(len(_url))
		self.__urls.append(_url)

	def path(self):
		return "file_%d" %self.__index

	def flush(self):
		with open('file_%d' %self.__index, 'a+') as fd:
			fd.write('\n'.join(self.__urls)+"\n")
			while len(self.__urls):
				self.__urls.pop()

	def sort(self):
		# Calculate the url occurences, and dump the result.
		with open("file_%d" % self.__index) as fd:
			url_map = UrlMap()
			while True:
				line_ = fd.readline()
				if not line_:
					break
				if line_[-1] == '\n':
					line_ = line_[:-1]
				if not url_map.has_key(line_):
					url_map[line_] = 0
				url_map[line_] += 1
			url_map.dump(self.__index)

def str_to_int(self, str_):
	# convert from string to an integer value.
	v = 0
	j = 0
	for i in str_:
		v += (1 << j ) * ord(i)
		j += 1
	return v

def split_urls(url_file_, limit_, base_= 0, func_=hash):
	# Split the large file into sub-files for processing.
	subfile_map = {}
	with open(url_file_) as fd:
		while True:
			line_    = fd.readline()
			if not line_:
				break
			if line_[-1] == '\n':
				line_    = line_[:-1]

			# A hash function is used for grouping urls into sub-files.
			fd_idx_  = func_(line_) % limit_ + base_
			g_index_map.setdefault(fd_idx_, 0)
			# Add the url into the sub-file.
			subfile = subfile_map.setdefault(fd_idx_, SubFileInfo(fd_idx_))
			subfile.add(line_)

		# Persistent all sub-files.
		for k,v in subfile_map.iteritems():
			v.flush()

	for idx_, subfile_ in subfile_map.iteritems():
		if subfile_.size() > GB:
			#Continue split until the resulting subfile's size fits the memory.
			split_urls(subfile_.path(), limit_, limit_ + base_, str_to_int)
		else:
			# The subfile's size fits the memory, calculate the occurence of the URLs.
			subfile_.sort()

def merge_result():
	# We merge all the sub-results for picking up the top-K URLs.
	heap_ = []
	for i in g_index_map.iterkeys():
		with open("result_%d" %i) as fd:
			line_ = fd.readline()
			if not line_:
				break
			fields_ = line_[:-1].split(':')
			v = int(fields_[0])
			k = ":".join(fields_[1:])
			if len(heap_) < K:
				heapq.heappush(heap_, (v,k))
			else:
				heapq.heappushpop(heap_, (v,k))
	# Now print the result.
	while len(heap_) > 0:
		print heapq.heappop(heap_)

def cleanup_result():
	for i in g_index_map.iterkeys():
		os.unlink("file_%d" %i)
		os.unlink('result_%d' %i)

if __name__ == "__main__":
	if len(sys.argv) < 2:
		print "Usgae: python %s url_file" %sys.argv[0]
		sys.exit(1)

	split_urls(sys.argv[1], K)
	merge_result()
	cleanup_result()

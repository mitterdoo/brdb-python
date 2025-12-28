from struct import Struct, unpack, pack, unpack_from, pack_into
from dataclasses import dataclass, field
from enum import Enum
from os import SEEK_SET, SEEK_CUR, SEEK_END
from errors import *
from io import BytesIO
from blake3 import blake3
import os
import os.path
import zstd

class EFormatVersion(Enum):
	INITIAL = 0

class ECompressionMethod(Enum):
	NONE = 0
	ZSTD = 1

@dataclass
class BRZIndex:
	folder_count: int = 0
	file_count: int = 0
	blob_count: int = 0
	folders: list[tuple[str, int]] = field(default_factory=list) # (name: str, parent: int)
	files: list[tuple[str, int, int]] = field(default_factory=list) # (name:str, parent: int, content: int)
	compression_methods: list[ECompressionMethod] = field(default_factory=list) 
	decompressed_lengths: list[int] = field(default_factory=list) 
	compressed_lengths: list[int] = field(default_factory=list)
	blob_hashes: [list[bytes]] = field(default_factory=list)
	blobs: list[bytes] = field(default_factory=list)

BRZFile = None # sigh... forward declaration for using the type later
@dataclass
class BRZFile:
	name: str = ''
	parent: BRZFile = None
	data: bytes = None
	is_folder: bool = False
	def path(self):
		names = []
		item = self
		while item != None:
			names.insert(0, item.name)
			item = item.parent
		return '/'.join(names) # this isn't M$ (or Linux/Unix) so we don't need to worry about using the appropriate separator


@dataclass
class BRZFolder(BRZFile):
	children: dict[str, BRZFile] = field(default_factory=dict)
	is_folder: bool = True

@dataclass
class BRZ:
	version: EFormatVersion = EFormatVersion.INITIAL
	index_compression_method: ECompressionMethod = ECompressionMethod.NONE
	index_decompressed_length: int = 0
	index_compressed_length: int = 0
	index_hash: bytes = b''
	index: BRZIndex = field(default_factory=BRZIndex)
	tree: BRZFolder = field(default_factory=BRZFolder)

	def dump(self, path: str):
		'''
		Dumps the root folder of the BRZ to the provided directory in the system.
		Hopefully this doesn't cause too much trouble with Windows paths??? (:
		'''
		
		os.mkdir(path) # throws error automatically if it exists. this is intended.
		
		queue = list(self.tree.children.values())
		while len(queue) > 0:
			item = queue.pop(0)

			item_path = item.path()
			combined_path = os.path.join(path, item_path.removeprefix('/'))
			if item.is_folder:
				os.mkdir(combined_path)
				queue.extend(list(item.children.values()))
			else:
				with self.open(item_path, 'r') as buffer:
					with open(combined_path, 'wb') as output:
						output.write(buffer.read())
	
	def open(self, path: str, mode: str) -> BytesIO:
		# open the BRZ file as a BytesIO stream.
				
		# r mode for read
		# w mode for write
		# only binary mode
		# this really just tells the code whether it needs to check for existing file
		# is there a cleaner way to do this? it just feels weird cause BytesIO doesn't care about read/write
		# meh
		exists = self.exists(path)
		file = None
		if exists:
			if self.isdir(path):
				raise IsADirectoryError(f'path "{path}" is a folder')
			file = self._locate(path)

		if mode.lower() == 'r' and not exists:
			# sanity check for john devlopr
			raise FileNotFoundError(f'attempt to open non-existent file {path} for reading')

		if file is None:
			parent_path = self.dirname(path)
			parent = self._locate(parent_path)
			file = BRZFile(self.basename(path), parent, b'')
		stream = BytesIO(file.data)
		return stream
		
	def mkdir(self, path):
		pass
	
	def remove(self, path):
		pass
	
	def dirname(self, path):
		separated = self._split(path)
		return separated[0:-1] # get everything except last item. auto-handles empty lists

	def basename(self, path):
		separated = self._split(path)
		if len(separated) > 0:
			return separated[-1]
		else:
			return ''
	
	def ls(self, path) -> list[str]:
		folder = self._locate(path)
		if not folder.is_folder:
			raise NotADirectoryError(f'path "{folder.path()}" is not a folder')

		return list(folder.children.keys())
	
	def exists(self, path) -> bool:
		try:
			self._locate(path)
		except FileNotFoundError:
			return False
		return True

	def isdir(self, path) -> bool:
		item = self._locate(path)
		return item.is_folder
	
	def _split(self, path):
		# split a path into components
		separated = path.split('/') # not sure if the format supports slashes in names. assuming not, since the devs are probably sane
		if len(separated) > 0 and separated[0] == '':
			del separated[0]
		if len(separated) > 0 and separated[-1] == '':
			del separated[-1]
		return separated

	def _locate(self, path) -> BRZFile:
		# find a file/folder by the given path
		separated = self._split(path)
		current = self.tree

		helper_path = []
		for name in separated:
			if name in current.children:
				current = current.children[name]
				helper_path.append(current) # breadcrumb in case we fail
			else:
				raise FileNotFoundError(f'could not find file "{helper_path.join("/")}"')
		return current


class BRZReader:
	def __init__(self, file):
		self.file = file
		self.brz = BRZ()
	
	def _read(self, count, f = None) -> bytes:
		if f == None:
			f = self.file
		data = f.read(count)
		if len(data) < count:
			raise BRZUnexpectedEOF(f'unexpected EOF when trying to read {count} byte(s); got {len(data)} instead')
		return data
	
	def _decompress(self, method: ECompressionMethod, count: int, expected_hash: bytes, f = None) -> bytes:
		if f == None:
			f = self.file
		compressed = self._read(count, f)
		match method:
			case ECompressionMethod.NONE:
				result_hash = blake3(compressed).digest()
				if result_hash != expected_hash:
					raise BRZDecompressionError('file hash mismatch')
				return compressed
			case ECompressionMethod.ZSTD:

				decompressed = zstd.decompress(compressed)
				result_hash = blake3(decompressed).digest()
				if result_hash != expected_hash:
					raise BRZDecompressionError('file hash mismatch')
				return decompressed

			case _:
				raise BRZFormatError(f'unsupported decompression method {method}')
	
	def read_archive(self):
		self.file.seek(0, SEEK_SET)
		self.read_header()
		self.read_index()

		self.brz.index.blobs = []
		for i in range(self.brz.index.blob_count):
			self.read_blob(i)

		self._construct_tree()

	def read_header(self):
		f = self.file
		brz = self.brz
		self.validate_magic()
		brz.version = unpack('<B', self._read(1))[0]
		if brz.version not in EFormatVersion:
			raise BRZVersionError(f'version {brz.version} is not supported')
		
		comp_method = unpack('<B', self._read(1))[0]
		try:
			comp_method = ECompressionMethod(comp_method)
			brz.index_compression_method = comp_method
		except ValueError:
			raise BRZFormatError(f'unsupported compression method {comp_method}')

		brz.index_decompressed_length, brz.index_compressed_length = unpack('<ii', self._read(8))
		if brz.index_decompressed_length < 0:
			raise BRZFormatError(f'index decompressed length is less than 0 ({brz.index_decompressed_length})')

		if brz.index_compressed_length < 0:
			raise BRZFormatError(f'index compressed length is less than 0 ({brz.index_compressed_length})')

		brz.index_hash = self._read(32)
		
		
	
	def validate_magic(self):
		data = self._read(3)
		if data != b'BRZ':
			raise BRZFormatError(f'invalid magic bytes{repr(data)} expected b\'BRZ\'')
	
	def read_index(self):
		f = self.file
		brz = self.brz
		
		index_decompressed = self._decompress(brz.index_compression_method, brz.index_compressed_length, brz.index_hash)
		if len(index_decompressed) != brz.index_decompressed_length:
			raise BRZDecompressionError(f'index decompresses to {len(index_decompressed)} bytes, but we expected {brz.index_decompressed_length}')
	
		with BytesIO(index_decompressed) as index:
			folder_count, file_count, blob_count = unpack('<iii', self._read(4 * 3, index))
			folder_parents = [unpack('<i', self._read(4, index))[0] for _ in range(folder_count)]
			folder_name_lengths = [unpack('<H', self._read(2, index))[0] for _ in range(folder_count)]
			folder_names = [self._read(folder_name_lengths[i], index).decode('utf-8') for i in range(folder_count)]
			
			file_parents = [unpack('<i', self._read(4, index))[0] for _ in range(file_count)]
			file_contents = [unpack('<i', self._read(4, index))[0] for _ in range(file_count)]
			file_name_lengths = [unpack('<H', self._read(2, index))[0] for _ in range(file_count)]
			file_names = [self._read(file_name_lengths[i], index).decode('utf-8') for i in range(file_count)]

			blob_compression_methods = [ECompressionMethod(unpack('<B', self._read(1, index))[0]) for _ in range(blob_count)]
			blob_decompressed_lengths = [unpack('<i', self._read(4, index))[0] for _ in range(blob_count)]
			blob_compressed_lengths = [unpack('<i', self._read(4, index))[0] for _ in range(blob_count)]
			blob_hashes = [self._read(32, index) for _ in range(blob_count)]


			brz.index.folder_count = folder_count
			brz.index.file_count = file_count
			brz.index.blob_count = blob_count
			# parse file folder and blob data into the nice properties of this obj

			brz.index.folders = []
			for i in range(folder_count):
				brz.index.folders.append((folder_names[i], folder_parents[i]))

			brz.index.files = []
			for i in range(file_count):
				brz.index.files.append((file_names[i], file_parents[i], file_contents[i]))

			brz.index.compression_methods = blob_compression_methods
			brz.index.decompressed_lengths = blob_decompressed_lengths
			brz.index.compressed_lengths = blob_compressed_lengths
			brz.index.blob_hashes = blob_hashes

	def read_blob(self, i):
		f = self.file
		brz = self.brz

		blob_decompressed = self._decompress(brz.index.compression_methods[i], brz.index.compressed_lengths[i], brz.index.blob_hashes[i])
		if len(blob_decompressed) != brz.index.decompressed_lengths[i]:
			raise BRZDecompressionError(f'blob {i} decompresses to {len(blob_decompressed)} bytes, but we expected {brz.index.decompressed_lengths[i]}')

		brz.index.blobs.append(blob_decompressed)

	def _construct_tree(self):
		# BOLD ASSUMPTION:
		# File/folder names cannot be duplicated in the same path
		f = self.file
		brz = self.brz
		
		brz.tree = BRZFolder()
		folders = [] # references to BRZFolders by index

		# propagate folders first, just get the indexes of parents for now
		for folder_name, folder_parent_id in brz.index.folders:
			folder = BRZFolder(children = {})
			folder.parent = folder_parent_id
			folder.name = folder_name
			folders.append(folder)

		files = []
		# get files next, but don't parent.
		for file_name, file_parent_id, file_blob_id in brz.index.files:
			if file_blob_id < 0 or file_blob_id >= len(brz.index.blobs):
				raise BRZFormatError(f'file "{file_name}" points to nonexistent blob {file_blob_id}')

			file = BRZFile(file_name, file_parent_id, brz.index.blobs[file_blob_id])
			#file = BRZFile(parent = file_parent_id, name=file_name, data = brz.index.blobs[file_blob_id])
			files.append(file)

		# now assign the parents to the instances of BRZFolders and BRZFiles.
		# the code should be identical regardless of whether it's a file or folder
		combined = folders.copy()
		combined.extend(files)
		for item in combined:
			if item.parent == -1:
				item.parent = brz.tree
			else:
				if item.parent < 0 or item.parent >= len(folders):
					raise BRZFormatError(f'folder has a parent {item.parent} that doesn\'t exist (max folder index is {len(folders)-1})')
				item.parent = folders[item.parent]
			
			if item.name in item.parent.children:
				raise BRZFormatError(f'folder "{item.parent.path()}" already has child item "{item.name}" but a duplicate is trying to be added')
			item.parent.children[item.name] = item

if __name__ == '__main__':
	with open('single brick.brz', 'rb') as f:
		reader = BRZReader(f)
		reader.read_archive()
		

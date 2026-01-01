"""a "lite" version of the msgpack standard.
Only has functionality to read and write the standard control tags (like fixint, fixarray, etc.)
"""

from struct import pack, unpack, calcsize
from enum import Enum

TAGS = {}

"""
I'm sorry i know this isn't the greatest way to do this.
But i didn't want to get stunlocked just while trying to figure out "the correct and most efficient way" to do this, or else I'd be too deep in the rabbit hole and lose motivation on this project.
"""

class Tag:
	def __init__(self, name: str, tag: int, tag_mask: int = 0xFF, fmt: str = ''):
		self.name = name
		self.tag = tag
		self.tag_mask = tag_mask
		self.data_size = calcsize(fmt)
		self.fmt = fmt
		TAGS[name] = self
	
	def match(self, byte: int) -> bool:
		"""Checks the byte to see if it matches this tag"""
		return (byte & self.tag_mask) == self.tag
	
	def get_value(self, byte: int) -> int:
		"""Extracts the value of a byte, separating it from the tag.
		Example: for fixstr, the tag mask is the 3 highest bits (of a byte), and the value for the size up to 31 bytes"""
		value_mask = (~self.tag_mask) & 0xFF
		if value_mask == 0:
			return 0

		return byte & value_mask

Tag('+fixint', 0, 0b10000000)
Tag('-fixint', 0b11100000, 0b11100000)
Tag('fixmap', 0b10000000, 0b11110000)
Tag('fixarray', 0b10010000, 0b11110000)
Tag('fixstr', 0b10100000, 0b11100000)

Tag('nil', 0xc0)
Tag('(never used)', 0xc1)
Tag('false', 0xc2)
Tag('true', 0xc3)

Tag('bin8', 0xc4, fmt='>B')
Tag('bin16', 0xc5, fmt='>H')
Tag('bin32', 0xc6, fmt='>I')

Tag('ext8', 0xc7, fmt='>Bb')
Tag('ext16', 0xc8, fmt='>Hb')
Tag('ext32', 0xc9, fmt='>Ib')

Tag('float32', 0xca, fmt='>f')
Tag('float64', 0xcb, fmt='>d')

Tag('uint8', 0xcc, fmt='>B')
Tag('uint16', 0xcd, fmt='>H')
Tag('uint32', 0xce, fmt='>I')
Tag('uint64', 0xcf, fmt='>Q')

Tag('int8', 0xd0, fmt='>b')
Tag('int16', 0xd1, fmt='>h')
Tag('int32', 0xd2, fmt='>i')
Tag('int64', 0xd3, fmt='>q')

Tag('fixext1', 0xd4, fmt='>b1s')
Tag('fixext2', 0xd5, fmt='>b2s')
Tag('fixext4', 0xd6, fmt='>b4s')
Tag('fixext8', 0xd7, fmt='>b8s')
Tag('fixext16', 0xd8, fmt='>b16s')

Tag('str8', 0xd9, fmt='>B')
Tag('str16', 0xda, fmt='>H')
Tag('str32', 0xdb, fmt='>I')

Tag('array16', 0xdc, fmt='>H')
Tag('array32', 0xdd, fmt='>I')

Tag('map16', 0xde, fmt='>H')
Tag('map32', 0xdf, fmt='>I')

class FamilyBase:
	def serialize(writer, data):
		raise NotImplemented
	
	def deserialize(reader) -> any:
		raise NotImplemented

class IntegerFamily(FamilyBase):
	def serialize(writer, data, bits=None):
		"""Serializes the data into a writer stream.
		Automatically selects the lowest possible bits to use, but can be overridden with bits argument"""
		raise NotImplemented
		# assert type(data) is int, f'expecting data of type int but got \'{type(data)}\''
	

class MPLReader:
	def __init__(self, file_like):
		self.file = file_like
	
	def read_next(self):
		"""Reads the next Tag"""
		data = self.file.read(1)
		assert data is not None, "Unexpected EOF"
		data = data[0] # converts to int but int(data) does not? thanks python
		for tag_name in TAGS:
			tag = TAGS[tag_name]
			if tag.match(data):
				return self._unpack_tag(tag, data)
		raise ValueError(f'unknown msgpack tag {hex(data)}')
		
	def _unpack_tag(self, tag: Tag, tag_byte: int):
		"""Unpacks a Tag's name and a tuple of any  subsequent values that come after it"""
		if tag.data_size > 0:
			# expecting to read multiple things
			data = self.file.read(tag.data_size)
			assert (data is not None) and (len(data) == tag.data_size), 'Unexpected EOF'
			values = unpack(tag.fmt, data)
			return tag.name, values

		else:
			# data is embedded in that same first byte
			return tag.name, (tag.get_value(tag_byte))
	"""
	I thought about adding functionality that would read a tag, and then the arbitrary data after it (such as arrays, maps, or byte arrays).
	But since this is just a lite module made for parsing/writing raw tags and maybe some values, i decided not to.
	Instead, it just returns the name of the tag, and any values associated with it.
	Reading any subsequent data such as array elements or byte buffers is left up to the tag interpreter (i.e. MPS)
	"""



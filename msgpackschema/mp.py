"""a "lite" version of the msgpack standard.
Only has functionality to read and write the standard control tags (like fixint, fixarray, etc.)

Tag is a "registered" Tag that is part of the format.
Marker is an instantiated class that has an assigned Tag type, and an optional value if extracted from the single byte
"""

from struct import pack, unpack, calcsize
from enum import Enum

TAGS = {}
TAG_PY_TYPES = {}

"""
I'm sorry i know this isn't the greatest way to do this.
But i didn't want to get stunlocked just while trying to figure out "the correct and most efficient way" to do this, or else I'd be too deep in the rabbit hole and lose motivation on this project.
"""

class Tag:
	def __init__(self, name: str, underlying_type: type, tag: int, tag_mask: int = 0xFF, fmt: str = ''):
		self.name = name
		self.underlying_type = underlying_type
		self.tag = tag
		self.tag_mask = tag_mask
		self.data_size = calcsize(fmt)
		self.fmt = fmt
		TAGS[name] = self
		TAG_PY_TYPES[name] = underlying_type
	
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

class Marker:
	def __init__(self, tag: Tag, value: any = None):
		self.tag = tag
		self.value = value
	
	"""
	Allow syntax of checking if Marker is a certain type
	e.g. Marker(Tag('fixmap')) == 'fixmap' would return True
	Also allows 'in' keyword
	Marker(Tag('fixmap')) in ('fixmap', 'map16', 'map32') would return True
	"""
	def __eq__(self, other):
		if type(other) is str:
			return self.tag.name == other
		else:
			return super().__eq__(other)
	
	def __ne__(self, other):
		if type(other) is str:
			return self.tag.name != other
		else:
			return super().__ne__(other)

Tag('+fixint', int, 0, 0b10000000)
Tag('-fixint', int, 0b11100000, 0b11100000)
Tag('fixmap', dict, 0b10000000, 0b11110000)
Tag('fixarray', list, 0b10010000, 0b11110000)
Tag('fixstr', str, 0b10100000, 0b11100000)

Tag('nil', None, 0xc0)
Tag('(never used)', None, 0xc1)
Tag('false', bool, 0xc2)
Tag('true', bool, 0xc3)

Tag('bin8', bytes, 0xc4, fmt='>B')
Tag('bin16', bytes, 0xc5, fmt='>H')
Tag('bin32', bytes, 0xc6, fmt='>I')

Tag('ext8', bytes, 0xc7, fmt='>Bb')
Tag('ext16', bytes, 0xc8, fmt='>Hb')
Tag('ext32', bytes, 0xc9, fmt='>Ib')

Tag('float32', float, 0xca, fmt='>f')
Tag('float64', float, 0xcb, fmt='>d')

Tag('uint8', int, 0xcc, fmt='>B')
Tag('uint16', int, 0xcd, fmt='>H')
Tag('uint32', int, 0xce, fmt='>I')
Tag('uint64', int, 0xcf, fmt='>Q')

Tag('int8', int, 0xd0, fmt='>b')
Tag('int16', int, 0xd1, fmt='>h')
Tag('int32', int, 0xd2, fmt='>i')
Tag('int64', int, 0xd3, fmt='>q')

Tag('fixext1', bytes, 0xd4, fmt='>b1s')
Tag('fixext2', bytes, 0xd5, fmt='>b2s')
Tag('fixext4', bytes, 0xd6, fmt='>b4s')
Tag('fixext8', bytes, 0xd7, fmt='>b8s')
Tag('fixext16', bytes, 0xd8, fmt='>b16s')

Tag('str8', str, 0xd9, fmt='>B')
Tag('str16', str, 0xda, fmt='>H')
Tag('str32', str, 0xdb, fmt='>I')

Tag('array16', list, 0xdc, fmt='>H')
Tag('array32', list, 0xdd, fmt='>I')

Tag('map16', dict, 0xde, fmt='>H')
Tag('map32', dict, 0xdf, fmt='>I')

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

def read_marker(buf) -> Marker:
	"""Reads a single byte and decodes it into a Marker, embedding any necessary value inside"""
	data = buf.read(1)
	assert len(data) == 1, 'Unexpected EOF'
	data = data[0] # convert to int

	for tag_name in TAGS:
		tag = TAGS[tag_name]
		if tag.match(data):
			if tag.tag_mask != 0xFF:
				return Marker(tag, tag.get_value(data))
			else:
				return Marker(tag)
	raise ValueError(f'unknown msgpack tag {hex(data)}')

def read_next(buf, *expected_types):
	"""Reads the next msgpack Tag from the given file-like buffer, and the unpacked value(s) if any.
	List any Tag or compatible type to do an extra check and make sure the intended type was read"""
	marker = read_marker(buf)
	if expected_types is not None and marker not in expected_types:
		raise TypeError(f'expected a Tag compatible with expected type(s) {repr(expected_types)} but got \'{marker.tag.name}\' instead')

	return _unpack_tag(buf, marker)

def read_any(buf):
	"""Reads any msgpack Tag, returning the name of the Tag read, and the values that were unpacked from it.
	Does not include any n-length arrays of data afterwards"""
	marker = read_marker(buf)
	return _unpack_tag(buf, marker)

def _unpack_tag(buf, marker: Marker):
	"""Unpacks a Tag's name and a tuple of any subsequent values that come after it"""
	tag = marker.tag
	if tag.data_size > 0:
		# expecting to read multiple things
		data = buf.read(tag.data_size)
		assert len(data) == tag.data_size, 'Unexpected EOF'
		values = unpack(tag.fmt, data)
		return tag.name, values

	else:
		# data is embedded in that same first byte as stored in Marker
		return tag.name, tuple([marker.value])

"""
I thought about adding functionality that would read a tag, and then the arbitrary data after it (such as arrays, maps, or byte arrays).
But since this is just a lite module made for parsing/writing raw tags and maybe some values, i decided not to.
Instead, it just returns the name of the tag, and any values associated with it.
Reading any subsequent data such as array elements or byte buffers is left up to the tag interpreter (i.e. MPS)
"""



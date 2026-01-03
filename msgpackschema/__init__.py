import msgpack
import logging
from .errors import *
from .msgpack_lite import MPLReader, TAG_PY_TYPES
from struct import unpack, calcsize

VALID_TYPES = {
	'bool': ('true', 'false'),
	'u8': ('+fixint', 'uint8'),
	'u16': ('+fixint', 'uint8', 'uint16'),
	'u32': ('+fixint', 'uint8', 'uint16', 'uint32'),
	'u64': ('+fixint', 'uint8', 'uint16', 'uint32', 'uint64'),

	'i8': ('+fixint', '-fixint', 'int8', 'uint8'),
	'i16': ('+fixint', '-fixint', 'int8', 'uint8', 'int16', 'uint16'),
	'i32': ('+fixint', '-fixint', 'int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32'),
	'i64': ('+fixint', '-fixint', 'int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32', 'int64'),

	'f32': ('+fixint', '-fixint', 'int8', 'int16', 'uint8', 'uint16', 'float32'),
	'f64': ('+fixint', '-fixint', 'int8', 'int16', 'int32', 'uint8', 'uint16', 'uint32', 'float32', 'float64'),

	'str': ('fixstr', 'str8', 'str16', 'str32'),

	'object': ('+fixint', '-fixint', 'int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32'),
	'class': ('+fixint', '-fixint', 'int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32')

}

VALID_ENUM_TYPES = (bool, int)

class PropertyType:
	def validate_mp_type(self, mp_type: str):
		"""After reading a Tag from msgpack, checks if the type of the tag fits the built-in type.
		Example:
		When expecting to read a 'u16', then reading a msgpack Tag, the type it returns could be a fixint, uint8, or uint16. 
		See below link for more info:
		https://gist.github.com/Zeblote/053d54cc820df3bccad57df676202895#schema-usage-spec
		"""
		

class Value(PropertyType):
	def __init__(self, the_type: str):
		self.type = the_type
	
	def __repr__(self):
		return f'Value(\'{self.type}\')'

class Array(PropertyType):
	def __init__(self, item_type: str, is_flat=False):
		self.type = item_type
		self.is_flat = is_flat
	
	def __repr__(self):
		if self.is_flat:
			return f'Array(\'{self.type}\', is_flat=True)'
		else:
			return f'Array(\'{self.type}\')'

class Map(PropertyType):
	def __init__(self, key_type: str, value_type: str):
		self.key_type = key_type
		self.value_type = value_type
	
	def __repr__(self):
		return f'Type(\'{self.key_type}\', \'{self.value_type}\')'


class MPS:
	"""A msgpack-schema class for reading and writing .mps files for Brickadia.
	It uses msgpack types under the hood, but since no data is needed for making key-value pairs in the .mps fille (offloaded to .schema file instead), it fundementally functions differently than regular msgpack.
	The specification can be found at the below link, but I'm told it may not be accurate to how Brickadia actually uses it now.
	https://gist.github.com/Zeblote/053d54cc820df3bccad57df676202895
	
	The Rust library for brdb files notes this discrepancy and its source code may have more insight on how it works under the hood.
	https://github.com/brickadia-community/brdb/
	"""
	
	# ----------
	# Public methods
	# ----------

	def __init__(self):
		self._enums = {}
		self._structs: PropertyType = {}
		self.logger = logging.getLogger('MPS')
		# temporary
		logging.basicConfig(level=logging.DEBUG)
	
	def import_schema(self, schema_data: bytes):
		"""Imports the contents of a .schema file (`schema_data` as bytes) and adds the Enums and Structs to this object's registry."""
		
		dumped = msgpack.unpackb(schema_data)
		assert type(dumped) is list, f'Schema must have an array/list as the root'
		assert len(dumped) == 2, f'Schema root map must have 2 children (enums and structs), but has {len(dumped)} instead.'
		assert type(dumped[0]) is dict, f'Schema enums section must be a map/dict, but it\'s {type(dumped[0])} instead.'
		assert type(dumped[1]) is dict, f'Schema structs section must be a map/dict, but it\'s {type(dumped[1])} instead.'

		return self.import_schema_raw(dumped[0], dumped[1])
	
	def import_schema_raw(self, enums: dict[str, any], structs: dict[str, any]):
		"""Imports the schema, as pure dictionaries of enums and structs, and registers them to this object."""
		for enum_name in enums:
			enum_value = enums[enum_name]
			self._register_enum(enum_name, enum_value)

		for struct_name in structs:
			struct_contents = structs[struct_name]
			self._register_struct(struct_name, struct_contents)
	
	def unpack(self, file_like, root_struct_name: str = None):
		"""Parses a .mps file in the `file_like` object that supports .read(n) where n is number of bytes.

		`root_struct_name` is the name of the registered Struct to treat as the "root" of the .mps file. If omitted, this will default to the most recently registered occurrence of a Struct with name ending in "SoA" (structure of arrays)
		"""

		if root_struct_name is not None:
			assert root_struct_name in self._structs, f'root struct \'{root_struct_name}\' not registered'
			root_struct = self._structs[root_struct_names]
		else:
			keys = list(self._structs.keys())
			keys.reverse()
			root_struct = None
			for struct_name in keys:
				if struct_name.endswith('SoA'):
					root_struct = self._structs[struct_name]
					break
			assert root_struct is not None, f'could not find a root struct registered with a name ending in \'SoA\''
		self.logger.debug(f'begin unpacking with root struct \'{root_struct_name}\'')
		self._reader = MPLReader(file_like)
		self._file_like = file_like
		reader = self._reader
		
		self._queue = [] # (container, container_child_key: any = None, property_type: PropertyType, is_key: bool, is_value: bool)
		self._cached_key = None # cached key str, for Values being interpreted as keyvalue pairs
		queue = self._queue
		"""
			The queue is a list of struct properties or array/maps to read next.
			`container` is the pointer to a list or dict for where to store the resulting data after unpacking it
			`container_child_key` is the index of WHERE in the container to put the data. if None, treats container like a list and just appends.
			`property_type` is the PropertyType of the value to expect to read
			`is_key` (for Value types only) tells the Value unpacker that it should store the unpacked Value as a key for the next item that has is_value set. container and container_child_key are both ignored.
			`is_value` (for Value types only) tells the Value unpacker to recall the most recent Value that used is_key, and treat is as the container_child_key for the given container
			
		"""

		tree = {}
		
		# add the root of the struct to the queue
		# for property_name in root_struct:
		# 	property_type = root_struct[property_name]
		# 	queue.append((tree, property_name, property_type))

		self._enqueue_struct(tree, root_struct)

		while len(queue) > 0:
			container, container_child_key, property_type, is_key, is_value = queue.pop(0)
			pointer = hex(file_like.tell())
			match property_type:
				case Value():
					self._unpack_value(container, container_child_key, property_type, is_key, is_value)

				case Array():
					self._unpack_array(container, container_child_key, property_type)

				case Map():
					self._unpack_map(container, container_child_key, property_type)

				case _:
					raise ValueError(f'unknown queued property type \'{property_type}\'')
		return tree

	
	def pack(self, file_like, root_struct=None):
		"""Outputs a .mps file to the `file_like` object that supports .write(x: bytes) method.
		`root_struct` is the name of the registered Struct to treat as the "root" of the .mps file. If omitted, this will default to the most recently registered occurrence of a Struct with name ending in "SoA" (structure of arrays)
		"""
		raise NotImplemented

	# ----------
	# Unpack helpers
	# ----------

	def _enqueue_struct(self, container, struct):
		self.logger.debug(f'_enqueue_struct:')
		queue_next_index = 0
		for property_name in struct:
			property_type = struct[property_name]
			self.logger.debug(f'> added \'{property_name}\': {property_type}')
			self._queue.insert(queue_next_index, (container, property_name, property_type, False, False))
			queue_next_index += 1

	def _unpack_value(self, container, container_child_key, property_type, is_key, is_value):
		pointer = hex(self._file_like.tell())
		reader = self._reader
		queue = self._queue

		if is_key:
			self.logger.debug('> is key')
		if is_value:
			container_child_key = self._cached_key
			self.logger.debug(f'> is value, for cached key \'{container_child_key}\'')
			assert container_child_key is not None, f'reading a Value with is_value=True, but no key was ever read beforehand!'

		self.logger.debug(f'_unpack_value with type {property_type} -> {container_child_key}')
		self.logger.debug(f'ptr: {pointer}')
		value_type = property_type.type
		domain = self._get_domain_of_type(value_type)
		self.logger.debug(f'> domain: {domain}')
		match domain:
			case 'builtin':
				"""tree[member_name] = 1"""
				result_type, values = reader.read_next()
				self.logger.debug(f'> read {result_type} with values: {values})')
				assert result_type in VALID_TYPES[value_type], f'expected to read a compatible \'{value_type}\' Tag at {pointer}, but got Tag type \'{result_type}\' instead'
				# not an array

				result_value = values[0]
				if value_type == 'str':
					# result value is len of string
					result_bytes = self._file_like.read(result_value)
					assert (result_bytes is not None) and (len(result_bytes) == result_value), f'unexpected EOF while reading a string of {result_value} bytes'
					result_value = result_bytes.decode('utf-8')

				if is_key:
					self._cached_key = result_value
				elif container_child_key is None:
					container.append(result_value)
				else:
					container[container_child_key] = result_value

			case 'enum':
				"""tree[member_name] = 'Enum.MY_ENUM_VALUE'"""
				result_type, values = reader.read_next()
				self.logger.debug(f'> read {result_type} with values: {values})')
				enum = self._enums[value_type]
				enum_first_key = next(iter(enum))
				enum_first_value = enum[enum_first_key]
				result_py_type = TAG_PY_TYPES[result_type]
				self.logger.debug(f'> enum_first_value type: {type(enum_first_value)}')
				self.logger.debug(f'> result_py_type: {result_py_type}')
				
				assert type(enum_first_value) is result_py_type, f'expected to read a \'{type(enum_first_value)}\' for enum \'{value_type}\' at {pointer}, got a \'{result_py_type}\' instead (via Tag \'{result_type}\''

				# should i just be saving the raw value in the result tree instead? or is it fine to just resolve the name of the enum as a str?
				# TODO may need to import as raw values, depending on whether enums are used as bitflags
				enumeration_name = self._lookup_enum(enum, values[0])
				self.logger.debug(f'> resolved enum name \'{enumeration_name}\')')
				assert enumeration_name is not None, f'could not find associated enum in {value_type} for value {values[0]} at {pointer}. If it is supposed to be a flag of different enum values, I unfortunately haven\'t implemented that yet.'
				if is_key:
					self._cached_key = enumeration_name 
				elif container_child_key is None:
					container.append(enumeration_name)
				else:
					container[container_child_key] = enumeration_name

				
			case 'struct':
				"""tree[member_name] = {}"""
				struct = self._structs[value_type]
				child = {}

				if is_key:
					raise ValueError(f'attempt to read struct {value_type} as a key for a dictionary, but that functionality is not yet implemented due to hashing of structs. sorry..')
				if container_child_key is None:
					container.append(child)
				else:
					container[container_child_key] = child
				self.logger.debug(f'> enqueuing struct {value_type}')
				self._enqueue_struct(child, struct)
	
	def _unpack_array(self, container, container_child_key, property_type):
		pointer = hex(self._file_like.tell())
		reader = self._reader
		queue = self._queue

		value_type = property_type.type
		is_flat = property_type.is_flat
		self.logger.debug(f'_unpack_array with type {property_type} -> {container_child_key}')
		self.logger.debug(f'ptr: {pointer}')
		self.logger.debug(f'> is_flat={is_flat}')
		if is_flat:
			self._unpack_flat_array(container, container_child_key, property_type)
			return
		
		# read size first
		array_tag_type, array_tag_values = reader.read_next()
		self.logger.debug(f'> array header Tag type {array_tag_type} with values {array_tag_values}')
		assert TAG_PY_TYPES[array_tag_type] == list, f'expected to read a list at {pointer}, but got \'{TAG_PY_TYPES[array_tag_type]}\' instead (via Tag \'{array_tag_type}\')'
		array_count = array_tag_values[0]

		child = []
		if container_child_key is None:
			container.append(child)
		else:
			container[container_child_key] = child
		
		# value reading is already implemented, so just add those as tags to read next
		array_item_type = Value(value_type)
		self.logger.debug(f'> enqueuing {array_count} list items')
		for i in range(array_count):
			queue.insert(i, (child, None, array_item_type, False, False))

	def _unpack_flat_array(self, container, container_child_key, property_type: Array):
		pointer = hex(self._file_like.tell())
		reader = self._reader
		
		item_type = property_type.type
		
		self.logger.debug(f'_unpack_flat_array with type {item_type}')
		self.logger.debug(f'ptr: {pointer}')
		fmt = self._get_flat_fmt(item_type)
		self.logger.debug(f'> fmt string: \'{fmt}\'')

		bin_type, bin_values = reader.read_next()
		self.logger.debug(f'> flat array header Tag type {bin_type} with values {bin_values}')
		assert TAG_PY_TYPES[bin_type] == bytes, f'expected to read bytes at {pointer}, but got \'{TAG_PY_TYPES[bin_type]}\' instead (via Tag \'{bin_type}\')'

		bin_size = bin_values[0]
		
		stride = calcsize(fmt)
		self.logger.debug(f'> bin_size: {bin_size}')
		self.logger.debug(f'> stride:   {stride}')
		assert bin_size % stride == 0, f'byte array at {pointer} has size of {bin_size} bytes and underlying type \'{item_type}\' with stride of {stride}, but size is not evenly divided by stride to get an integer number of elements (got {bin_size/stride} instead)'

		count = bin_size // stride
		self.logger.debug(f'> reading {count} flat array items')

		the_array = []
		for _ in range(count):
			raw = self._file_like.read(stride)
			assert raw is not None and len(raw) == stride, f'unexpected EOF while reading flat array element {_}'
			data = unpack(fmt, raw)
			self.logger.debug(f'> > {_}: data is {data}')
			match self._get_domain_of_type(item_type):
				case 'builtin':
					self.logger.debug(f'> > {_}: builtin; appending {data[0]}')
					the_array.append(data[0])

				case 'enum':
					self.logger.debug(f'> > {_}: enum; appending {data[0]}')
					the_array.append(data[0])

				case 'struct':
					self.logger.debug(f'> > {_}: struct; getting list of keys')
					struct = self._structs[item_type]
					struct_keys = list(struct.keys())
					keys_values = zip(struct_keys, data, strict=True)
					child = {key: value for key, value in keys_values}

					the_array.append(child)
		if container_child_key is None:
			container.append(the_array)
		else:
			container[container_child_key] = the_array
		
	
	def _unpack_map(self, container, container_child_key, property_type):
		pointer = hex(self._file_like.tell())
		reader = self._reader
		queue = self._queue

		key_type = property_type.key_type
		value_type = property_type.value_type
		self.logger.debug(f'_unpack_map with key: {key_type} and value: {value_type} -> {container_child_key}')
		self.logger.debug(f'ptr: {pointer}')

		map_tag_type, map_tag_values = reader.read_next()
		self.logger.debug(f'> map header Tag type {map_tag_type} with values {map_tag_values}')
		assert TAG_PY_TYPES[map_tag_type] == dict, f'expected to read a dict at {pointer}, but got \'{TAG_PY_TYPES[map_tag_type]}\' instead (via Tag \'{map_tag_type}\')'

		dict_count = map_tag_values[0]

		child = {}
		if container_child_key is None:
			container.append(child)
		else:
			container[container_child_key] = child

		dict_key_type = Value(key_type)
		dict_value_type = Value(value_type)
		self.logger.debug(f'> enqueuing {dict_count} dict items')
		for i in range(dict_count):
			queue.insert(i*2, (None, None, dict_key_type, True, False)) # treat the Value as a dict key, save for later
			queue.insert(i*2 + 1, (child, None, dict_value_type, False, True)) # treav Value as a dict value, use saved key

	# ----------
	# Schema operations
	# ----------
	
	def _register_enum(self, name: str, values: dict):
		if name in self._enums:
			raise DuplicateError(f'enum \'{name}\' has already been registered')

		if len(values) == 0:
			raise ValueError(f'attempt to create enum with no values')
		established_type = None
		used_values = []
		for value_key in values:
			value = values[value_key]
			if value in used_values:
				raise ValueError(f'enum {name}.{value_key} already has value {value} in use')
			used_values.append(value)

			value_type = type(value)
			if value_type not in VALID_ENUM_TYPES:
				raise TypeError(f'enums can only contain one of {repr(VALID_ENUM_TYPES)} but got \'{value_type}\' instead')
			if established_type == None:
				established_type = value_type
			elif established_type != value_type:
				raise TypeError(f'enum \'{name}\' is established to have values of type \'{established_type}\' but tried to register value {repr(value)} of type \'{value_type}\'')

		self._enums[name] = values
	
	def _lookup_enum(self, enum: dict, value: any) -> any:
		"""Looks up a value inside the enum dict and returns the key for the enum"""
		for key in enum:
			if enum[key] == value:
				return key
		return None
	
	def _check_type(self, typename: str):
		return typename in self._enums or \
			typename in self._structs or \
			typename in VALID_TYPES

	def _get_domain_of_type(self, typename: str) -> str:
		"""Given the typename, checks if it's a builtin type (see VALID_TYPES), a registered enum, or a registered struct.
		Returns 'builtin', 'enum', 'struct', or None if no type is found."""
		if typename in VALID_TYPES:
			return 'builtin'
		elif typename in self._enums:
			return 'enum'
		elif typename in self._structs:
			return 'struct'
		return None
	
	FLAT_LOOKUP = {
		# Zeblote said flat arrays should be like a C struct, which i had assumed to mean aligning everything to 4 byte offsets
		# This doesn't seem to be the case, as I've encountered a case in ChunksShared.schema where an i16 is given 2 bytes instead of rounding up to 4
		# I will assume it doesn't have to align to 4 bytes.
		'bool': '?',
		'u8': 'B',
		'u16': 'H',
		'u32': 'I',
		'u64': 'Q',

		'i8': 'b',
		'i16': 'h',
		'i32': 'i',
		'i64': 'q',

		'f32': 'f',
		'f64': 'd',

		# str not allowed, it's an array of bytes and can be any size
		# there are no fixed size strs in structs at this time

		'object': 'i',
		'class': 'i'
	}
	def _get_flat_fmt(self, typename: str, _shallow: bool = False) -> str:
		"""Given the typename, constructs a format string to use in struct.unpack or struct.pack.
		This is only for usage in parsing/writing flat arrays. It should not be used for determining the smallest msgpack Tag to pack a type into.
		Can be a builtin type, an enum, or a struct"""
		assert self._check_type(typename), f'attempt to get flat format of unknown or unregistered type \'{typename}\''
		domain = self._get_domain_of_type(typename)

		match domain:
			case 'builtin':
				assert typename in self.FLAT_LOOKUP, f'type \'{typename}\' is not valid for a flat array'
				if _shallow:
					return self.FLAT_LOOKUP[typename]
				else:
					return '<' + self.FLAT_LOOKUP[typename]

			case 'enum':
				return 'Q' if _shallow else '<Q' # u64

			case 'struct':
				assert not _shallow, f'reading a nested struct inside flat array is not allowed'
				struct = self._structs[typename]
				fmt = '<'
				for property_key in struct:
					property_value = struct[property_key]
					assert type(property_value) is Value, f'can only get flat array format for flat structs; found nested {property_value}'
					fmt += self._get_flat_fmt(property_value.type, True)
				return fmt
			case _:
				raise ValueError(f'unknown domain {domain} for {typename}')
					


	def _register_struct(self, name: str, contents: dict):
		if name in self._structs:
			raise DuplicateError(f'struct \'{name}\' has already been registered')
		if len(contents) == 0:
			raise ValueError(f'attempt to create struct with no properties')

		s = {} # final result to actually place in registry
		for property_name in contents:
			property_type = contents[property_name]
			match property_type:
				case str():
					# single value
					assert self._check_type(property_type), f'struct {name}.{property_name}: unknown or unregistered identifier \'{property_type}\''
					s[property_name] = Value(property_type)

				case list():
					# list/array. expect [str] or [str, None]
					assert len(property_type) == 1 or len(property_type) == 2, f'struct {name}.{property_name}: unexpected size of array (expected size of 1 or 2)'
					assert type(property_type[0]) is str, f'struct {name}.{property_name}: 1st item in array must be a str, got \'{type(property_type[0])}\''
					assert self._check_type(property_type[0]), f'struct {name}.{property_name}: unknown or unregistered identifier \'{property_type[0]}\''
					if len(property_type) == 2:
						assert property_type[1] is None, f'struct {name}.{property_name}: optional 2nd item in array must be None, got \'{type(property_type[1])}\''
					s[property_name] = Array(property_type[0])
					if len(property_type) == 2:
						s[property_name].is_flat = True

				case dict():
					# dict/map. expect {str: str}
					assert len(property_type) == 1, f'struct {name}.{property_name}: dict must only have 1 item, got \'{len(property_type)}\' instead'
					first_key = next(iter(property_type))
					assert type(first_key) is str, f'struct {name}.{property_name}: item in dict must have key type of str, got \'{type(first_key)}\''
					assert self._check_type(first_key), f'struct {name}.{property_name}: unknown or registered identifier for item key \'{first_key}\''
					assert (first_key not in ('object', 'class')) and (first_key not in self._structs), f'struct {name}.{property_name}: key type \'{first_key}\' hashing is not yet implemented, so it cannot be used as a key at this time.'
					first_value = property_type[first_key]
					assert type(first_value) is str, f'struct {name}.{property_name}: item in dict must have value type of str, got \'{type(first_value)}\''
					assert self._check_type(first_value), f'struct {name}.{property_name}: unknown or registered identifier for item value \'{first_value}\''
					
					s[property_name] = Map(first_key, first_value)

				case _:
					raise RegistrationError(f'struct \'{name}\' unexpected property value of type \'{type(property_type)}\' (expected str, list, or dict)')
			print(f'struct {name}.{property_name} registered')
		self._structs[name] = s
		print(f'struct {name} registered')


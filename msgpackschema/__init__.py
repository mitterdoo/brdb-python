import msgpack
from .errors import *
from .msgpack_lite import MPLReader

VALID_TYPES = ('bool', 'u8', 'u16', 'u32', 'u64', 'i8', 'i16', 'i32', 'i64', 'f32', 'f64', 'str', 'object', 'class')
VALID_ENUM_TYPES = (bool, int, float)

class PropertyType:
	pass

class PropertyTypeValue(PropertyType):
	def __init__(self, the_type: str):
		self.type = the_type
	
	def __repr__(self):
		return f'PropertyTypeValue(\'{self.type}\')'

class PropertyTypeArray(PropertyType):
	def __init__(self, item_type: str, is_flat=False):
		self.type = item_type
		self.is_flat = is_flat
	
	def __repr__(self):
		if self.is_flat:
			return f'PropertyTypeArray(\'{self.type}\', is_flat=True)'
		else:
			return f'PropertyTypeArray(\'{self.type}\')'

class PropertyTypeMap(PropertyType):
	def __init__(self, key_type: str, value_type: str):
		self.key_type = key_type
		self.value_type = value_type
	
	def __repr__(self):
		return f'PropertyTypeMap(\'{self.key_type}\', \'{self.value_type}\')'
		

class MPS:
	"""A msgpack-schema class for reading and writing .mps files for Brickadia.
	It uses msgpack types under the hood, but since no data is needed for making key-value pairs in the .mps fille (offloaded to .schema file instead), it fundementally functions differently than regular msgpack.
	The specification can be found at the below link, but I'm told it may not be accurate to how Brickadia actually uses it now.
	https://gist.github.com/Zeblote/053d54cc820df3bccad57df676202895
	
	The Rust library for brdb files notes this discrepancy and its source code may have more insight on how it works under the hood.
	https://github.com/brickadia-community/brdb/
	"""

	def __init__(self):
		self._enums = {}
		self._structs: PropertyType = {}
	
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
	
	def unpack(self, file_like, root_struct=None):
		"""Parses a .mps file in the `file_like` object that supports .read(n) where n is number of bytes.

		`root_struct` is the name of the registered Struct to treat as the "root" of the .mps file. If omitted, this will default to the most recently registered occurrence of a Struct with name ending in "SoA" (structure of arrays)
		"""
		raise NotImplemented

		# construct a "resolved tree structure" from our root Struct, so it has all of the sub-structs and enums inside.
		# we'd only use this for reading/writing in this case, since it's possible for the user to pick a different root
	
	def pack(self, file_like, root_struct=None):
		"""Outputs a .mps file to the `file_like` object that supports .write(x: bytes) method.
		`root_struct` is the name of the registered Struct to treat as the "root" of the .mps file. If omitted, this will default to the most recently registered occurrence of a Struct with name ending in "SoA" (structure of arrays)
		"""
		raise NotImplemented

	
	def _register_enum(self, name: str, values: dict):
		if name in self._enums:
			raise DuplicateError(f'enum \'{name}\' has already been registered')

		if len(values) == 0:
			raise ValueError(f'attempt to create enum with no values')
		established_type = None
		for value_key in values:
			value = values[value_key]
			value_type = type(value)
			if value_type not in VALID_ENUM_TYPES:
				raise TypeError(f'enums can only contain one of {repr(VALID_ENUM_TYPES)} but got \'{value_type}\' instead')
			if established_type == None:
				established_type = value_type
			elif established_type != value_type:
				raise TypeError(f'enum \'{name}\' is established to have values of type \'{established_type}\' but tried to register value {repr(value)} of type \'{value_type}\'')

		self._enums[name] = values
	
	def _check_type(self, typename: str):
		return typename in self._enums or \
			typename in self._structs or \
			typename in VALID_TYPES
	 
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
					s[property_name] = PropertyTypeValue(property_type)

				case list():
					# list/array. expect [str] or [str, None]
					assert len(property_type) == 1 or len(property_type) == 2, f'struct {name}.{property_name}: unexpected size of array (expected size of 1 or 2)'
					assert type(property_type[0]) is str, f'struct {name}.{property_name}: 1st item in array must be a str, got \'{type(property_type[0])}\''
					assert self._check_type(property_type[0]), f'struct {name}.{property_name}: unknown or unregistered identifier \'{property_type[0]}\''
					if len(property_type) == 2:
						assert property_type[1] is None, f'struct {name}.{property_name}: optional 2nd item in array must be None, got \'{type(property_type[1])}\''
					s[property_name] = PropertyTypeArray(property_type[0])
					if len(property_type) == 2:
						s[property_name].is_flat = True

				case dict():
					# dict/map. expect {str: str}
					assert len(property_type) == 1, f'struct {name}.{property_name}: dict must only have 1 item, got \'{len(property_type)}\' instead'
					first_key = next(iter(property_type))
					assert type(first_key) is str, f'struct {name}.{property_name}: item in dict must have key type of str, got \'{type(first_key)}\''
					assert self._check_type(first_key), f'struct {name}.{property_name}: unknown or registered identifier for item key \'{first_key}\''
					first_value = property_type[first_key]
					assert type(first_value) is str, f'struct {name}.{property_name}: item in dict must have value type of str, got \'{type(first_value)}\''
					assert self._check_type(first_value), f'struct {name}.{property_name}: unknown or registered identifier for item value \'{first_value}\''
					
					s[property_name] = PropertyTypeMap(first_key, first_value)

				case _:
					raise RegistrationError(f'struct \'{name}\' unexpected property value of type \'{type(property_type)}\' (expected str, list, or dict)')
			print(f'struct {name}.{property_name} registered')
		self._structs[name] = s
		print(f'struct {name} registered')


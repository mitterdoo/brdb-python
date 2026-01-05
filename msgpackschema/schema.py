import msgpack
from .errors import *
from .values import *

VALID_TYPES = {
	'bool',
	'u8',
	'u16',
	'u32',
	'u64',

	'i8',
	'i16',
	'i32',
	'i64',

	'f32',
	'f64',

	'str',

	'object',
	'class',

	'wire_graph_variant',
	'wire_graph_prim_math_variant'
}
VALID_ENUM_TYPES = (bool, int)

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

def lookup_enum(enum: dict, value: any) -> any:
	"""Looks up a value inside the enum dict and returns the key for the enum"""
	for key in enum:
		if enum[key] == value:
			return key
	return None

class Schema:
	def __init__(self):
		self._enums = {}
		self._structs = {}
		self.external_asset_references = [] # important tool that will help us later?
	
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
			
	def get_enum(self, enum_name: str):
		if enum_name in self._enums:
			return self._enums[enum_name]
		else:
			return None
	
	def get_struct(self, struct_name: str):
		if struct_name in self._structs:
			return self._structs[struct_name]
		else:
			return None	
	
	def get_default_struct(self) -> str:
		"""Returns the name of the default struct to use for reading a MPS file.
		For Brickadia, this is the most recent occurrence of a struct in the schema ending in "SoA"
		"""
		keys = list(self._structs.keys())
		keys.reverse()
		for key in keys:
			if key.endswith('SoA'):
				return key
		return None

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
	
	def has_named_type(self, typename: str):
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
	

	def get_flat_fmt(self, typename: str, _shallow: bool = False) -> str:
		"""Given the typename, constructs a format string to use in struct.unpack or struct.pack.
		This is only for usage in parsing/writing flat arrays. It should not be used for determining the smallest msgpack Tag to pack a type into.
		Can be a builtin type, an enum, or a struct"""
		assert self.has_named_type(typename), f'attempt to get flat format of unknown or unregistered type \'{typename}\''
		domain = self._get_domain_of_type(typename)

		match domain:
			case 'builtin':
				assert typename in FLAT_LOOKUP, f'type \'{typename}\' is not valid for a flat array'
				if _shallow:
					return FLAT_LOOKUP[typename]
				else:
					return '<' + FLAT_LOOKUP[typename]

			case 'enum':
				return 'Q' if _shallow else '<Q' # u64

			case 'struct':
				assert not _shallow, f'reading a nested struct inside flat array is not allowed'
				struct = self._structs[typename]
				fmt = '<'
				for property_key in struct:
					property_value = struct[property_key]
					assert type(property_value) is Value, f'can only get flat array format for flat structs; found nested {property_value}'
					fmt += self.get_flat_fmt(property_value.type, True)
				return fmt
			case _:
				raise ValueError(f'unknown domain {domain} for {typename}')
					
	def _register_struct(self, name: str, contents: dict):
		if name in self._structs:
			raise DuplicateError(f'struct \'{name}\' has already been registered')
		if len(contents) == 0:
			# raise ValueError(f'attempt to create struct with no properties')
			# Zeb's gist said that a struct must have one or more properties. yet, as of 3 Jan 2026, a prefab can be exported with a ComponentsShared.schema with an empty 'BrickComponentData_Rerouter' struct. oh well
			pass

		s = {} # final result to actually place in registry
		for property_name in contents:
			property_type = contents[property_name]
			match property_type:
				case str():
					# single value
					assert self.has_named_type(property_type), f'struct {name}.{property_name}: unknown or unregistered identifier \'{property_type}\''
					s[property_name] = Value(property_type)

				case list():
					# list/array. expect [str] or [str, None]
					assert len(property_type) == 1 or len(property_type) == 2, f'struct {name}.{property_name}: unexpected size of array (expected size of 1 or 2)'
					assert type(property_type[0]) is str, f'struct {name}.{property_name}: 1st item in array must be a str, got \'{type(property_type[0])}\''
					assert self.has_named_type(property_type[0]), f'struct {name}.{property_name}: unknown or unregistered identifier \'{property_type[0]}\''
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
					assert self.has_named_type(first_key), f'struct {name}.{property_name}: unknown or registered identifier for item key \'{first_key}\''
					assert (first_key not in ('object', 'class')) and (first_key not in self._structs), f'struct {name}.{property_name}: key type \'{first_key}\' hashing is not yet implemented, so it cannot be used as a key at this time.'
					first_value = property_type[first_key]
					assert type(first_value) is str, f'struct {name}.{property_name}: item in dict must have value type of str, got \'{type(first_value)}\''
					assert self.has_named_type(first_value), f'struct {name}.{property_name}: unknown or registered identifier for item value \'{first_value}\''
					
					s[property_name] = Map(first_key, first_value)

				case _:
					raise RegistrationError(f'struct \'{name}\' unexpected property value of type \'{type(property_type)}\' (expected str, list, or dict)')
		self._structs[name] = s


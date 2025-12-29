import msgpack

VALID_TYPES = ('bool', 'u8', 'u16', 'u32', 'u64', 'i8', 'i16', 'i32', 'i64', 'f32', 'f64', 'str', 'object', 'class')

class MPS:
	"""A msgpack-schema class for reading and writing .mps files for Brickadia.
	It uses msgpack types under the hood, but since no data is needed for making key-value pairs in the .mps fille (offloaded to .schema file instead), it fundementally functions differently than regular msgpack.
	The specification can be found at the below link, but I'm told it may not be accurate to how Brickadia actually uses it now.
	https://gist.github.com/Zeblote/053d54cc820df3bccad57df676202895
	
	The Rust library for brdb files notes this discrepancy and its source code may have more insight on how it works under the hood.
	https://github.com/brickadia-community/brdb/
	"""

	def __init__(self):
		self.enums = {}
		self.structs = {}
	
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
			self._import_enum(enum_name, enum_value)

		for struct_name in structs:
			struct_contents = structs[struct_name]
			self._import_struct(struct_name, struct_contents)
	
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

	
	def _import_enum(self, name: str, value):
		raise NotImplemented
	
	def _import_struct(self, name: str, contents):
		raise NotImplemented

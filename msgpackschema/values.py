from enum import IntEnum, Enum

class WireVariantType(IntEnum):
	NUMBER = 0
	INT = 1
	BOOL = 2
	OBJECT = 3
	EXEC = 4

class WireVariant:
	def __init__(self, typ: WireVariantType, value: any = None):
		self.type = typ
		self.value = value

BrdbValue = None # forward declaration

class BrdbEnum:
	def __init__(self, schema, name, value: int):
		self.schema = schema
		self.name = name
		self.value = value
	
	def __repr__(self):
		return f'{self.name} ({self.value})'

class BrdbValueType(Enum):
	Nil = 0
	Bool = 1
	U8 = 2
	U16 = 3
	U32 = 4
	U64 = 5
	I8 = 6
	I16 = 7
	I32 = 8
	I64 = 9
	F32 = 10
	F64 = 11
	String = 12
	Asset = 13
	Enum = 14
	Struct = 15
	Array = 16
	FlatArray = 17
	Map = 18
	WireVar = 19

class BrdbValue:
	def __init__(self, typ: BrdbValueType, value: any = None):
		self.type = typ
		self.value = value
	
	def __repr__(self):
		return f'BrdbValue({self.type}, value={self.value})'
	
	def bake(self):
		"""Constructs a dict/list structure that reflects this value, including any nested values (if this value is a struct, array, or map)"""
		match self.type:
			case BrdbValueType.Array | BrdbValueType.FlatArray:
				return [x.bake() for x in self.value]
			case BrdbValueType.Map | BrdbValueType.Struct:
				return {key: value.bake() for key, value in zip(self.value.keys(), self.value.values())}
			case _:
				return self.value
	
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




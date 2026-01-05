from .errors import *
from .mp import Marker, read_marker, read_next
from .values import *
from struct import unpack, calcsize
import logging


_log = logging.getLogger(__name__)

# No typehinting for the Schema type
# Otherwise, importing it would create a circular import dependency

# Instead, pass the Schema and file buffer as references

# Most of this functionality was transcoded from https://github.com/brickadia-community/brdb/blob/main/crates/brdb/src/schema/read.rs
# Many thanks to Cake/Meshiest/Isaac for their work on that

def read_type(schema, buf, typ: str) -> BrdbValue:
	"""Read the specified type (builtin/enum/struct) from the buffer using the Schema provided"""
	_log.debug(f'read.read_type at {hex(buf.tell())} with type \'{typ}\'')
	match typ:
		case 'bool':
			return BrdbValue(BrdbValueType.Bool, _read_bool(buf))
		case 'u8':
			return BrdbValue(BrdbValueType.U8, _read_uint(buf))
		case 'u16':
			return BrdbValue(BrdbValueType.U16, _read_uint(buf))
		case 'u32':
			return BrdbValue(BrdbValueType.U32, _read_uint(buf))
		case 'u64':
			return BrdbValue(BrdbValueType.U64, _read_uint(buf))

		case 'i8':
			return BrdbValue(BrdbValueType.I8, _read_int(buf))
		case 'i16':
			return BrdbValue(BrdbValueType.I16, _read_int(buf))
		case 'i32':
			return BrdbValue(BrdbValueType.I32, _read_int(buf))
		case 'i64':
			return BrdbValue(BrdbValueType.I64, _read_int(buf))

		case 'str':
			return BrdbValue(BrdbValueType.String, _read_str(buf))

		case 'wire_graph_variant':
			val = _read_uint(buf)
			match val:
				case 0:
					result = WireVariant(WireVariantType.NUMBER, _read_float64(buf))
				case 1:
					result = WireVariant(WireVariantType.INT, _read_int(buf))
				case 2:
					result = WireVariant(WireVariantType.BOOL, _read_bool(buf))
				case 3:
					result = WireVariant(WireVariantType.OBJECT, 'unknown') # from __future__ import the_rest_of_brickadias_planned_wire_types
				case 4:
					result = WireVariant(WireVariantType.EXEC)
				case _:
					raise UnknownWireVariantError(val)
			return BrdbValue(BrdbValueType.WireVar, result)
		case 'wire_graph_prim_math_variant':
			val = _read_uint(buf)
			match val:
				case 0:
					result = WireVariant(WireVariantType.NUMBER, _read_float64(buf))
				case 1:
					result = WireVariant(WireVariantType.INT, _read_int(buf))
				case _:
					raise UnknownWireVariantError(val)
			return BrdbValue(BrdbValueType.WireVar, result)
		case 'class' | 'object':
			asset_id = _read_int(buf)
			if asset_id < 0:
				return BrdbValue(BrdbValueType.Asset, None)
			else:
				if len(schema.external_asset_references) <= asset_id:
					raise UnknownAssetError(typ, asset_id)
				return BrdbValue(BrdbValueType.Asset, asset_id)
		
		case _:
			if schema.has_named_type(typ):
				return _read_named_type(schema, buf, typ)
			else:
				raise UnknownTypeError(typ)

def flat_type_size(schema, typ: str) -> int:
	"""Gets the number of bytes for a flat type in the schema"""
	match typ:
		case 'u8': return 1
		case 'u16': return 2
		case 'u32': return 4
		case 'u64': return 8

		case 'i8': return 1
		case 'i16': return 2
		case 'i32': return 4
		case 'i64': return 8
		
		case 'f32': return 4
		case 'f64': return 8

		case _:
			if enum := schema.get_enum(typ):
				return 8 # u64
			elif struct := schema.get_struct(typ):
				size = 0
				for key in struct:
					value = struct[key]
					if type(value) is Value:
						size += flat_type_size(schema, value.type)
				return size
			else:
				return 0

def _read_named_type(schema, buf, typ: str) -> BrdbValue:
	"""Reads the type that's been registered in the Schema. basically just chooses whether to read an enum or a struct."""
	if struct := schema.get_struct(typ):
		return _read_struct(schema, buf, typ, struct)
	elif enum := schema.get_enum(typ):
		return _read_enum(schema, buf, typ, enum)
	else:
		raise UnknownTypeError(typ)

def _read_struct(schema, buf, typ: str, struct) -> BrdbValue:
	properties = {}
	for prop_name in struct:
		prop_type: PropertyType = struct[prop_name]
		properties[prop_name] = _read_struct_property(schema, buf, prop_type)
	return BrdbValue(BrdbValueType.Struct, properties)

def _read_struct_property(schema, buf, typ: PropertyType) -> BrdbValue:
	match typ:
		case Value():
			return read_type(schema, buf, typ.type)
		case Array():
			if not typ.is_flat:
				values = []
				_t, array_len = read_next(buf, 'fixarray', 'array16', 'array32')
				array_len = array_len[0]
				for _ in range(array_len):
					values.append(read_type(schema, buf, typ.type))
				return BrdbValue(BrdbValueType.Array, values)
			else:
				_t, bin_size = read_next(buf, 'bin8', 'bin16', 'bin32')
				bin_size = bin_size[0]
				stride = flat_type_size(schema, typ.type)
				
				assert bin_size % stride == 0, f'byte array has size of {bin_size} bytes and underlying type \'{typ.type}\' with stride of {stride}, but size is not evenly divided by stride to get an integer number of elements (got {bin_size/stride} instead)'

				count = bin_size // stride

				the_array = []
				for _ in range(count):
					the_array.append(_read_flat_type(schema, buf, typ.type))

				return BrdbValue(BrdbValueType.FlatArray, the_array)
				
		case Map():
			container = {}
			map_len = _read_uint(buf)
			for _ in range(map_len):
				key = _read_named_type(schema, buf, typ.key_type)
				value = _read_named_type(schema, buf, typ.value_type)
				container[key] = value
			return BrdbValue(BrdbValueType.Map, container)

def _read_enum(schema, buf, typ: str, enum) -> BrdbValue:
	value = _read_uint(buf)
	key = schema.lookup_enum(enum, value)

	if key is None:
		raise EnumValueError(enum, value)
	
	return BrdbValue(BrdbValueType.Enum, BrdbEnum(schema, key, value))

def _read_bool(buf) -> bool:
	_, values = read_next(buf, 'true', 'false')
	return values[0]

def _read_str(buf) -> str:
	_, str_len = read_next(buf, 'fixstr', 'str8', 'str16', 'str32')
	str_len = str_len[0]
	return buf.read(str_len).decode('utf-8')

def _read_int(buf) -> int:
	tag, values = read_next(buf, '+fixint', '-fixint', 'uint8', 'uint16', 'uint32', 'uint64', 'int8', 'int16', 'int32', 'int64')
	return values[0]

def _read_uint(buf) -> int:
	tag, values = read_next(buf, '+fixint', '-fixint', 'uint8', 'uint16', 'uint32', 'uint64')
	value = values[0]
	# thanks Cake for documenting this in your code. it probably would've drove me up the wall if i hadn't seen it
	if tag == '-fixint':
		# values 224 thru 225 use negative fixint numbers apparently
		return (256 + value)
	else:
		return value

def _read_float32(buf) -> float:
	_, values = read_next(buf, '+fixint', '-fixint', 'int8', 'int16', 'uint8', 'uint16', 'float32')
	return float(values[0])

def _read_float64(buf) -> float:
	_, values = read_next(buf, '+fixing', '-fixint', 'int8', 'int16', 'int32', 'uint8', 'uint16', 'uint32', 'float32', 'float64')
	return float(values[0])

def _read_exact(buf, count: int):
	value = buf.read(count)
	if len(value) != count:
		raise UnexpectedEOFError()
	return value

def _unpack(fmt: str, buf):
	return unpack(fmt, _read_exact(buf, calcsize(fmt)))

def _read_flat_type(schema, buf, typ: str) -> BrdbValue:
	"""Reads a flat type which is usually just a struct dumped from memory, and doesn't use msgpack Tags"""
	match typ:
		case 'u8':
			data = _unpack('<B', buf)
			return BrdbValue(BrdbValueType.U8, data[0])
		case 'u16':
			data = _unpack('<H', buf)
			return BrdbValue(BrdbValueType.U16, data[0])
		case 'u32':
			data = _unpack('<I', buf)
			return BrdbValue(BrdbValueType.U32, data[0])
		case 'u64':
			data = _unpack('<Q', buf)
			return BrdbValue(BrdbValueType.U64, data[0])

		case 'i8':
			data = _unpack('<b', buf)
			return BrdbValue(BrdbValueType.I8, data[0])
		case 'i16':
			data = _unpack('<h', buf)
			return BrdbValue(BrdbValueType.I16, data[0])
		case 'i32':
			data = _unpack('<i', buf)
			return BrdbValue(BrdbValueType.I32, data[0])
		case 'i64':
			data = _unpack('<q', buf)
			return BrdbValue(BrdbValueType.I64, data[0])

		case 'f32':
			data = _unpack('<f', buf)
			return BrdbValue(BrdbValueType.F32, data[0])
		case 'f64':
			data = _unpack('<d', buf)
			return BrdbValue(BrdbValueType.F64, data[0])

		case _:
			if struct := schema.get_struct(typ):
				return _read_flat_struct(schema, buf, typ, struct)
			else:
				raise InvalidFlatTypeError(typ)

def _read_flat_struct(schema, buf, typ: str, struct) -> BrdbValue:
	properties = {}
	for key in struct:
		value = struct[key]
		properties[key] = _read_flat_struct_property(schema, buf, value)
	
	return BrdbValue(BrdbValueType.Struct, properties)

def _read_flat_struct_property(schema, buf, typ: PropertyType) -> BrdbValue:
	match typ:
		case Value():
			return _read_flat_type(schema, buf, typ.type)
		case _:
			raise UnknownTypeError(f'flat {repr(typ)}')



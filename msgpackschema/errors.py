class SchemaError(Exception):
	pass

class RegistrationError(SchemaError):
	pass

class DuplicateError(RegistrationError):
	pass

class UnexpectedEOFError(SchemaError):
	def __init__(self):
		super().__init__('unexpected EOF')

class UnknownWireVariantError(SchemaError):
	def __init__(self, value):
		super().__init__(f'unexpected wire variant type {value}')

class UnknownAssetError(SchemaError):
	def __init__(self, typ: str, asset: int):
		super().__init__(f'unknown asset {asset} for {typ}')

class UnknownTypeError(SchemaError):
	def __init__(self, typ: str):
		super().__init__(f'unknown or unregistered type \'{typ}\'')

class InvalidFlatTypeError(SchemaError):
	def __init__(self, typ: str):
		super().__init__(f'invalid flat type \'{typ}\'')

class EnumValueError(SchemaError):
	def __init__(self, enum: str, val: int):
		super().__init__(f'value {val} is not a member of enum \'{enum}\'')


class BRZException(Exception):
	pass

class BRZFormatError(BRZException):
	pass

class BRZUnexpectedEOF(BRZException):
	pass

class BRZVersionError(BRZException):
	pass

class BRZDecompressionError(BRZException):
	pass

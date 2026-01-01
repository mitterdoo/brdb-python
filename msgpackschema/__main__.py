from . import MPS
from .msgpack_lite import MPLReader
from pprint import pp

if __name__ == '__main__':
	f = open('output/World/0/GlobalData.schema', 'rb')
	mps = MPS()
	mps.import_schema(f.read())
	f.close()
	pp(mps._enums)
	pp(mps._structs)

	f = open('output/World/0/GlobalData.mps', 'rb')
	x = MPLReader(f)
	print(x.read_next())


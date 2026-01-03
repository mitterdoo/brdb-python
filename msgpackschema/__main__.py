from . import MPS
from .msgpack_lite import MPLReader
from pprint import pp

if __name__ == '__main__':
	f = open('output_new/World/0/Bricks/ChunksShared.schema', 'rb')
	mps = MPS()
	mps.import_schema(f.read())
	f.close()
	pp(mps._enums)
	pp(mps._structs)

	f = open('output_new/World/0/Bricks/Grids/1/Chunks/0_0_0.mps', 'rb')
	
	tree = mps.unpack(f)
	f.close()


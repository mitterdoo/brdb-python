from . import MPS
from .msgpack_lite import MPLReader
from pprint import pp

if __name__ == '__main__':

	def read(schema_path, mps_path):
		f = open(schema_path, 'rb')
		mps = MPS()
		mps.import_schema(f.read())
		f.close()
		pp(mps._enums)
		pp(mps._structs)

		f = open(mps_path, 'rb')
		
		tree = mps.unpack(f)
		print('\n-----------------\n')
		pp(tree)
		f.close()

	read('output_new/World/0/Bricks/ChunksShared.schema', 'output_new/World/0/Bricks/Grids/1/Chunks/0_0_0.mps')


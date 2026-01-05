from .schema import Schema
from .read import read_type
from pprint import pp
import logging

if __name__ == '__main__':

	logging.basicConfig(level=logging.DEBUG)
	def read(schema_path, mps_path):
		f = open(schema_path, 'rb')
		schema = Schema()
		schema.import_schema(f.read())
		f.close()
		pp(schema._enums)
		pp(schema._structs)

		f = open(mps_path, 'rb')
		
		root = schema.get_default_struct()
		tree = read_type(schema, f, root)
		print('\n-----------------\n')
		pp(tree.bake())
		
		remaining = f.read()
		print(f'there are {len(remaining)} byte(s) leftover')
		f.close()
		return tree

	print('========== Chunks schema ==========')
	read('output_new/World/0/Bricks/ChunksShared.schema', 'output_new/World/0/Bricks/Grids/1/Chunks/0_0_0.mps')

	print('\n\n\n')
	print('========== Components schema ==========')
	read('output_new/World/0/Bricks/ComponentsShared.schema', 'output_new/World/0/Bricks/Grids/1/Components/0_0_0.mps')


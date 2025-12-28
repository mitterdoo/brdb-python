from . import BRZ
if __name__ == '__main__':
	brz = BRZ('assets/single brick.brz')
	print(brz.ls('/')) # should output ['Meta', 'World']

	# extract thumbnail
	with open('output/thumbnail.png', 'wb') as out_file:
		with brz.open('/Meta/Thumbnail.png', 'r') as in_file:
			out_file.write(in_file.read())
	

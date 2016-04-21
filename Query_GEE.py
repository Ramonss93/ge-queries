####################################
#
# Purpose: This script will query for data from Google Earth Engine by point
#
# Author: Gerardo Armendariz
#
# Modified: 3-1-2016
#
#
####################################

# Import the Earth Engine Python Package
import ee, os, csv, datetime, time

def main():
	# Initialize the Earth Engine object, using the authentication credentials.
	ee.Initialize()

	# Print the information for an image asset.
	#image = ee.Image('srtm90_v4')
	#print(image.getInfo())


	stations = {'USC00143218: KS GREAT BEND 3W':'38.3614,-98.8281',
	'USW00013962: TX ABILENE RGNL AP':'32.4106,-99.6822',
	'USW00012924: TX CORPUS CHRISTI':'27.7839,-97.5108',
	'USW00024089: WY CASPER NATRONA CO AP':'42.8975,-106.4636',
	'USW00093010: CO LIMON WSMO':'39.1894,-103.7158',
	'USW00024033: MT BILLINGS LOGAN INTL AP':'45.8069,-108.5422',
	'USC00261485: NV CARSON CITY':'39.1253,-119.7678',
	#'USC00267640: NV SNOWBALL RCH':'39.0403,-116.1989',
	'USC00425733: UT MOAB':'38.5744,-109.5458',
	'USW00023155: CA BAKERSFIELD AP':'35.4344,-119.0542',
	'USW00093193: CA FRESNO YOSEMITE INTL AP':'36.7800,-119.7194',
	'USC00047851: CA SAN LUIS OBISPO POLY':'35.3056,-120.6619',
	'USC00351862: OR CORVALLIS STATE UNIV':'44.6342,-123.1900',
	#'USC00355162: OR MALHEUR REFUGE HQ':'43.2650,-118.8447',
	#'USC00457319: WA SAPPHO 8 E':'48.0667,-124.1167',
	'USW00024157: WA SPOKANE INTL AP':'47.6217,-117.5281',
	'USW00014922: MN MINNEAPOLIS/ST PAUL AP':'44.8831,-93.2289',
	'USC00133632: IA HARLAN':'41.6453,-95.3339',
	'USC00033466: AR HOT SPRINGS 1 NNE':'34.5128,-93.0486',
	'USW00013976: LA LAFAYETTE RGNL AP':'30.2050,-91.9875',
	'USC00098703: GA TIFTON':'31.4461,-83.4767',
	'USC00440766: VA BLACKSBURG NWSO':'37.2039,-80.4144',
	'USW00014606: ME BANGOR INTL AP':'44.7978,-68.8186',
	#'USC00369408: PA WELLSBORO 4 SW':'41.7003,-77.3872',
	'USC00331890: OH COSHOCTON WPC PLT':'40.2403,-81.8711',
	'USC00403280: TN FRANKLIN SEWAGE PLT':'35.9417,-86.8686',
	'USC00080369: FL AVON PARK 2 W':'27.5947,-81.5267',
	'USW00024011: ND BISMARCK':'46.7708,-100.7603',
	'USW00024028: NE SCOTTSBLUFF HEILIG AP':'41.8706,-103.5931',
	'USW00024025: SD PIERRE RGNL AP':'44.3814,-100.2856',
	
	'USC00028619: AZ TOMBSTONE':'31.7119,-110.0686',
        'USW00023160: AZ TUCSON INTL AP':'32.1314,-110.9553',
        'USC00042402: CA DE SABLA':'39.8717,-121.6108',
        'USC00051959: CO CRESTED BUTTE':'38.8739,-106.9772',
        'USC00124715: IN LAFAYETTE 8 S':'40.2964,-86.9028',
        'USC00187330: MD PRINCESS ANNE':'38.2122,-75.6822',
        'USC00221094: MS BROOKHAVEN CITY':'31.5447,-90.4581',
        'USC00231275: MO CANTON L&D 20':'40.1433,-91.5158',
        'USW00023050: NM ALBUQUERQUE INTL AP':'35.0419,-106.6156',
        'USC00340908: OK BOISE CITY 2 E':'36.7236,-102.4806',

        'USC00262296: NV DIAMOND VALLEY EUREKA 14 NNW':'39.7086,-116.0494',
        'USC00355160: OR MALHEUR BRANCH EXPERIMENTAL STATION':'43.9794,-117.0247',
        'USC00369298: PA WARREN':'41.84667,-79.14944',
        'USC00457185: WA ROSS DAM':'48.7272,-121.0722'}

        single_station = {'USC00340908: OK BOISE CITY 2 E':'36.7236,-102.4806'}


	dateStart = "1980-01-01"
	dateEnd = "2015-01-01"

	# set the stations to query
        stations = updated_stations

	for station_key in stations.keys():
		latitude = stations[station_key].split(',')[0]
		longitude = stations[station_key].split(',')[1]
		station_key = station_key.replace(": ", "_").replace(" ", "_").replace("/","_")
		print "Processing.... " + station_key + "   " + str(float(latitude)) + "    " + str(float(longitude))

		pointFeatures = [ee.Feature(ee.Geometry.Point(float(longitude), float(latitude)))] #, {'name': station_key + ".csv"})]
		pointFeatureCollection = ee.FeatureCollection(pointFeatures)

		queryForGRIDMET(pointFeatureCollection, station_key, dateStart, dateEnd)
		queryForDAYMET(pointFeatureCollection, station_key, dateStart, dateEnd)



#####################
#
# Fuction to query for GRIDMET data from Google Earth Engine
#
####################
def queryForGRIDMET(pointFeatureCollection,station_key,dateStart,dateEnd):
	vVar = 'tmmx'

	vPixel = 4000

	collection = ee.ImageCollection('IDAHO_EPSCOR/GRIDMET').filterDate(dateStart, dateEnd)
	#print(collection);

	#  con 
	def convert(img):
		ppt = img.select(["pr"])
	 	ktcmx = img.select(['tmmx'],['tmmx_c']).subtract(273.15)
	 	ktcmn = img.select(['tmmn'],['tmmn_c']).subtract(273.15)
	  
		return img.addBands(ppt).addBands(ktcmx).addBands(ktcmn)

	ktoc = collection.map(convert)

	def reduceMyREgion(img):
		mean = img.reduceRegion(reducer=ee.Reducer.mean(),geometry=pointFeatureCollection, scale=vPixel,maxPixels=50000000000,bestEffort=True)
		
		return ee.Feature(None, {
			'date': ee.Date(ee.Number(img.get('system:time_start'))).format("MM-dd-yyyy"),
			'Ppt_MM': mean.get('pr'),
			'TMin_C': mean.get('tmmn_c'),
			'TMax_C': mean.get('tmmx_c')})

	mapCollection = ktoc.map(reduceMyREgion)

	taskParams = {
		'driveFolder':'GEE',
		'driveFileNamePrefix':'GRIDMET_' + station_key,
		'fileFormat':'CSV'
	}

	# expor the feature colleciton
	MyTry = ee.batch.Export.table(ee.FeatureCollection(mapCollection), 'GRIDMET_' + station_key, taskParams)

	# set a time and progress update for the task 
	MyTry.start()
	state = MyTry.status()['state']
	while state in ['READY','RUNNING']:
		print state + "...  " + station_key
		time.sleep(1)
		state = MyTry.status()['state']
	print 'Done.', MyTry.status()



#####################
#
# Fuction to query for GRIDMET data from Google Earth Engine
#
####################
def queryForDAYMET(pointFeatureCollection,station_key,dateStart,dateEnd):
	vPixel = 1000

	collection = ee.ImageCollection('NASA/ORNL/DAYMET').filterDate(dateStart, dateEnd)
	#print(collection);

	#  con 
	def convert(img):
		prcp = img.select(["prcp"])
	 	tmax = img.select(['tmax'])
	 	tmin = img.select(['tmin'])
	  
		return img.addBands(prcp).addBands(tmax).addBands(tmin)

	ktoc = collection.map(convert)

	def reduceMyREgion(img):
		mean = img.reduceRegion(reducer=ee.Reducer.mean(),geometry=pointFeatureCollection, scale=vPixel,maxPixels=50000000000,bestEffort=True)
		
		return ee.Feature(None, {
			'date': ee.Date(ee.Number(img.get('system:time_start'))).format("MM-dd-yyyy"),
			'Ppt_MM': mean.get('prcp'),
			'TMin_C': mean.get('tmin'),
			'TMax_C': mean.get('tmax')})

	mapCollection = ktoc.map(reduceMyREgion)

	taskParams = {
		'driveFolder':'GEE',
		'driveFileNamePrefix':'DAYMET_' + station_key,
		'fileFormat':'CSV'
	}

	# expor the feature colleciton
	MyTry = ee.batch.Export.table(ee.FeatureCollection(mapCollection), 'DAYMET_' + station_key, taskParams)

	# set a time and progress update for the task 
	MyTry.start()
	state = MyTry.status()['state']
	while state in ['READY','RUNNING']:
		print state + "...  " + station_key
		time.sleep(1)
		state = MyTry.status()['state']
	print 'Done.', MyTry.status()



####################
#
# The main function
#
####################
if __name__ == '__main__':
	main()

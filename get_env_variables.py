import os

os.environ['env'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

env = os.environ['env']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

appName = 'Analysis'

current = os.getcwd()

src_olap = current + '/source/olap'

src_oltp = current + '/source/oltp'

city_path = 'output/cities'

presc_path = 'output/prescriber'
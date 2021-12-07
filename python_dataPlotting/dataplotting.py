
import matplotlib.pyplot as plt
import zipfile
import io
import swiftclient
import quinn
import yaml
import cartopy.io.img_tiles as cimgt
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
from pyspark.sql.functions import explode
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, array, lit
from pyspark.context import SparkContext
import time

spark = SparkSession\
  .builder \
  .appName("DataPlotting") \
  .getOrCreate()

sparkContext = SparkContext\
  .getOrCreate()

plottedLines = 0
start_time = time.time()

config = yaml.safe_load(open("config.yml"))

# Function to extract datas from zip file
def zip_extract(x):
    in_memory_data = io.BytesIO(x[1])
    file_obj = zipfile.ZipFile(in_memory_data, "r")
    files = [i for i in file_obj.namelist()]
    return dict(zip(files, [file_obj.open(file).read() for file in files]))

# Get the datas from the zip file
zips = sparkContext.binaryFiles("datas.zip")

# Extract it into a dict
files_data = zips.map(zip_extract).collect()[0]

# Init the map
ax = plt.axes(projection=ccrs.PlateCarree())

# Init the terrain to add to the map
stamen_terrain      = cimgt.QuadtreeTiles('RGB', 'street')

# projections that are involved
st_proj = stamen_terrain.crs  #projection used by Stamen images
ll_proj = ccrs.UTM(10) #UTM for datas and extent

# create fig and axes using intended projection
fig = plt.figure(figsize=(11.69,8.27))
ax = fig.add_subplot(111, projection=st_proj)   

# Read the datas out of the master CSV file that handle all the other CSV files
colors = list(map(lambda col_name: lit(col_name), mcolors.CSS4_COLORS))

# Converting the dict into a readable format for the Dataframe ie an array of 
# two values : the name of the file and the datas it contains
files_data_array= list(map(list, files_data.items()))

# Converting to a dataframe with a structure name/value
df = spark.createDataFrame(files_data_array, ["name", "value"])

# Preparing the datas : we first remove everything that is not a CSV and we remove the master CSV that we don't need
# Then, we'll cast the value as it is considered as an array of bytes
# After that, we choose a random color for this file to draw on the map later
# In the end, we map all our lines so that the content of the file is split into an array each containing one line
df = df.filter((~ df.name.endswith("master.csv")) & df.name.endswith(".csv")) \
    .withColumn("value", col("value").cast("String")) \
    .withColumn("color", quinn.array_choice(array(*colors))) \
    .rdd.map(lambda line: (line.name, line.value.split("\n")[1:], line.color)) \
    .toDF(["name", "value", "color"])

# We explode our Dataframe so that we have one line into one per line of the CSV files
# We also remove the empty lines
explodedDf = df.select(df.name,explode(df.value), df.color) \
    .filter("col <> ''")
# We create new columns to get the values we need : x, y, x_end and y_end
explodedDf = explodedDf.withColumn('x', split(explodedDf['col'], ',').getItem(0).cast('Double')) \
       .withColumn('y', split(explodedDf['col'], ',').getItem(1).cast('Double')) \
       .withColumn('x_end', split(explodedDf['col'], ',').getItem(30).cast('Double')) \
       .withColumn('y_end', split(explodedDf['col'], ',').getItem(31).cast('Double'))

# We make a list of lines out of our Dataframe
vectors = explodedDf.rdd.map(lambda line: ([line.x, line.x_end], [line.y, line.y_end], line.color)).collect()

# We also get the max and min x and y values so that we can make a correct extent
max_x = max(explodedDf.agg({"x": "max"}).collect()[0][0], explodedDf.agg({"x_end": "max"}).collect()[0][0])
min_x = min(explodedDf.agg({"x": "min"}).collect()[0][0], explodedDf.agg({"x_end": "min"}).collect()[0][0])
max_y = max(explodedDf.agg({"y": "max"}).collect()[0][0], explodedDf.agg({"y_end": "max"}).collect()[0][0])
min_y = min(explodedDf.agg({"y": "min"}).collect()[0][0], explodedDf.agg({"y_end": "min"}).collect()[0][0])

# Define the extent of the datas we want to see
margin              = 10.0
extent              = [min_x - margin, max_x + margin, min_y - margin, max_y + margin]

# Set the extents
ax.set_extent(extent, crs=ll_proj)

# Get and plot Stamen images
ax.add_image(stamen_terrain, 18) # this requests image, and plot
ax.gridlines(draw_labels=True)

# Finally draw the lines
for vector in vectors:
    plottedLines = plottedLines + 1
    plt.plot(vector[0], vector[1],
            color=vector[2], linewidth=1,
            transform=ll_proj
            )

# Define the format and name of the temp file
image_format = 'png' # e.g .png, .svg, etc.
image_name = 'fish_datas_plotted.png'

# Save the temp file
plt.savefig(image_name, format=image_format, dpi=300)

# Connect to our container
swift = swiftclient.client.Connection(
    auth_version='3',
    preauthurl=config['preauthurl'],
    preauthtoken= config['preauthtoken']
    )

# Get our temp file datas
with open(image_name, 'rb') as f:
    file_data = f.read()

# Put the file on our container
swift.put_object(config['container_name'], image_name, file_data)

print("--- %s plotted lines in %s seconds ---" % (plottedLines, (time.time() - start_time)))

# Stop Spark Process
spark.stop()
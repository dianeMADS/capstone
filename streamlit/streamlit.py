# -*- coding: utf-8 -*-
"""streamlit.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1PNepn-yeKr3KDAexRLmP0zaBsaichYbp
"""

import streamlit as st
import pandas as pd
import numpy as np

import gzip
import os

from PIL import Image

import matplotlib.pyplot as plt
import altair as alt
from streamlit_folium import folium_static
import folium
import seaborn as sb

from statsmodels.tsa.api import VAR
from statsmodels.tsa.vector_ar.var_model import VARResults, VARResultsWrapper
 
# # Suppress warnings
# import warnings
# from statsmodels.tools.sm_exceptions import ValueWarning
# warnings.simplefilter("ignore", ValueWarning) 
# Title

# st.title("MADS 697/698 - CAPSTONE")
# st.header("Diane O and James N")


def main():
  # "# MADS CAPSTONE, SIADS 697-698"
  st.header("MADS CAPSTONE, SIADS 697-698")
  # st.markdown('\n\n')
  st.title("Covid-19 Lockdown and Pollution in the City of Chicago?")
  # st.markdown('\n\n')
  st.header("Diane O. and James N.")
  st.markdown('\n\n')

  # menu = ["Test 1", "Test 2"]
  # choice = st.sidebar.selectbox('Menu', menu)
  # if choice == 'Test 1':
  #     st.header("This is Test 1")

  st.markdown(
    """For our project, we explored the Chicago Array of Things Dataset.  This massive dataset was collected between 2018 and 2020 from a node array installed throughout the city of Chicago.  Nodes have a variety of sensors installed at each location.  These sensors fall into one of six categories air quality, meteorological, physical, environmental, system, and vision.  Initially, we hoped to focus on determining the quality of life around the city.  This proved to be a bit too ambitious, however.  We found it very difficult to define quality of life and focus in on key measurements.  Instead, we focus mainly on the air quality metrics.  These metrics are easier to track over time and there are clear definitions for gas concentrations that are considered dangerous or unhealthy.  The analysis focuses on the concentrations of five gasses: CO, H2S, NO2, O3, and SO2, as well as oxidizing and reducing gas concentrations."""
  )

  st.markdown('\n\n')
  st.header("Reducing the Data")
  st.markdown(
      """The data was initially collected and stored in a column style database with seven columns: timestamp, node id, subsystem, sensor, parameter, raw value, hrf value (processed value).  Ideally, we would have hosted this data on a cloud server and maintained the column database format by using Cassandra or a similar database schema or migrated to a relational database format such as PostgreSQL, however, the dataset was over 300GB in size and pushed the cost too high for our project.  We instead elected to write our own program to reduce the file to a manageable size. """
  )
  st.markdown(
      """After the file was un tar ed to a gzip file, the first step was to split the data into separate files based on the node that collected the data.  This was done using the bash command line and the function below:"""
  )
  st.code(
      """zcat file.csv.gz | awk -F “,” ‘{print>“subfolder/”$2“.csv”}’""", language="cli"
  )
  st.markdown(
      """This command unzips the file line by line and pipes it to the awk function.  This function splits the line by a comma and prints the line to a file with the name of value found in the second column (the node id).  This significantly reduce the size of each file we worked with but with the largest files around 10 GB, we still had to reduce the data.  Data was collected every second at each node.  We did not need this level of granularity and decided to average the readings every hour.  Theoretically, this would reduce our data by a factor of about 3600.  We completed this reduction in C# due to its speed and support for parallel processes, but a similar pipeline could be implemented in python. """
  )
  st.markdown(
      """The process we used is only possible with the data was already sorted by ascending date.  The entire code can be found on our GitHub page.   The process was to first assign a thread to read a file.  The first line in the file becomes the start time rounded to the nearest half hour rounded down (ex 4:15 would have a start time at 3:30 and 4:45 would have a start time of 4:30) and the end time is the start time plus one hour.  A dictionary of values with the parameter, subsensor, and system as the key and the total for each as the values was then created.  Once the timestamp is greater than the end time or there are no more lines in the file to read, the thread takes the mean value for each key and appends it to a new csv file with the node id.  The values are cleared and the thread continues if there are more values in the file or moves on to the next file.  The entire process takes about 30 minutes to run, and we were able to reduce the size of the entire dataset from 300 GB down to 1.8 GB.  This size is near the limits for pandas but proved to be manageable after filtering by metrics for each analysis."""
  )

  st.markdown("data from one node")
  sample_data = pd.read_csv('streamlit/data/Numeric_001e06112e77.csv')
  st.dataframe(sample_data.head())

  st.markdown("master dafaframe")
  master_df = pd.read_csv("streamlit/data/cleaned_dataset.zip")
  st.dataframe(master_df.head())

  st.markdown('nodes')
  nodes = pd.read_csv("streamlit/data/nodes.csv")
  st.dataframe(nodes.head())

  st.markdown('sensors')
  sensors = pd.read_csv('streamlit/data/sensors.csv')
  st.dataframe(sensors.head())

  st.markdown('\n\n')
  st.header("Data Exploration")
  st.markdown(
      """Once the data was reduced to hourly readings, we were able to explore the data using pandas.  Our initial exploration provided us with locations of the sensors and the time frame that each sensor was active.  With this information, we decided to modify our initial plan of focusing on quality of life to focus instead on a causal inference model comparing how trends in air quality changed due to Covid related lockdowns in the city."""
  )
  


  st.markdown('node locations')
  st.write(
  """The first step of our data exploration was to look at the node distribution across the city. The map below shows node locations based on their latitude and longitude; there is a very good coverage of the city of Chicago, with a good bunch of nodes along the coast of Lake Michigan. This exercise suggests some clustering for the upcoming analysis, since we expect air quality to better close to the lake than it is in the city, especially in industrial zones."""
  )
  latlon = list(zip(nodes['lat'], nodes['lon'], nodes['node_id']))
  mapit = folium.Map( location=[41.85, -87.65], zoom_start=11 )

  for coord in latlon:
    folium.Marker( location=[ coord[0], coord[1] ],
                tooltip=('node:', coord[2], 'lat:', coord[0], 'lon:', coord[1]),
                #  tooltip = ''
                popup=coord[2]).add_to( mapit )

  folium.TileLayer('cartodbpositron').add_to(mapit)
  folium_static(mapit)


  st.markdown('\n\n')
  st.markdown('time for data collection')
  st.write(
      """The city of Chicago AoT website reported that nodes were commissioned and decommissioned between 2017 and 2020. Further than commissioning/decommissioning period of times, the plot below indicates exactly when data collection started and ended for the given nodes. We notice lots of inconsistencies among nodes, with recording times going from 0 to 1112 days for an average of 416 days. It is difficult to envisage a per node study, otherwise many if not most of the nodes will not have enough data for a decent analysis of the periods before covid-19 (i.e. from the commissioning to March 20th, 2020) and lockdown from March 21st to May 31st 2020."""
  )
  st.markdown(
    """Two main takes on from this: (i) we decided to average parameters over nodes, assuming that they shouldn’t be that different within the same city within the same season. (ii) In a second step, we will refine the study to average and thereby make the analysis per similarity or within each cluster."""
  )

  up_df = pd.DataFrame(columns=['node_id', 'start', 'end'])
  idx = 0
  for node in nodes.node_id:
    sample = master_df[master_df.node_id == node]
    up_df.loc[idx] = [node, pd.to_datetime(sample.date).min(), pd.to_datetime(sample.date).max()]
    idx += 1

  up_df['days_up'] = (up_df.end.dt.date - up_df.start.dt.date).dt.days
  st.dataframe(up_df.head())

  st.markdown('time for data collection, description ...')
  st.dataframe(up_df.describe().T)  

  base = alt.Chart(up_df).encode(
      alt.X('node_id:N')
  ).properties(width = 1400, height = 350)

  rule = base.mark_rule().encode(
      alt.Y('start:T', axis = alt.Axis(format='%m/%y', title='Date')), #,labelAngle=-45
      alt.Y2('end:T')
  )

  startpoints = base.mark_circle(size=60).encode(
      alt.Y('start:T'),
      # alt.Y2('end:T')
  )

  endpoints = base.mark_circle(size=60).encode(
      # alt.Y('start:T'),
      alt.Y('end:T'), color = alt.value("#FFAA00")
  )

  st.altair_chart(rule + startpoints + endpoints)


  st.markdown('\n\n')
  st.markdown('node sensor types')
  st.markdown(
    """Another important aspect of the city of Chicago AoT dataset is that nodes comprise a set of sensor subsystems that might differ from one node to the other. The visualization below shows ten different types of subsystems and indicates whether they are present in given AoT nodes. All 118 nodes (i.e. except one) comprise lightsense and meltsense that collect meteorological and a few environmental data. Out of those 118 nodes, 94 contain a chemsense to record air quality gaze concentrations. It is noticeable that most of the nodes (but three) that have an alphasense do not have a plantower and vice versa, both types collecting air quality particle counts. Our assumption is that plantower and alphasense are two versions of the same type of subsystem."""
  )
  st.markdown(
    """Remaining susbystems are available in a few nodes only, and/or belong to device and network quality that is out of the scope of this study. At this point, we realized that the amount of image and audio sensors reporting such important metrics as traffic (e.g. cars or people counts) and noise is not enough; we are not able to build some quality of life definition around the topic…"""
  )

  subsystem_types = master_df[['node_id', 'subsystem']].groupby(['node_id', 'subsystem']).count().reset_index()
  subsystem_types['count'] = 1


  sensor_chart = alt.Chart(subsystem_types).mark_tick().encode(
    x='node_id',
    y='subsystem',
    color='subsystem'
  ).properties(width=1400) #, height=250
  st.altair_chart(sensor_chart)


  st.markdown('\n\n')
  st.markdown('sensors')
  st.markdown(
    """Going deeper in the node examination, we finally looked at the actual metrics collected per sensor for the selected set of subsystems. In the visualization below, the six subsystems under consideration are color-coded, y-axis identifies sensors and x-axis gives corresponding parameters. Parameters related to pollution are (i) concentration from every gaze present in chemsense, (ii) and particles from plantower and alphasense. In order to simplify this first look at AoT, we limited our causal inference analysis to those air quality attributes."""
  )

  subsystem_sensor_types = master_df[['subsystem', 'sensor']].groupby(['subsystem', 'sensor']).count().reset_index()
  subsystem_types['count'] = 1

#   subsystem_chart = alt.Chart(subsystem_sensor_types).mark_rect().encode(
#     x='sensor',
#     y='subsystem',
#     # color='subsystem'
#   ).properties(width=1400)
#   st.altair_chart(subsystem_chart)

  filtered_subsystems = master_df[master_df['subsystem'].isin(['lightsense', 'metsense', 'chemsense', 'alphasense', 'plantower'])]
  subsystem_sensor_types = filtered_subsystems[['subsystem', 'sensor']].groupby(['subsystem', 'sensor']).count().reset_index()
  subsystem_types['count'] = 1

#   filteredsub_chart = alt.Chart(subsystem_sensor_types).mark_rect().encode(
#     x='sensor',
#     y='subsystem',
#     color='subsystem'
#   ).properties(width=1400)
#   st.altair_chart(filteredsub_chart)

  sensor_types_parameters = filtered_subsystems[['subsystem', 'sensor', 'parameters']].groupby(['subsystem', 'sensor', 'parameters']).count().reset_index()
  sensor_types_parameters['count'] = 1

  param_chart = alt.Chart(sensor_types_parameters).mark_rect().encode(
    x='parameters',
    y='sensor',
    color='subsystem'
  ).properties(width=1400, height=500)
  st.altair_chart(param_chart)

  st.dataframe(filtered_subsystems.head())


  
  st.markdown('\n\n')
  st.markdown('* Particles count')
  st.markdown(
    """ ."""
  )

  st.markdown(
    """ """
  )


  pms = ['10um_particle', '1um_particle', '2_5um_particle', '5um_particle', 'pm1', 'pm10', 'pm10_atm', 'pm10_cf1', 'pm1_atm', 'pm1_cf1', 'pm25_atm', 'pm25_cf1', 'pm2_5', 'point_3um_particle', 'point_5um_particle', 'fw', 'sample_flow_rate', 'sampling_period']
  # 'concentration', 
  df_w_pms = filtered_subsystems[filtered_subsystems['parameters'].isin(pms) ].drop(['node_id', 'subsystem', 'sensor'], axis=1)

  df_w_pms = pd.pivot_table(df_w_pms, values = 'values', index = 'date', columns = 'parameters', aggfunc=np.mean).reset_index()
  # df_w_pms = df_w_pms.fillna(method="bfill")

  # df_w_pms
  st.dataframe(df_w_pms.describe())

#   "# might need a static image for seaborn and refer back to the notebook..."
#   plt.rc('figure', figsize=(25, 10))
#   fig = sb.heatmap(df_w_pms.corr(method='pearson'), cmap='YlGnBu', annot=True)
#   st.plt(fig)


  st.markdown('\n\n')
  st.markdown('Initial clustering')
  st.markdown(
      """We also ran an initial clustering analysis of the nodes based on the types of sensors at each location. A sparse matrix for each node with the sensor types as the values was used.  The sensor types were limited to only the subsystem types that include air quality metrics.  The histogram below shows the clusters identified using agglomerative clustering with a distance threshold of 15.  """
  )
  st.image(Image.open("streamlit/data/ExploratoryClusterCounts.jpg"))

  st.markdown(
     '''The clustering shows that the concentrations of the gasses are well represented in the blue and red cluster groups.  The particle sizes are only represented in the red cluster group and appear at less than 30 nodes. Therefore, these metrics may not be appropriate for this analysis.  The image below shows the locations of these clusters.  We can see that group 1, represented in orange, covers much of the city but lacks the air quality sensors.  We still believe that the other clusters provide enough coverage of the city and will provide valid results.'''
    )
    
  st.header("Cluster Analysis")
  
  st.markdown(
      """Node clustering seemed like an obvious choice for analysis with similar data being collected around the city.  We were expecting to find some similarity between nodes with each metric that may indicate differences or similarities between neighborhoods that are not physically close to each other.  The focus of this analysis is on the air quality metrics based on the concentrations of selected gasses.  This analysis does not explore what is “good” vs “bad” air quality, it instead focuses on identifying similar concentration levels of gasses over time. The results of this analysis may be interesting to correlate with other metrics such as income maps, housing prices, zoning (ex. business vs residential areas), and heath maps."""
  )
  st.markdown(
      """Two representation for the data were used to compare results.  The first representation averages the data over each day of the week.  The motivation for this comes from the assumption that there are differences in behavior on each weekday that may influence concentrations of gasses in the air such as differences between workday and weekend traffic (shape (number of nodes, 7 * parameters)).  The second representation averages all the values by date, combining the date in previous years together (shape = (number of nodes, 366*parameters)). Both representations do not include data collected during the lockdowns to prevent any lockdown related fluctuations from skewing the results.  Both data representations have their benefits.  The weekly representation reduces the daily noise significantly but loses much of the trends over the year that may be important for cluster separation.  The date representation maintains much of the trends but will place more weight on noisy data.  This representation also gives null values more weight since they must be treated as zeros."""
  )
  st.markdown(
      """Agglomerative clustering was used as our baseline algorithm to compare the other methods to.  This method provides the most direct control over the groupings and is the easiest to modify.  Due to the large daily fluctuations, we defined the number of clusters to be 5 instead of tuning for the distance threshold.  DBSCAN and OPTICS rely on identifying groupings of clusters.  These algorithms also identify noise points that are between clusters.  DBSCAN proved extremely difficult to generate reliable clusters.  Most of the runs produced a single cluster and noise points.  OPTICS is a generalized version of DBSCAN and was able to provide better results.  The spectral clustering algorithm had two clusters specified with the radial basis function as the affinity matrix.  Finally, affinity propagation was run as it attempts to determine how close two nodes are to being copies. This algorithm used the preset values suggested by the sklearn documentation with Euclidean distance in the affinity matrix."""
  )
  st.markdown(
      """The results of this analysis were not as consistent as we had hoped.  The most reliable results came from the Agglomerative clustering with the date averaging data representation.  The plot below shows the clusters for the date representation.  The agglomerative clustering algorithm is the only one that appears to have some consistency in its clusters.  DBSCAN was run using a wide range of values for the eps parameter but it was never able to identify more than one cluster and noise.  OPTICS consistently provided a noise group and one or two clusters.  There appears to be a difference between the two clusters in the timeseries, but it is not clear why the separation occurred.  Both groups appear to have significant noise and inconsistent measurement across the timeframe.  The spectral clustering algorithm continually failed to provide fully connected graphs and thus produced unreliable cluster groups.  Affinity propagation produced 22 clusters with both data representations.  It is unclear why so many groups were produced but some differences in the active timeframes for each node and spikes in gas concentrations may account for some of the variability. """
  )
   
  def cluster_timeseries():
    data = pd.read_csv('streamlit/data/clustered_dataset2.zip')
    cols = ['concentration_co', 'concentration_h2s', 'concentration_no2', 'concentration_o3', 'concentration_oxidizing_gases', 'concentration_reducing_gases', 'concentration_so2']
    row = None
    column = None
    for i in ['Agglomerative','DBSCAN','OPTICS', 'Spectral','AffinityPropagation']:
      row = None
      for j in cols:
        df = data.groupby(['date', i]).mean()[[j]].reset_index()
        c = alt.Chart(df).mark_line().encode(
            x=alt.X('date:T', title=''),
            y=f'{j}:Q',
            color=f'{i}:N'
        ).properties(width=200)
        if row == None:
          row = c 
        else:
          row = row|c
      if column == None:
        column = row
      else:
        column = (column&row).resolve_scale(color='independent')
    return column
  st.altair_chart(cluster_timeseries())
  st.markdown(
      """For the agglomerative clustering, it appears that there are differences in the cluster groups that are clear in the average time series for each cluster especially between 2019 and 2020. The blue (0) cluster group has far greater variance than the orange (4) cluster.  We can also see that the blue cluster does not continue into 2021. In the map, we can see that most of the nodes belong to a single cluster.   The smallest clusters appear to be the least consistent sensors and have many data gaps or were only active for a short period of time. It appears that the clusters may have only separated based on measurement consistency.  Further analysis is required to understand why these nodes are producing noisier data.  It is unclear if this is due to a faulty sensor or if there are local sources of pollution that cause the spikes.  Future projects could compare these clusters to traffic patterns and industrial centers that could be producers.  It would also be beneficial to include more variables such as wind speed and direction that have an impact on the direction of pollution plume movement.  """
  )
  st.image(Image.open('streamlit/data/AgglomerativeClusters.jpg'), caption='Agglomorative Clusters')



if __name__ == '__main__':
  main()

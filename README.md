# capstone

Welcome to our Github repository for our analysis of the Chicago Array of Things Dataset.  The entire dataset can be found here: http://arrayofthings.github.io/.  

Please see our video explanation of our project located here: https://drive.google.com/file/d/13i5_bAYNkdLmUPxKhUNY_RBTNTcorssL/view?usp=sharing.
This provides an overview of our methods and analysis of the results.

The source code for our blog can be found in this repository. The blog can be viewed at this address: https://share.streamlit.io/dianemads/capstone/main/streamlit/streamlit.py

The CSharpSplits provides the code we used to reduce the dataset from its original 300 GB down to the 1.8 GB we used in our analysis.  This process requires data to be uncompressed to its gzip format.  Once there we used the bash command line to split the file by node ids using the command: -zcat data.csv.gz | awk -F "," '{print>$2".csv"}'.  This command produces separate unzipped files with the node id as the file name.  From there, our process was relatively simple.  Each file is read in parallel line by line.  The values are compared to the accepted values provided by the Chicago Array of Things dataset authors found in the sensors.csv file in the tar download.  If the values are outside of the accepted range, they are ignored. If not, they are added to a dictionary with the sensor information as the key.  Once the thread reaches the end of the hour or there are no values remaining, the values for each key are averaged and output to a file with the node name.  The process repeats until there are no lines remaining in the file.  
When using this code, it is important to note that the user must define the location of three files: the sensor.csv file, the folder that holds the split data, and the desired outfile location.

The notebooks are self guided and are named for each of the analysies completed.  The Capstone_Clustering.ipynb (also located here: https://colab.research.google.com/drive/1cOF5HxLqgX7ctv5Kh9x1HmH6RCngOZdk?usp=sharing) provides the guide for how the clustering was completed.  The causal_inf.ipynb (also located here: https://colab.research.google.com/drive/1d4nclhPZjrF58zoMY0_WiKBNh8j3n16a?usp=sharing) provides a guided tour of the causal inference analysis.

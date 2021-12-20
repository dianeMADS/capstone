using System;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;


namespace CSharpSplit
{
    class Reader
    {
        
        const string filepath = "D:\\Waggle_Others.complete.2021-10-30\\data";
        const string outPath = "D:\\Waggle_Others.complete.2021-10-30\\CSharpSplits\\";
        static string[] filesList = Directory.GetFiles(filepath, "*.csv");
        // int processCount = System.Environment.ProcessorCount;
        static Random rnd = new Random();
        static System.Globalization.CultureInfo enUS = new System.Globalization.CultureInfo("en-US");

        //Check if value can be parsed
        static public float? isFloat(string value){
            float val;
            if (float.TryParse(value, out val))
            {
                return val;
            } else {return null;}
        }

        // Write the data
        static public void writeVals(string outfile, 
            Dictionary<string, Dictionary<string, Dictionary<string, float?>>> dataDict, 
            DateTime start)
            {
            bool exists = File.Exists(outfile);
            using (StreamWriter w = File.AppendText(outfile)){
                if (!exists){
                    w.WriteLine("timestamp,subsystem,sensor,parameters,value_raw,value_hrf,valid_raw_measurements_count,valid_hrf_measurements_count");
                }
                foreach (string key in dataDict.Keys)
                {
                    foreach(string metric in dataDict[key].Keys)
                    {
                        float? count_raw = dataDict[key][metric]["raw_count"];
                        float? count_hrf = dataDict[key][metric]["hrf_count"];
                        float? vraw;
                        float? vhrf;
                        if (count_raw > 0){ 
                            vraw = dataDict[key][metric]["value_raw"] / count_raw;
                        } else {vraw = null;}
                        if (count_hrf>0){
                            vhrf = dataDict[key][metric]["value_hrf"] / count_hrf;
                        } else {vhrf = null;}
                        
                        w.WriteLine($"{start},{metric},{key},{vraw},{vhrf},{count_raw},{count_hrf}");
                    }
                }
            }
        }
        
        static public Dictionary<string, float?[]> ValsRange(string rangeFile){
            Dictionary<string, float?[]> vals = new Dictionary<string, float?[]>();
            using (var sr = new StreamReader(@rangeFile)){
                string lines;
                while ((lines = sr.ReadLine()) != null){
                    string[] line = lines.Split(',');
                    float?[] values =  {isFloat(line[5]), isFloat(line[6])};
                    vals.Add(line[1]+","+line[2]+","+line[3], values);
                }
                
            }
            return vals;

        }

        static public (DateTime, DateTime) getTime(DateTime startDate){
            DateTime start = new DateTime();
            DateTime endDate = new DateTime();
            if (startDate.Minute < 30) {
                endDate = new DateTime(startDate.Year, startDate.Month, startDate.Day, startDate.Hour, 30, 0);
                start = startDate.AddHours(-1);
            } else {
                endDate = startDate.AddHours(1);
                start = new DateTime(startDate.Year, startDate.Month, startDate.Day, startDate.Hour, 30, 0);
            }
            return (start, endDate);
        }

        static public Dictionary<string, Dictionary<string, Dictionary<string, float?>>> updateDict(string[] data, 
            Dictionary<string, Dictionary<string, Dictionary<string, float?>>> dataDict)
        {
            float? raw = isFloat(data[5]);
            float? hrf = isFloat(data[6]);
            int count_raw = 1;
            int count_hrf = 1; 
            if (raw == null){
                count_raw = 0;
                raw = 0;
            }
            if (hrf == null){
                count_hrf =0;
                hrf = 0;
            }
            
            if (dataDict.ContainsKey(data[4]))
            {
                if (dataDict[data[4]].ContainsKey(data[2]+","+data[3])){
                    string kv = data[2]+","+data[3];
                    dataDict[data[4]][kv]["value_raw"] += raw;
                    dataDict[data[4]][kv]["value_hrf"] += hrf;
                    dataDict[data[4]][kv]["raw_count"] += count_raw;
                    dataDict[data[4]][kv]["hrf_count"] += count_hrf;
                    
                }else{
                    dataDict[data[4]].Add(data[2]+","+data[3], new Dictionary<string, float?>
                        {{"value_raw", raw},
                        {"value_hrf", hrf},
                        {"raw_count", count_raw},
                        {"hrf_count", count_hrf} 
                        
                    });
                }
            } else {
                Dictionary <string, float?> vals = new Dictionary<string, float?>{
                        {"value_raw", raw},
                        {"value_hrf", hrf},
                        {"raw_count", count_raw},
                        {"hrf_count", count_hrf}
                        
                };
                dataDict.Add(data[4], new Dictionary<string, Dictionary<string, float?>>
                    {{data[2]+","+data[3], vals}} );
            }
            return dataDict;
        }

        static void Main(string[] args)

        {
            var rangeDict = ValsRange("D://Waggle_Others.complete.2021-10-30//sensors.csv");
            Parallel.ForEach(filesList, new ParallelOptions {MaxDegreeOfParallelism = 3},
            infile => {
                int folder = rnd.Next(1,3);
                string fileName = Path.GetFileName(infile);
                string numOutfile = outPath + folder.ToString() + "//Numeric_" +fileName;
                //string stringOutfile = outPath + "String_" +fileName;

                using (var sr = new StreamReader(@infile)) {
                    
                    string line;
                    var valTypes = new List<string>();
                    var valueRange = rangeDict;
                    // string str_vals = "";
                    
                    DateTime startDate = new DateTime(1970,1,1,0,0,0);
                    DateTime start = new DateTime (1970,1,1,0,0,0);
                    DateTime endDate = new DateTime(2030, 1,1,0,0,0);

                    Dictionary<string, Dictionary<string, Dictionary<string, float?>>> dataDict = 
                        new Dictionary<string, Dictionary<string, Dictionary<string, float?>>>();

                    while ((line = sr.ReadLine()) != null) {
                        var data = line.Split(",");

                        //Check if it is a string value
                        float number;
                        if (!((float.TryParse(data[5], out number)) || (float.TryParse(data[6], out number)))) 
                        {
                            continue;
                        }
                        
                        if (valueRange.ContainsKey(data[2]+","+data[3]+","+data[4])){
                            float? min = valueRange[data[2]+","+data[3]+","+data[4]][0];
                            float? max = valueRange[data[2]+","+data[3]+","+data[4]][1];
                            float? hrf = isFloat(data[6]);
                            if ((hrf > max || hrf < min) && hrf!= null){
                                continue;
                            }
                        }

                        //Check and parse the date
                        DateTime date;
                        if (!(DateTime.TryParseExact(data[0], "yyyy/MM/dd HH:mm:ss", null, 
                            System.Globalization.DateTimeStyles.None, out date)))
                        {
                            continue;
                        }
                        if (startDate.Year < 2000) {
                            startDate = date;
                            (start, endDate) = getTime(startDate);
                        }
                        
                        //Check if the hour is up
                        
                        if (endDate < date ){
                            //Write the average hourly value to the file
                            writeVals(numOutfile, dataDict, start);
                            //Clear the dictionary
                            dataDict.Clear();
                        

                            //Set the new time window
                            startDate = date;
                            (start, endDate) = getTime(startDate);
                        } else 
                        { 
                            //Add to dictionary of values if time is not up and keep count of the values
                            dataDict = updateDict(data, dataDict);
                        } 
                    }

                    //Append the dictionary once all values are read  
                    writeVals(numOutfile, dataDict, start);

                }
            });
        }
    }
}
    

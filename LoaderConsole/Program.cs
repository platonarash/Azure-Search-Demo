using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace LoaderConsole
{

    public class Coordinate
    {

        public string type;

        public float[] coordinates = new float[2];
    }


    public class ZipCode
    {
        public ZipCode()
        {
            geo_location = new Coordinate();
        }

        public string zip_code;
        public string country;
        public string city;
        public string state;
        public string state_ab;
        public string county;
        public Coordinate geo_location;
    }


    class Program
    {

        static string _serviceUrl = "https://arashtest2.search.windows.net";
        static string _jobsIndex = "nycjobs";
        static string _zipIndex = "zipcodes";
        static string _apiVersion = "2015-02-28";
        static string _key = "DF16A195430D05B4823981FF62BC6247";

       


        static void Main(string[] args)
        {
            FixJson();

            Task.Run(async () =>
            {
                // Do any async anything you need here without worry
                await DoWork();
            }).Wait();
        }

        private async static Task DoWork()
        {

            //*********** DELETE  INDEXS ****************
            // Delete Jobs index
            var deletedJobs = DeleteIndex(_serviceUrl, _jobsIndex, _apiVersion, _key).Result;
            if (!deletedJobs)
            {
                Console.WriteLine("ERROR: Index not deleted!" + _jobsIndex);
                return;
            }

            // Delete Jobs index
            var deletedZips = DeleteIndex(_serviceUrl, _zipIndex, _apiVersion, _key).Result;
            if (!deletedJobs)
            {
                Console.WriteLine("ERROR: Index not deleted!" + _jobsIndex);
                return;
            }




            //*********** CREATE  INDEXS ****************

            // Create Jobs index
            string nyjobsSchemaFile = @"C:\Demos\KÖR 2 - Azure Search Suggestions\search-dotnet-asp-net-mvc-jobs-master\NYCJobsWeb\Schema_and_Data\nycjobs.schema";
            bool isIndexCreatedJob = CreateIndex(nyjobsSchemaFile, _serviceUrl, _jobsIndex, _apiVersion, _key).Result;

            if (!isIndexCreatedJob)
            {
                Console.WriteLine("ERROR: Index not created!" + _jobsIndex);
                return;
            }


            // create Zipcodes index
            string zipSchemaFile = @"C:\Demos\KÖR 2 - Azure Search Suggestions\search-dotnet-asp-net-mvc-jobs-master\NYCJobsWeb\Schema_and_Data\zipcodes.schema";
            bool isIndexCreatedZip = CreateIndex(zipSchemaFile, _serviceUrl, _zipIndex, _apiVersion, _key).Result;

            if (!isIndexCreatedZip)
            {
                Console.WriteLine("ERROR: Index not created!" + _zipIndex);
                return;
            }



            //************ LOAD INDEXES ******************
            DirectoryInfo currentDir = new DirectoryInfo(@"C:\Demos\KÖR 2 - Azure Search Suggestions\search-dotnet-asp-net-mvc-jobs-master\NYCJobsWeb\Schema_and_Data");
            System.IO.FileInfo[] nyjobs = currentDir.GetFiles("nycjobs*.json");
            LoadIndexes(nyjobs, _jobsIndex).Wait();


            DirectoryInfo currentDir2 = new DirectoryInfo(@"C:\Demos\KÖR 2 - Azure Search Suggestions\search-dotnet-asp-net-mvc-jobs-master\NYCJobsWeb\Schema_and_Data");
            System.IO.FileInfo[] zipCodes = currentDir2.GetFiles("zipcodes*.json.new");

            //System.IO.FileInfo[] zipCodes = currentDir2.GetFiles("zipcodesTest.json");

            LoadIndexes(zipCodes, _zipIndex).Wait();

        }

        static async Task<bool> CreateIndex(string schemaFilePath, string serviceUrl, string indexName, string apiVersion, string key)
        {

            // Load the json containing the schema from an external file
            string json = File.ReadAllText(schemaFilePath);

            using (HttpClient client = new HttpClient())
            {
                client.BaseAddress = new Uri(serviceUrl);
                client.DefaultRequestHeaders.Add("api-key", _key);
                var requestUri = "indexes/" + indexName + "?api-version=" + _apiVersion;

                HttpResponseMessage result = client.PutAsync(requestUri, new StringContent(json, Encoding.UTF8, "application/json")).Result;

                if (result.StatusCode == HttpStatusCode.Created)
                {
                    Console.WriteLine("Index created. \n");
                    return true;
                }

                Console.WriteLine("Index creation failed: {0} {1} \n", (int)result.StatusCode, result.Content.ReadAsStringAsync().Result.ToString());
                return false;
            }
        }


        static async Task<bool> DeleteIndex(string serviceUrl, string indexName, string apiVersion, string key)
        {
            //This will execute a delete request
            using (HttpClient client = new HttpClient())
            {
                client.BaseAddress = new Uri(serviceUrl);
                client.DefaultRequestHeaders.Add("api-key", _key);
                var requestUri = "indexes/" + indexName + "?api-version=" + _apiVersion;
                HttpResponseMessage result = client.DeleteAsync(requestUri).Result;

                if (result.StatusCode == HttpStatusCode.NoContent)  // HTTP 204 is ok
                {
                    Console.WriteLine("Index Deleted. \n");
                    return true;
                }
                else if (result.StatusCode == HttpStatusCode.NotFound)
                {
                    Console.WriteLine("Could not find existing index, continuing... \n");
                    return true;
                }

                Console.WriteLine("Index Deletion Failed: : {0} {1} \n", (int)result.StatusCode, result.Content.ReadAsStringAsync().Result.ToString());
                return false;
            }

        }

        static async Task LoadIndexes(System.IO.FileInfo[] files, string indexName)
        {

            /****** LOAD ****************/
            HttpClient client = new HttpClient();
            client.BaseAddress = new Uri(_serviceUrl);
            client.DefaultRequestHeaders.Add("api-key", _key);
            var requestUri = "indexes/" + indexName + "/docs/index?api-version=" + _apiVersion;


            foreach (var j in files)
            {
                var json = File.ReadAllText(j.FullName);

                // To add data use POST
                var result = await client.PostAsync(requestUri, new StringContent(json, Encoding.UTF8, "application/json"));

                if (!result.IsSuccessStatusCode)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Insert failed: : {0} {1} \n", (int)result.StatusCode, result.Content.ReadAsStringAsync().Result.ToString());
                    Console.ForegroundColor = ConsoleColor.Gray;
                }
                else
                {
                    Console.WriteLine(j.FullName);
                }
            }
        }


        static void FixJson()
        {
            DirectoryInfo currentDir2 = new DirectoryInfo(@"C:\Demos\KÖR 2 - Azure Search Suggestions\search-dotnet-asp-net-mvc-jobs-master\NYCJobsWeb\Schema_and_Data");
            System.IO.FileInfo[] zipCodes = currentDir2.GetFiles("zipcodes*.json");


            foreach (var z in zipCodes)
            {
                var json = File.ReadAllText(z.FullName);


                var r = JObject.Parse(json);
                var oldJson = from d in r["value"].Children()
                              select new
                              {
                                  zip_code = d["zip_code"].ToString(),
                                  country = d["country"].ToString(),
                                  city = d["city"].ToString(),
                                  state = d["state"].ToString(),
                                  state_ab = d["state_ab"].ToString(),
                                  county = d["county"].ToString(),
                                  longitud = d["geo_location"]["coordinates"].ElementAt(1),
                                  latidtud = d["geo_location"]["coordinates"].ElementAt(0),
                              };

                var newJson = from x in oldJson
                              select new ZipCode()
                              {
                                  zip_code = x.zip_code,
                                  country = x.country,
                                  city = x.city,
                                  state = x.state,
                                  state_ab = x.state_ab,
                                  county = x.county,
                                  geo_location = new Coordinate() { type = "Point", coordinates = new float[2] { (float)x.longitud, (float)x.latidtud } }
                              };


                string output = JsonConvert.SerializeObject(newJson);
                string newOutput = "{ \"value\" :" + output + "}";
                File.WriteAllText(z.FullName + ".new", newOutput);

                //JsonSerializer serializer = new JsonSerializer();
                //using (StreamWriter sw = new StreamWriter(z.FullName + ".new"))
                //using (JsonWriter writer = new JsonTextWriter(sw))
                //{
                //    serializer.Serialize(writer, newJson);
                //}

            }

        }
    }
}

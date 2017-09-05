using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace Client
{
    class Program
    {
        static HttpClient client = new HttpClient();
        private static string _authToken;
        private static Uri _batchStatusUri;
        private static string _preSignedUri;

        static void Main(string[] args)
        {
            client.BaseAddress = new Uri("https://azbvl23kkf.execute-api.eu-west-1.amazonaws.com/Dev/");
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            AuthAsync().Wait();

            CreateBatchAsync().Wait();

            UploadObject(_preSignedUri);
        }


        static async Task<HttpResponseMessage> AuthAsync()
        {
            HttpResponseMessage response = await client.PostAsJsonAsync("auth", new { username = "hello@ditto.ai", password = "qwerty123" });
            response.EnsureSuccessStatusCode();

            AuthToken token = (AuthToken)await response.Content.ReadAsAsync(typeof(AuthToken));

            if (token != null)
                _authToken = token.authToken;

            return response;
        }

        static async Task<Uri> CreateBatchAsync()
        {
            client.DefaultRequestHeaders.Add("authorization", _authToken);

            HttpResponseMessage response = 
                await client.PostAsJsonAsync("wastedata/batch",  new { accountId = "4559FA1B-B6E8-4717-BE0E-6879B022A834", totalRowsExpected = 100 });
            response.EnsureSuccessStatusCode();

            Batch batch = (Batch)await response.Content.ReadAsAsync(typeof(Batch));
            _preSignedUri = batch.uploadUrl;

            return _batchStatusUri = response.Headers.Location;
        }

        static void UploadObject(string url)
        {
            HttpWebRequest httpRequest = WebRequest.Create(url) as HttpWebRequest;
            httpRequest.Method = "PUT";

            using (Stream dataStream = httpRequest.GetRequestStream())
            {
                byte[] buffer = new byte[8000];

                string filePath = Path.GetFullPath("../../TestData/TestImport.xlsx");

                using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
                {
                    int bytesRead = 0;
                    while ((bytesRead = fileStream.Read(buffer, 0, buffer.Length)) > 0)
                    {
                        dataStream.Write(buffer, 0, bytesRead);
                    }
                }
            }

            HttpWebResponse response = httpRequest.GetResponse() as HttpWebResponse;
        }

    }

    public class AuthToken
    {
        public string authToken { get; set; }
    }

    public class Batch
    {
        public string batchId { get; set; }
        public string status { get; set; }
        public int maxRowsAccepted { get; set; }
        public string uploadUrl { get; set; }
    }
}

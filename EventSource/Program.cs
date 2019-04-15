using EventStore.ClientAPI;
using Newtonsoft.Json;
using System;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace EventSource
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            //
            //try
            //{
            //    var conn = EventStoreConnection.Create(new Uri("tcp://admin:changeit@localhost:1113"));

            //    conn.ConnectAsync().Wait();

            //    Parallel.For(0, 5, async i =>
            //    {
            //        var user = new User();
            //        user.Id = 1903;
            //        user.Name = "Tester";
            //        user.DateOfBirth = DateTime.Now;

            //        var eventData = new EventData(Guid.NewGuid(), "userCreated", true, Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user)), Encoding.UTF8.GetBytes("{Id:" + user.Id + "}"));

            //        string streamName = $"customerDomain-{user.Id}";

            //        await conn.AppendToStreamAsync(streamName, ExpectedVersion.Any, eventData); // NoStream
            //    });

            //    Console.WriteLine("Tamamlandı");
            //    Console.ReadKey();
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine(ex.Message);
            //

            {
                //const string STREAM = "a_test_stream";
                const int DEFAULTPORT = 1113;
                //uncomment to enable verbose logging in client.
                var settings = ConnectionSettings.Create();//.EnableVerboseLogging().UseConsoleLogger();
                using (var conn = EventStoreConnection.Create(settings, new IPEndPoint(IPAddress.Loopback, DEFAULTPORT)))
                {
                    conn.ConnectAsync().Wait();
                    for (var x = 0; x < 1; x++)
                    {
                        Parallel.For(0, 5, async i =>
                            {
                               var user = new User();
                               user.Id = 1903;
                               user.Name = "Tester";
                               user.DateOfBirth = DateTime.Now;

                               var eventData = new EventData(Guid.NewGuid(), "userCreated", true, Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user)), Encoding.UTF8.GetBytes("{Id:" + user.Id + "}"));

                               string streamName = $"customerDomain-{user.Id}";

                               await conn.AppendToStreamAsync(streamName, ExpectedVersion.Any, eventData); // NoStream
                            });

                        Console.WriteLine("Tamamlandı");
                        Console.ReadKey();
                        //conn.AppendToStreamAsync(STREAM,
                        //    ExpectedVersion.Any,
                        //    GetEventDataFor(x)).Wait();
                        //Console.WriteLine("event " + x + " written.");
                    }
                }
            }       
        }
        private static EventData GetEventDataFor(int i)
        {
            return new EventData(
                Guid.NewGuid(),
                "eventType",
                true,
                Encoding.ASCII.GetBytes("{'somedata' : " + i + "}"),
                Encoding.ASCII.GetBytes("{'metadata' : " + i + "}")
                );
        }
    }
}

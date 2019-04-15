using EventSource;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
//using EventStore.ClientAPI.SystemData;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Text;

namespace EventSourceConsumer
{
    internal class Program
    {
        private static void Main()
        {
            var subscription = new PersistentSubscriptionClient();
            subscription.Start();
        }
        //_eventStoreConnection.SubscribeToStreamAsync("$ce-customerDomain", true, (x, y) =>
        // {

        //     var hack = x;

        // });


        //ConcurrentDictionary<Guid, string> dict = new ConcurrentDictionary<Guid, string>();

        //var persistent = PersistentSubscriptionSettings.Create();
        //persistent.StartFromBeginning();
        //persistent.ResolveLinkTos();

        //var test = "customerDomainGroup";

        //_eventStoreConnection.CreatePersistentSubscriptionAsync("ce-customerDomain", test, persistent, null).Wait();

        //var subscription1 = _eventStoreConnection.ConnectToPersistentSubscriptionAsync("$ce-customerDomain", test,
        //                                            (sub, e) =>
        //                                            {
        //                                                if (dict.ContainsKey(e.Event.EventId))
        //                                                    Console.Write("appeared1 multiple " + e.Event.EventId);
        //                                                else
        //                                                {
        //                                                    dict.TryAdd(e.Event.EventId, "");

        //                                                    var data = Encoding.ASCII.GetString(e.Event.Data);
        //                                                    var obj = JsonConvert.DeserializeObject<User>(data);

        //                                                    Console.WriteLine("Received: " + e.Event.EventStreamId + ":" + e.Event.EventNumber);
        //                                                    Console.WriteLine(data);
        //                                                }
        //                                            },
        //                                            (sub, reason, ex) => { Console.WriteLine(ex.Message); });


        //var user = new EventSource.User();
        //string streamName = $"customerDomain-{user.Id}";

        //var readEvents = _eventStoreConnection.ReadStreamEventsForwardAsync(streamName, 0, 10, true).Result;
        //foreach (var evt in readEvents.Events)
        //    Console.WriteLine(Encoding.UTF8.GetString(evt.Event.Data));


        //PersistentSubscriptionSettings settings = PersistentSubscriptionSettings.Create()
        //.DoNotResolveLinkTos()
        //.StartFromCurrent();



        //_eventStoreConnection.CreatePersistentSubscriptionAsync(streamName, "examplegroup", settings, null).Wait();

        //_eventStoreConnection.ConnectToPersistentSubscription(streamName, "examplegroup", (_, x) =>
        //{
        //    var data = Encoding.ASCII.GetString(x.Event.Data);
        //    Console.WriteLine("Received: " + x.Event.EventStreamId + ":" + x.Event.EventNumber);
        //    Console.WriteLine(data);
        //}, (sub, reason, ex) => { }, null);

        //Console.WriteLine("waiting for events. press enter to exit");
        //Console.ReadLine();

        public class PersistentSubscriptionClient
        {
            private IEventStoreConnection _conn;
            private const string STREAM = "a_test_stream";
            private const string GROUP = "a_test_group";
            private const int DEFAULTPORT = 1113;
            private static readonly UserCredentials User = new UserCredentials("admin", "changeit");
            //private static readonly UserCredentials User = new UserCredentials("admin", "changeit");
            private EventStorePersistentSubscriptionBase _subscription;

            public void Start()
            {
                //uncommet to enable verbose logging in client.
                var settings = ConnectionSettings.Create(); //.EnableVerboseLogging().UseConsoleLogger();

                using (_conn = EventStoreConnection.Create(new Uri("tcp://admin:changeit@localhost:1113")))
                {
                    _conn.ConnectAsync().Wait();

                    CreateSubscription();
                    ConnectToSubscription();

                    Console.WriteLine("waiting for events. press enter to exit");
                    Console.ReadLine();
                }
            }

            private void ConnectToSubscription()
            {
                var bufferSize = 10;
                var autoAck = true;

                _subscription = _conn.ConnectToPersistentSubscription(STREAM, GROUP, EventAppeared, SubscriptionDropped,
                    User, bufferSize, autoAck);
            }

            private void SubscriptionDropped(EventStorePersistentSubscriptionBase eventStorePersistentSubscriptionBase,
    SubscriptionDropReason subscriptionDropReason, Exception ex)
            {
                ConnectToSubscription();
            }

            private static void EventAppeared(EventStorePersistentSubscriptionBase eventStorePersistentSubscriptionBase,
           ResolvedEvent resolvedEvent)
            {

                var data = Encoding.ASCII.GetString(resolvedEvent.Event.Data);
                Console.WriteLine("Received: " + resolvedEvent.Event.EventStreamId + ":" + resolvedEvent.Event.EventNumber);
                Console.WriteLine(data);
            }

            private void CreateSubscription()
            {
                PersistentSubscriptionSettings settings = PersistentSubscriptionSettings.Create()
                    .DoNotResolveLinkTos()
                    .StartFromCurrent();

                try
                {
                    _conn.CreatePersistentSubscriptionAsync(STREAM, GROUP, settings, User).Wait();
                }
                catch (AggregateException ex)
                {
                    if (ex.InnerException.GetType() != typeof(InvalidOperationException)
                        && ex.InnerException?.Message != $"Subscription group {GROUP} on stream {STREAM} already exists")
                    {
                        throw;
                    }
                }
            }

        }


        //var readResult = _eventStoreConnection.ReadEventAsync(streamName, 0, true).Result;
        //Console.WriteLine(Encoding.UTF8.GetString(readResult.Event.Value.Event.Data));


        //    Console.ReadKey();
    }



    
}

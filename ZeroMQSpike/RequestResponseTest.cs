using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;
using ZeroMQ;

namespace ZeroMQSpike
{
    public class RequestResponseTest
    {
        [Fact]
        public void should_receive_messages()
        {
            const int numberOfMessages = 100;
            const int numberOfClients = 5;

            const string port = "5555";
            var endpoint = $"tcp://*:{port}";

            var server = new Server(endpoint);

            var task = Task.Run(() => server.Listen(numberOfMessages * numberOfClients));

            Enumerable.Range(0, numberOfClients).ToList().AsParallel().ForAll(i =>
            {
                var client = new Client($"tcp://127.0.0.1:{port}");
                client.Send(numberOfMessages);
            });


            server.Received.Should().Be(numberOfMessages * numberOfClients);
        }
    }

    internal class Server
    {
        private readonly string _endpoint;

        internal Server(string endpoint)
        {
            _endpoint = endpoint;
        }

        internal int Received { get; private set; }

        internal void Listen(int numberOfMessages)
        {
            using (var context = new ZContext())
            using (var responder = new ZSocket(context, ZSocketType.REP))
            {
                responder.Bind(_endpoint);
                for (var i = 0; i < numberOfMessages; i++)
                {
                    using (responder.ReceiveFrame())
                    {
                        Received++;
                        responder.Send(new ZFrame("received"));
                    }
                }
            }
        }
    }

    internal class Client
    {
        private readonly string _endpoint;

        internal Client(string endpoint)
        {
            _endpoint = endpoint;
        }

        private int Sent { get; set; }

        internal void Send(int numberOfMessages)
        {
            using (var context = new ZContext())
            using (var requester = new ZSocket(context, ZSocketType.REQ))
            {
                requester.Connect(_endpoint);
                for (var i = 0; i < numberOfMessages; i++)
                {
                    requester.Send(new ZFrame("message"));
                    using(var frame = requester.ReceiveFrame())
                    {
                        Sent++;
                    }
                }
            }
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;
using ZeroMQ;

namespace ZeroMQSpike
{
    public class PubSubTest
    {
        [Fact]
        public void subscribers_should_receive_messages()
        {
            const string port = "5556";
            var endpoint = $"tcp://*:{port}";


            var subscriber = new Subscriber($"tcp://127.0.0.1:{port}", "subscription-C");
            Task.Run(() => subscriber.Receive(2));

            var publisher = new Publisher(endpoint);
            // see note on "the subscriber will always miss the first messages that the publisher sends"
            // http://zguide.zeromq.org/page:all#Getting-the-Message-Out
            Thread.Sleep(500);

            publisher.Publish("subscription-A", "message A1");
            publisher.Publish("subscription-B", "message B1");
            publisher.Publish("subscription-C", "message C1");
            publisher.Publish("subscription-A", "message A2");
            publisher.Publish("subscription-B", "message B2");
            publisher.Publish("subscription-C", "message C2");



            subscriber.Received.Should().BeEquivalentTo(new List<string>
            {
                "subscription-C message C1",
                "subscription-C message C2"
            });
        }
    }

    public class Subscriber : IDisposable
    {
        private readonly ZContext _context;
        private readonly ZSocket _socket;

        public Subscriber(string endpoint, string subscription)
        {
            _context = new ZContext();
            _socket = new ZSocket(_context, ZSocketType.SUB);
            _socket.Connect(endpoint);
            _socket.Subscribe(subscription);
        }

        public void Dispose()
        {
            _socket.Dispose();
            _context.Dispose();
        }

        public void Receive(int numberOfMessages)
        {
            for (int i = 0; i < numberOfMessages; i++)
            {
                using (var frame = _socket.ReceiveFrame())
                {
                    var message = frame.ReadString();
                    Received.Add(message);
                }
            }
        }

        public List<string> Received { get; } = new List<string>();
    }

    public class Publisher : IDisposable
    {
        private readonly string _endpoint;
        private ZContext _context;
        private ZSocket _socket;

        public Publisher(string endpoint)
        {
            _endpoint = endpoint;
            Connect();
        }

        private void Connect()
        {
            _context = new ZContext();
            _socket = new ZSocket(_context, ZSocketType.PUB);
            _socket.Bind(_endpoint);
        }

        public void Publish(string subscription, string message)
        {
            using(var frame = new ZFrame($"{subscription} {message}"))
            {
                _socket.Send(frame);
            }
        }

        public void Dispose()
        {
            Disconnect();
        }

        private void Disconnect()
        {
            _socket.Dispose();
            _context.Dispose();
        }
    }
}
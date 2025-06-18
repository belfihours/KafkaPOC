
using Confluent.Kafka;

const string topic = "purchases";

string[] users = { "simone", "riccardo", "maria", "jbernard", "htanaka", "awalther" };
string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };

var config = new ProducerConfig
{
    // User-specific properties that you must set
    BootstrapServers = "localhost:49694",
    // Fixed properties
    Acks = Acks.All
};

using var producer = new ProducerBuilder<string, string>(config).Build();

var numProduced = 0;
Random rnd = new();
const int numMessages = 20;
for (int i = 0; i < numMessages; i++)
{
    
    var user = users[rnd.Next(users.Length)];
    var item = items[rnd.Next(items.Length)];
    
    producer.Produce(topic, new Message<string, string> { Key = user, Value = item },
        (deliveryPort) =>
        {
            if (deliveryPort.Error.Code != ErrorCode.NoError)
            {
                Console.WriteLine($"Failed to deliver message: {deliveryPort.Error.Reason}");
            }
            else
            {
                Console.WriteLine($"Produced event to topic {topic}: key = {user}, value = {item}");
                numProduced++;
            }
        });
}

producer.Flush(TimeSpan.FromSeconds(10));
Console.WriteLine($"{numProduced} produced events to topic {topic}.");
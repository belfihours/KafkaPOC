

using Confluent.Kafka;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:49694",
    
    // Fixed properties
    GroupId = "dev-kafka-consumer",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

const string topic = "purchases";

CancellationTokenSource cts = new CancellationTokenSource();
Console.CancelKeyPress+= (_, e) =>
{
    e.Cancel = true;  // prevent the process from terminating.
    cts.Cancel();
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe(topic);
try
{
    while (true)
    {
        var cr = consumer.Consume(cts.Token);
        Console.WriteLine($"Consumed event from topic {topic}: key = {cr.Message.Key,-10} value = {cr.Message.Value}");
    }
}catch(OperationCanceledException)
{
    ;
}
finally
{
    consumer.Close();
}
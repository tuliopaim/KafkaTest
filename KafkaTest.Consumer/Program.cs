using Confluent.Kafka;

var conf = new ConsumerConfig
{
    GroupId = "test-consumer-group",
    BootstrapServers = "127.0.0.1:9092",
    AutoOffsetReset = AutoOffsetReset.Earliest,
};

using var consumer = new ConsumerBuilder<Ignore, string>(conf).Build();

consumer.Subscribe("topico_teste");

Console.WriteLine($"Consumidor online");

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var messageCount = 0;

try
{
    while (true)
    {
        try
        {
            var result = consumer.Consume(cts.Token);

            var message = $"Recebida '{result.Message.Value}' '" +
                          $"Partition: {result.Partition.Value} " +
                          $"| MessageNumber: {++messageCount}'\n";

            Console.WriteLine(message);
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Error occured: {e.Error.Reason}");
        }
    }
}
catch (OperationCanceledException)
{
    consumer.Close();
}
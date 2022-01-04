using Confluent.Kafka;

var config = new ProducerConfig { BootstrapServers = "127.0.0.1:9092" };

using var p = new ProducerBuilder<Null, string>(config).Build();

try
{
    while (true)
    {
        Console.WriteLine("Aguardando tecla...");
        Console.ReadKey();

        for (var count = 1; count <= 300; count++)
        {
            var message = "oi";

            var dr = await p.ProduceAsync(
                "topico_teste",
                new Message<Null, string> { Value = message });

            Console.WriteLine($"Publicada {message} "+ 
                              $"'Partition: {dr.Partition.Value} "+
                              $"| MessageNumber: {count}'\n");
        }
    }
}
catch (ProduceException<Null, string> e)
{
    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
}
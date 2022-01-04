using Confluent.Kafka;
using Confluent.Kafka.Admin;

using var adminClient = new AdminClientBuilder(new AdminClientConfig
{
    BootstrapServers = "127.0.0.1:9092",
}).Build();

await CriarTopico(adminClient, "topico_teste", numPartitions: 3);

static async Task CriarTopico(IAdminClient adminClient, string topicName, short replicationFactor = 1, int numPartitions = 1)
{
    try
    {
        Console.WriteLine($"Criando Tópico '{topicName}' " +
                          $"- ReplicationFactor: {replicationFactor} " +
                          $"| NumPartitions: {numPartitions}\n");

        await adminClient.CreateTopicsAsync(new TopicSpecification[]
        {
            new TopicSpecification
            {
                Name = topicName,
                ReplicationFactor = replicationFactor,
                NumPartitions = numPartitions
            }
        });

        Console.WriteLine($"Tópico '{topicName}' criado!");
    }
    catch (Exception)
    {
        Console.WriteLine("Falha ao criar o tópico!");
    }
}
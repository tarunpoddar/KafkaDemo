using Confluent.Kafka;
using Newtonsoft.Json;

var config = new ConsumerConfig
{
    GroupId = "test-consumer-group",
    BootstrapServers = "localhost:9092",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();

consumer.Subscribe("my_test_topic");

CancellationTokenSource tokenSource = new();

try
{
    while(true)
    {
        var resp = consumer.Consume(tokenSource.Token);
        if(resp != null && resp.Message !=null)
        {
            var employee = JsonConvert.DeserializeObject<Employee>
                (resp.Value);
            Console.WriteLine($"id: {employee.id}, name: {employee.name}");
        }
    }
}
catch (Exception)
{
	throw;
}

public record Employee(int id, string name);

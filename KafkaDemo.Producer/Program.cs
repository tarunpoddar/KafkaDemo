using Confluent.Kafka;
using Newtonsoft.Json;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092"
};

var producer = new ProducerBuilder<string, string>(config).Build();

try
{
    string? input = string.Empty;
    do
    {
        Console.Write("Enter the employee id: ");
        int id = Convert.ToInt32(Console.ReadLine());

        Console.Write("Enter the employee name: ");
        string name = Console.ReadLine()??"";

        var employee = new Employee(id, name);
        var response = producer.ProduceAsync("my_test_topic",
       new Message<string, string>
       {
           Key = JsonConvert.SerializeObject(employee.id),
           Value = JsonConvert.SerializeObject(employee)
       });
        Console.WriteLine("Do you want to send more message? Y/n");
        input = Console.ReadLine();
    } while (input != null && (input.Contains('y') || input.Contains('Y')));
}
catch (ProduceException<Null, string> ex)
{
    Console.WriteLine(ex.Message);
}

public record Employee(int id, string name);

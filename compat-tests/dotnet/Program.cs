// Basic scenarios with the official Hazelcast .NET client against a Hecaton member.
using Hazelcast;
using Hazelcast.Serialization.Compact;

var prefix = args.Length > 0 ? args[0] : "dotnet";
var options = new HazelcastOptionsBuilder()
    .With(o =>
    {
        o.ClusterName = "compat";
        o.Networking.Addresses.Add("127.0.0.1:5961");
        o.Serialization.Compact.AddSerializer(new OrderSerializer());
    })
    .Build();

await using var client = await HazelcastClientFactory.StartNewClientAsync(options);

var map = await client.GetMapAsync<string, int>(prefix + "-map");
var added = new TaskCompletionSource<string>();
// events of different keys may arrive in any order
await map.SubscribeAsync(events => events.EntryAdded((_, a) => added.TrySetResult(a.Key)), "a", includeValues: true);
await map.PutAsync("a", 1);
await map.PutAsync("b", 2);
Check(await map.GetAsync("a") == 1 && await map.GetSizeAsync() == 2, "map put/get/size");
Check(await added.Task.WaitAsync(TimeSpan.FromSeconds(30)) == "a", "entry listener");

var queue = await client.GetQueueAsync<string>(prefix + "-queue");
await queue.OfferAsync("first");
await queue.OfferAsync("second");
Check(await queue.PollAsync() == "first" && await queue.GetSizeAsync() == 1, "queue offer/poll");

var topic = await client.GetTopicAsync<string>(prefix + "-topic");
var received = new TaskCompletionSource<string>();
await topic.SubscribeAsync(events => events.Message((_, a) => received.TrySetResult(a.Payload)));
await topic.PublishAsync("hello");
Check(await received.Task.WaitAsync(TimeSpan.FromSeconds(30)) == "hello", "topic publish/listen");

var name = prefix + "_orders";
var orders = await client.GetMapAsync<long, Order>(name);
await orders.PutAsync(1, new Order(1, "alice", 10.5));
await orders.PutAsync(2, new Order(2, "bob", 20.0));
Check((await orders.GetAsync(2))!.Customer == "bob", "compact get");
await client.Sql.ExecuteCommandAsync($"CREATE OR REPLACE MAPPING {name} (id BIGINT, customer VARCHAR, amount DOUBLE) "
    + "TYPE IMap OPTIONS ('keyFormat' = 'bigint', 'valueFormat' = 'compact', 'valueCompactTypeName' = 'dotnet.Order')");
await using (var result = await client.Sql.ExecuteQueryAsync($"SELECT customer FROM {name} WHERE amount > ? ORDER BY id", new object[] { 15.0 }))
{
    var customers = new List<string>();
    await foreach (var row in result)
    {
        customers.Add(row.GetColumn<string>("customer"));
    }
    Check(customers.Count == 1 && customers[0] == "bob", "sql select");
}

Console.WriteLine("OK " + prefix);

static void Check(bool condition, string what)
{
    if (!condition)
    {
        throw new InvalidOperationException("check failed: " + what);
    }
}

record Order(long Id, string Customer, double Amount);

sealed class OrderSerializer : ICompactSerializer<Order>
{
    public string TypeName => "dotnet.Order";

    public Order Read(ICompactReader reader) =>
        new(reader.ReadInt64("id"), reader.ReadString("customer")!, reader.ReadFloat64("amount"));

    public void Write(ICompactWriter writer, Order value)
    {
        writer.WriteInt64("id", value.Id);
        writer.WriteString("customer", value.Customer);
        writer.WriteFloat64("amount", value.Amount);
    }
}

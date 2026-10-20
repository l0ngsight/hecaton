// Basic scenarios with the official Hazelcast Go client against a Hecaton member.
package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type Order struct {
	ID       int64
	Customer string
	Amount   float64
}

type OrderSerializer struct{}

func (OrderSerializer) Type() reflect.Type { return reflect.TypeOf(Order{}) }

func (OrderSerializer) TypeName() string { return "go.Order" }

func (OrderSerializer) Read(r serialization.CompactReader) interface{} {
	customer := r.ReadString("customer")
	return Order{ID: r.ReadInt64("id"), Customer: *customer, Amount: r.ReadFloat64("amount")}
}

func (OrderSerializer) Write(w serialization.CompactWriter, value interface{}) {
	o := value.(Order)
	w.WriteInt64("id", o.ID)
	w.WriteString("customer", &o.Customer)
	w.WriteFloat64("amount", o.Amount)
}

func check(condition bool, what string) {
	if !condition {
		panic("check failed: " + what)
	}
}

func must[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

func receive[T any](ch chan T, what string) T {
	select {
	case v := <-ch:
		return v
	case <-time.After(30 * time.Second):
		panic("timed out: " + what)
	}
}

func main() {
	prefix := "go"
	if len(os.Args) > 1 {
		prefix = os.Args[1]
	}
	ctx := context.Background()
	config := hazelcast.Config{}
	config.Cluster.Name = "compat"
	config.Cluster.Network.SetAddresses("127.0.0.1:5961")
	config.Serialization.Compact.SetSerializers(OrderSerializer{})
	client := must(hazelcast.StartNewClientWithConfig(ctx, config))
	defer client.Shutdown(ctx)

	m := must(client.GetMap(ctx, prefix+"-map"))
	added := make(chan interface{}, 10)
	listener := hazelcast.MapEntryListenerConfig{IncludeValue: true}
	listener.NotifyEntryAdded(true)
	// events of different keys may arrive in any order
	listener.Key = "a"
	must(m.AddEntryListener(ctx, listener, func(event *hazelcast.EntryNotified) { added <- event.Key }))
	must(m.Put(ctx, "a", int64(1)))
	must(m.Put(ctx, "b", int64(2)))
	check(must(m.Get(ctx, "a")) == int64(1) && must(m.Size(ctx)) == 2, "map put/get/size")
	check(receive(added, "entry listener") == "a", "entry listener")

	q := must(client.GetQueue(ctx, prefix+"-queue"))
	must(q.Add(ctx, "first"))
	must(q.Add(ctx, "second"))
	check(must(q.Poll(ctx)) == "first" && must(q.Size(ctx)) == 1, "queue offer/poll")

	topic := must(client.GetTopic(ctx, prefix+"-topic"))
	received := make(chan interface{}, 10)
	must(topic.AddMessageListener(ctx, func(event *hazelcast.MessagePublished) { received <- event.Value }))
	if err := topic.Publish(ctx, "hello"); err != nil {
		panic(err)
	}
	check(receive(received, "topic listener") == "hello", "topic publish/listen")

	name := prefix + "_orders"
	orders := must(client.GetMap(ctx, name))
	must(orders.Put(ctx, int64(1), Order{ID: 1, Customer: "alice", Amount: 10.5}))
	must(orders.Put(ctx, int64(2), Order{ID: 2, Customer: "bob", Amount: 20.0}))
	check(must(orders.Get(ctx, int64(2))).(Order).Customer == "bob", "compact get")
	must(client.SQL().Execute(ctx, fmt.Sprintf("CREATE OR REPLACE MAPPING %s (id BIGINT, customer VARCHAR, amount DOUBLE) "+
		"TYPE IMap OPTIONS ('keyFormat' = 'bigint', 'valueFormat' = 'compact', 'valueCompactTypeName' = 'go.Order')", name))).Close()
	result := must(client.SQL().Execute(ctx, fmt.Sprintf("SELECT customer FROM %s WHERE amount > ? ORDER BY id", name), 15.0))
	defer result.Close()
	it := must(result.Iterator())
	var customers []interface{}
	for it.HasNext() {
		row := must(it.Next())
		customers = append(customers, must(row.Get(0)))
	}
	check(len(customers) == 1 && customers[0] == "bob", "sql select")
	fmt.Println("OK " + prefix)
}

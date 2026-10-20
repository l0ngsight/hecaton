"""Basic scenarios with the official Hazelcast Python client against a Hecaton member."""
import queue
import sys

import hazelcast
from hazelcast.serialization.api import CompactReader, CompactSerializer, CompactWriter


class Order:
    def __init__(self, id, customer, amount):
        self.id = id
        self.customer = customer
        self.amount = amount


class OrderSerializer(CompactSerializer[Order]):
    def read(self, reader: CompactReader) -> Order:
        return Order(reader.read_int64("id"), reader.read_string("customer"), reader.read_float64("amount"))

    def write(self, writer: CompactWriter, obj: Order) -> None:
        writer.write_int64("id", obj.id)
        writer.write_string("customer", obj.customer)
        writer.write_float64("amount", obj.amount)

    def get_type_name(self) -> str:
        return "python.Order"

    def get_class(self):
        return Order


def check(condition, what):
    if not condition:
        raise AssertionError(what)


def main(prefix):
    client = hazelcast.HazelcastClient(cluster_name="compat", cluster_members=["127.0.0.1:5961"],
                                       compact_serializers=[OrderSerializer()])
    try:
        m = client.get_map(prefix + "-map").blocking()
        added = queue.Queue()
        # events of different keys may arrive in any order
        m.add_entry_listener(include_value=True, key="a", added_func=lambda event: added.put(event.key))
        m.put("a", 1)
        m.put("b", 2)
        check(m.get("a") == 1 and m.size() == 2, "map put/get/size")
        check(added.get(timeout=30) == "a", "entry listener")

        q = client.get_queue(prefix + "-queue").blocking()
        q.offer("first")
        q.offer("second")
        check(q.poll() == "first" and q.size() == 1, "queue offer/poll")

        topic = client.get_topic(prefix + "-topic").blocking()
        received = queue.Queue()
        topic.add_listener(lambda message: received.put(message.message))
        topic.publish("hello")
        check(received.get(timeout=30) == "hello", "topic publish/listen")

        name = prefix + "_orders"
        orders = client.get_map(name).blocking()
        orders.put(1, Order(1, "alice", 10.5))
        orders.put(2, Order(2, "bob", 20.0))
        check(orders.get(2).customer == "bob", "compact get")
        client.sql.execute(
            f"CREATE OR REPLACE MAPPING {name} (id BIGINT, customer VARCHAR, amount DOUBLE) TYPE IMap "
            "OPTIONS ('keyFormat' = 'bigint', 'valueFormat' = 'compact', 'valueCompactTypeName' = 'python.Order')"
        ).result()
        result = client.sql.execute(f"SELECT customer FROM {name} WHERE amount > ? ORDER BY id", 15.0).result()
        check([row["customer"] for row in result] == ["bob"], "sql select")
        print("OK " + prefix)
    finally:
        client.shutdown()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "python")

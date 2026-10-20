'use strict';
// Basic scenarios with the official Hazelcast Node.js client against a Hecaton member.
const { Client } = require('hazelcast-client');
const Long = require('long');

class Order {
    constructor(id, customer, amount) {
        this.id = id;
        this.customer = customer;
        this.amount = amount;
    }
}

class OrderSerializer {
    getClass() {
        return Order;
    }

    getTypeName() {
        return 'nodejs.Order';
    }

    read(reader) {
        return new Order(reader.readInt64('id'), reader.readString('customer'), reader.readFloat64('amount'));
    }

    write(writer, order) {
        writer.writeInt64('id', order.id);
        writer.writeString('customer', order.customer);
        writer.writeFloat64('amount', order.amount);
    }
}

function check(condition, what) {
    if (!condition) {
        throw new Error('check failed: ' + what);
    }
}

function waitFor(executor, what) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('timed out: ' + what)), 30000);
        executor((value) => {
            clearTimeout(timer);
            resolve(value);
        });
    });
}

async function main(prefix) {
    const client = await Client.newHazelcastClient({
        clusterName: 'compat',
        network: { clusterMembers: ['127.0.0.1:5961'] },
        serialization: { compact: { serializers: [new OrderSerializer()] } },
    });
    try {
        const map = await client.getMap(prefix + '-map');
        let onAdded;
        const added = waitFor((resolve) => { onAdded = resolve; }, 'entry listener');
        // events of different keys may arrive in any order
        await map.addEntryListener({ added: (event) => onAdded(event.key) }, 'a', true);
        await map.put('a', 1);
        await map.put('b', 2);
        check(await map.get('a') === 1 && await map.size() === 2, 'map put/get/size');
        check(await added === 'a', 'entry listener');

        const queue = await client.getQueue(prefix + '-queue');
        await queue.offer('first');
        await queue.offer('second');
        check(await queue.poll() === 'first' && await queue.size() === 1, 'queue offer/poll');

        // the Node.js client offers reliable topics only
        const topic = await client.getReliableTopic(prefix + '-topic');
        let onMessage;
        const received = waitFor((resolve) => { onMessage = resolve; }, 'topic listener');
        topic.addMessageListener((message) => onMessage(message.messageObject));
        await topic.publish('hello');
        check(await received === 'hello', 'topic publish/listen');

        const name = prefix + '_orders';
        const orders = await client.getMap(name);
        await orders.put(Long.fromNumber(1), new Order(Long.fromNumber(1), 'alice', 10.5));
        await orders.put(Long.fromNumber(2), new Order(Long.fromNumber(2), 'bob', 20.0));
        check((await orders.get(Long.fromNumber(2))).customer === 'bob', 'compact get');
        await client.getSql().execute(`CREATE OR REPLACE MAPPING ${name} (id BIGINT, customer VARCHAR, amount DOUBLE) `
            + "TYPE IMap OPTIONS ('keyFormat' = 'bigint', 'valueFormat' = 'compact', 'valueCompactTypeName' = 'nodejs.Order')");
        const result = await client.getSql().execute(`SELECT customer FROM ${name} WHERE amount > ? ORDER BY id`, [15.5]);
        const customers = [];
        for await (const row of result) {
            customers.push(row['customer']);
        }
        check(customers.length === 1 && customers[0] === 'bob', 'sql select');
        console.log('OK ' + prefix);
    } finally {
        await client.shutdown();
    }
}

main(process.argv[2] || 'nodejs').catch((error) => {
    console.error(error);
    process.exit(1);
});

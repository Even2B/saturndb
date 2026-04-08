const { SaturnDBClient } = require("./saturn");

async function main() {
    const client = new SaturnDBClient("127.0.0.1", 7379, "saturn-admin-secret");
    await client.connect();
    console.log("connected to saturn");

    console.log("ping →", await client.ping());

    await client.watch("users:*", ({ stream, payload }) => {
        console.log(`[watch] ${stream} →`, payload);
    });

    await client.emit("users:1", { name: "evan", status: "online" });
    await client.emit("users:2", { name: "john", status: "away" });
    await client.emit("orders:1", { total: 99 }); // should NOT trigger watch

    const val = await client.get("users:1");
    console.log("get users:1 →", val);

    await sleep(200);
    client.disconnect();
}

function sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
}

main().catch(console.error);

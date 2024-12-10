import asyncio
from asyncua import Client, ua

# Define a class for handling subscription events
class SubHandler:
    async def datachange_notification(self, node, val, data):
        print(f"New value for node {node} is {val}")

async def main():
    url = "opc.tcp://0.0.0.0:4840/achu/svTemporal"  # Change this to the URL of your server
    client = Client(url)

    subscription = None  # Initialize subscription to None

    try:
        await client.connect()
        print("Connected to the OPC UA server")

        node_to_monitor = client.get_node("ns=2;s=MyVariable")  # Change to your node's identifier
        
        handler = SubHandler()
        subscription = await client.create_subscription(500, handler)
        monitored_item = await subscription.subscribe_data_change(node_to_monitor)

        print("Monitoring started. Press Ctrl+C to stop.")
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            print("Monitoring stopped by user")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if subscription:
            await subscription.unsubscribe(monitored_item)
            await subscription.delete()
        await client.disconnect()
        print("Disconnected from the OPC UA server")

if __name__ == "__main__":
    asyncio.run(main())
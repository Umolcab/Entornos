from asyncua import Server
import asyncio

async def run_discovery_server():
    server = Server()
    server.set_endpoint("opc.tcp://localhost:4848/discovery")
    server.set_server_name("DiscoveryServer")
    
    # Configurar como servidor de descubrimiento,
    # servidor central que permite a los clientes localizar otros servidores OPC UA disponibles, 
    # sus endpoints, y características de conexión.
    server.is_discovery_server = True  

# El servidor ha de estar en constante ejecución para que siempre sean visibles (y se sepa que estén activos)
# todos los servidores
    print("Discovery Server iniciado en opc.tcp://localhost:4848/discovery")
    async with server:
        await asyncio.Future()  

if __name__ == "__main__":
    asyncio.run(run_discovery_server())

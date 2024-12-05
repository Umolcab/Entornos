import time
from asyncua.sync import Client

if __name__ == "__main__":
	
	try:
		
		url_client = "opc.tcp://localhost:4840/achu/horas"
		nodeId = "ns=2;i=2"

		client = Client(url_client)
		client.connect()

		node = client.get_node(nodeId)
		value_client = node.get_value().replace(tzinfo=None) ## elimina el +00:00 de la cadena(zona horaria)


	finally:
		client.disconnect()
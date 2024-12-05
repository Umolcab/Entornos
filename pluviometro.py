from datetime import datetime
import time
import openpyxl

from asyncua.sync import Server
from asyncua.sync import Client

if __name__ == "__main__":
	servidor= Server()
	servidor.set_endpoint(("opc.tcp://localhost:4841/achu/Pluviometro"))
	uri="http://www.achu.es/pluviometro"
	idx=servidor.register_namespace(uri)
	mi_obj=servidor.nodes.objects.add_object(idx,"pluviomentro")
	mi_hora = mi_obj.add_variable(idx, "Hora", datetime.now())
	mi_lluv = mi_obj.add_variable(idx, "Lluvia", 0.0)
	mi_hora.set_writable()
	mi_lluv.set_writable()
	servidor.start()

#-----------------------------------------------
#Conexion con el server de horas
	url_client = "opc.tcp://localhost:4840/achu/horas"
	nodeId = "ns=2;i=2"

	client = Client(url_client)
	client.connect()

	node = client.get_node(nodeId)
	value_client = node.get_value().replace(tzinfo=None) ## elimina el +00:00 de la cadena(zona horaria)

#-----------------------------------------------

	workbook = openpyxl.load_workbook('PluviometroChiva_29octubre2024.xlsx') 
	sheet = workbook.active

	data = [] 
	for row in sheet.iter_rows(values_only=True):
		data.append(row)

	try:
		for row in data: 
			for value in row: 
				if isinstance(value, (datetime)):
					print(f"ServerH {value_client}")
					print(f"Fecha {value}")
					#Espera la hora del server de horas y la local sean iguales
					while value != value_client:
						time.sleep(0.5)
						#Actualiza hora del server de horas
						value_client = node.get_value().replace(tzinfo=None)
						print(f"**ServerH {value_client}")
					print(f"ServerH {value_client}")
					mi_hora.write_value(value)
					print(f"Fecha {value}")
				if isinstance(value, (int, float)): 
					mi_lluv.write_value(float(value))
					print(f"Valor {float(value)} de tipo {type(value)}")
					print("-------------------------------------------")
			time.sleep(0.5)

		'''
		contador = 0
		while True:
			time.sleep(1)
			contador=mi_var.read_value()
			contador+=0.1
			mi_var.write_value(contador)'''
	finally:
	    servidor.stop()
		
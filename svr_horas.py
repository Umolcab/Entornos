import time
from datetime import datetime, timedelta
from asyncua.sync import Server

if __name__ == "__main__":

	servidor = Server()
	servidor.set_endpoint(("opc.tcp://0.0.0.0:4840/achu/horas"))
	uri = "http://www.achu.es/horas"
	idx = servidor.register_namespace(uri)
	#Creación del objeto del servidor de horas
	#obj_horas = servidor.nodes.objects.add_object(idx,"srvHoras")
	#Generación de la fecha y hora inicial con el formato Año-Mes-Día Hora-Minuto-Segundo
	hora_ini = datetime(2024, 10, 28, 10, 25, 00)
	#Creación de la variable inicializada
	#var_horas = obj_horas.add_variable(idx, "Hora", hora_ini)
	#Poner la variable como editable
	#var_horas.set_writable()
	servidor.import_xml("nodes.xml")
	obj = servidor.nodes.objects.add_object(idx, "pluv", "ns=1;i=2001")
	servidor.start()
	print("Servidor arrancado")

	try:
		hora = hora_ini
		#Bucle que se encarga de ir incrementado la hora en 5 minutos y las publica cada segundo. 
		while True:
			hora += timedelta(minutes=5.0)
			#var_horas.write_value(hora)
			time.sleep(1)
			#Si la hora es la última que aparece en los datos finaliza el servidor
			if hora == datetime(2024, 11, 1, 10, 25, 00):
				break

	finally:
		servidor.stop()
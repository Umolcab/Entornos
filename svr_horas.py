import time
from datetime import datetime, timedelta
from asyncua.sync import Server

#Función que se encarga de crear un servidor
def crearSrv():
	servidor = Server()
	servidor.set_endpoint(("opc.tcp://localhost:4840/achu/svTemporal"))
	return servidor

#-------------------------------------------------------------------------
#Función que se encarga de darle un namespace
def setNamespace(servidor):
	uri="http://www.achu.es/srvtemporal"
	idx=servidor.register_namespace(uri)
	return idx

#-------------------------------------------------------------------------
#Función que se encarga de crear un objeto del tipo cargado desde el .xml
def nuevoObj(servidor, idx, nombre, tipo):
	return servidor.nodes.objects.add_object(idx,nombre,tipo)

#-------------------------------------------------------------------------
#Función que se encarga de iniciar el servicio de horas
def iniciarServicioHoras():
	#Asignación de las variables para la creación del servidor
	nombre = "objTemporal"
	tipo = "ns=1;i=2001"
	#Creación del servidor
	servidor = crearSrv()
	#Asignaciónd el namespace
	idx = setNamespace(servidor)
	#Carga de los nodos desde el .xml
	servidor.import_xml("nodes.xml")
	#Nodo del objeto para poder acceder a las variables
	obj = nuevoObj(servidor, idx, nombre, tipo)
	#Nodo de la variable del objeto
	tiempo = obj.get_child("1:Fecha_y_hora")
	#Hora de inicio de los datos
	hora_ini = datetime(2024, 10, 28, 23, 00, 00)
	servidor.start()
	try:
		hora = hora_ini
		#Bucle que se encarga de ir incrementado la hora en 5 minutos y las publica cada segundo. 
		while True:
			hora += timedelta(minutes=5.0)
			tiempo.write_value(hora)
			time.sleep(0.3)
			#Si la hora es la última que aparece en los datos finaliza el servidor
			if hora == datetime(2024, 11, 1, 10, 25, 00):
				break

	finally:
		servidor.stop()

if __name__ == "__main__":
	iniciarServicioHoras()
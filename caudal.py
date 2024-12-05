import time
import pandas as pd
from asyncua.sync import Server
from asyncua.sync import Client
from datetime import datetime



##METODOS PARA CONECTARSE CON EL CLIENTE - OPTIMIZADO
def conectar_cliente():
	# Creamos la conexion al cliente
	cliente_horas = Client("opc.tcp://0.0.0.0:4840/achu/horas")
	#Realizamos la conexion
	cliente_horas.connect()
	return cliente_horas

def obtener_nodo(cliente):
	#Nodo de las horas
	node_id = "ns=2;i=2"
	#Obtener el nodo
	node = cliente.get_node(node_id)
	return node,node_id

def configurar_cliente():
	cliente_horas = conectar_cliente()
	node, nodo_id = obtener_nodo(cliente_horas)
	return cliente_horas,node,nodo_id


#METODOS PARA CREAR EL SERVER
def crear_server():
	servidor= Server()
	servidor.set_endpoint(("opc.tcp://0.0.0.0:4842/achu/embalse"))
	return servidor

def anadir_namespace(servidor):
	uri="http://www.achu.es/embalse"
	idx=servidor.register_namespace(uri)
	return idx

def anadir_tipo(servidor,idx,tipo):
	return servidor.nodes.base_object_type.add_object_type(idx,tipo)

def anadir_objeto(servidor,idx,nombre,tipo):
	return servidor.nodes.objects.add_object(idx,nombre,tipo)

def anadir_variable(mi_obj,idx,nombre,tipo):
	var = mi_obj.add_variable(idx, nombre, tipo)
	var.set_writable()
	return var

def conectar_server():
	servidor = crear_server()

	idx = anadir_namespace(servidor)
	
	tipo = anadir_tipo(servidor,idx,"Caudalimetro")
	print(tipo)
	tipo.add_object(idx,"Caudalimetro")
	tipo.add_property(idx,"Units","m3/s")

	#Anadir propiedades al object type- trabajo para seguir
	
	mi_obj = anadir_objeto(servidor,idx,"Poyo",tipo)
	#mi_obj.add_property(idx, "Unidad", "m3/s").set_modelling_rule(True)
	
	caudal = anadir_variable(mi_obj,idx,"caudal",0.0)
	caudal.add_property(idx,"Units","m3/s")
	caudal.add_property(idx,"Estado","Funcionando")

	hora = anadir_variable(mi_obj,idx,"fecha",datetime.now())

	estado = anadir_variable(mi_obj,idx,"estado","estado")
	return servidor,idx,mi_obj,caudal,hora,estado

#Metodo para cargar el CSV
def cargar_csv():
	#Cargar excel en codigo y transformarlo a diccionario
	file = pd.read_csv("poyo.csv")
	file = dict(file)
	return file

def Proceso():
	#Creamos y arrancamos sel server 
	#servidor,idx,mi_obj,caudal,hora,estado = conectar_server()
	servidor = crear_server()
	servidor.import_xml("nodes.xml")
	idx = anadir_namespace(servidor)
	
	types = servidor.get_node()
	obj = servidor.nodes.objects.add_object(idx,"Embalse","Caudalimetro")
	servidor.start()

	'''
	hora = servidor.get_node("ns=1;i=2004")
	estado = servidor.get_node("ns=1;i=2003")
	caudal = servidor.get_node("ns=1;i=2002")
	'''
	
	#Configuramos la conexion con el cliente y obtenemos el nodo y el nodo id
	cliente_horas,node,node_id = configurar_cliente()

	#Cargar excel en codigo y lo transformamos a diccionario para acceder mas rapido
	file = cargar_csv()
	
	#Empezamos el proceso
	try:
		#Bucle donde comprobamos los valores del cliente constantemente y sincronizamos con nuestro
		# excel
		while True:
			#Obtenemos el valor del cliente
			value_client = node.get_value()	

			#Buscamos el valor dentro del diccionario que hemos cargado anteriormente
			for x in range(len(file["Fecha"])):
				#Cuando la fecha coincide con el valor del cliente escribimos nuestros valores
				if file["Fecha"][x]== str(value_client.replace(tzinfo=None)): 
					try:
						#Escribimos la hora
						hora.write_value(datetime.strptime(file["Fecha"][x], "%Y-%m-%d %H:%M:%S"))
						#Escribimos el estado actual del poyo
						estado.write_value(str(file["Estado"][x]))

						#Si el estado que leemos de esa hora es un fallo, escribimos 0.0 en caudal
						#Y imprimimimos estado fallido
						#Si el estado no es fallo, escribimos en la variable el caudal leido
						if file["Estado"][x] == "FALLO":
							caudal.write_value(0.0)
							print(file["Fecha"][x],str(file["Estado"][x]),"0.0")
							#caudal.get_child(["2:Estado"]).write_value("Fallido")
						else:
							caudal.write_value(float(file["Caudal"][x].replace(',','.')))
							print(file["Fecha"][x],str(file["Estado"][x]),float(file["Caudal"][x].replace(',','.')))
							#caudal.get_child(["2:Estado"]).write_value("Activo")
					except Exception as e:
						#Si surge algun error, que nos lo imprima y nos imprima en la fecha y el estado donde se ha quedado
						print(e)
						print(file["Fecha"][x],str(file["Estado"][x]),"0.0" )
						
			time.sleep(1.0)
			
			

	finally:
		servidor.stop()
		

if __name__ == "__main__":
	Proceso()
	
		
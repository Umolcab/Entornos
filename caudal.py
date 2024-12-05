import time
import pandas as pd
from asyncua.sync import Server
from asyncua.sync import Client
from datetime import datetime

#Autor: Carlos Mira Lopez

#---- Metodos de conexion cliente ------#

#Metodo al que le pasamos la URL del cliente para establecer la conexion
def conectar_cliente(url):
	# Creamos la conexion al cliente
	cliente_horas = Client(url)
	#Realizamos la conexion
	cliente_horas.connect()
	return cliente_horas

#Metodo que hace la configuracion final, la conexion con el cliene
# y obtener el nodo para poder acceder a la variable de las horas del server
def configurar_cliente(url,node_id):
	cliente_horas = conectar_cliente(url)
	node = cliente_horas.get_node(node_id)
	#node, nodo_id = obtener_nodo(cliente_horas,"ns=2;i=2")
	return node


'''
def obtener_nodo(cliente,node_id):
	#Obtener el nodo
	pass
	#return node,node_id

def configurar_cliente(url,node_id):
	cliente_horas = conectar_cliente(url)
	node = cliente_horas.get_node(node_id)
	node, nodo_id = obtener_nodo(cliente_horas,"ns=2;i=2")
	return cliente_horas,node,nodo_id

'''



#---- Metodos de creación del server ------#

#Este metodo crear un servidor en la url pasada a la funcion, e importa
# el tipo de datos Caudalimetro que le pasamos en la ruta
def crear_server(url,ruta_xml):
	servidor= Server()
	servidor.set_endpoint((url))
	servidor.import_xml(ruta_xml)
	idx = crear_namespace(servidor)
	return servidor,idx

#Este metodo crea el objeto, del tipo importado en el xml y nos lo devuelve
def crear_objeto(servidor, idx):
	my_object_type = servidor.get_node("ns=1;i=2001") 
	my_obj = servidor.nodes.objects.add_object(idx, "Embalse", my_object_type)
	return my_obj

#Este metodo nos registra un namspace con una URI personal
def crear_namespace(servidor):
	uri="http://www.achu.es/embalse"
	idx=servidor.register_namespace(uri)
	return idx


'''
def anadir_tipo(servidor,idx,tipo):
	return servidor.nodes.base_object_type.add_object_type(idx,tipo)

def anadir_objeto(servidor,idx,nombre,tipo):
	return servidor.nodes.objects.add_object(idx,nombre,tipo)

def anadir_variable(mi_obj,idx,nombre,tipo):
	var = mi_obj.add_variable(idx, nombre, tipo)
	var.set_writable()
	return var

def conectar_server(url, ruta_xml):
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
'''

#Metodo para cargar el CSV
def cargar_csv():
	#Cargar excel en codigo y transformarlo a diccionario
	file = pd.read_csv("poyo.csv")
	file = dict(file)
	return file

def Proceso():
	#Creamos y arrancamos sel server 
	#servidor,idx,mi_obj,caudal,hora,estado = conectar_server()
	
	
	#Creamos el servidor y lo arrancamos
	print("Configurando servidor")
	#Creamos el servidor
	servidor,idx = crear_server("opc.tcp://0.0.0.0:4842/achu/embalse","nodes_caudal.xml")
	#Creamos el objeto
	my_obj = crear_objeto(servidor,idx)
	#Arrancamos el servidor
	servidor.start()
	print("Servidor arrancado correctamente")


	'''
	hora = servidor.get_node("ns=1;i=2004")
	estado = servidor.get_node("ns=1;i=2003")
	caudal = servidor.get_node("ns=1;i=2002")
	'''
	
	#Configuramos la conexion con el cliente y obtenemos el nodo y el nodo id
	#cliente_horas,node,node_id = configurar_cliente("opc.tcp://0.0.0.0:4840/achu/horas","ns=2;i=2")
	node = configurar_cliente("opc.tcp://0.0.0.0:4840/achu/horas","ns=2;i=2")
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
	
		
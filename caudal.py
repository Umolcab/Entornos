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
	try:
		cliente_horas = Client(url) #Iniciamos la conexion con el cliente a la URL que le pasamos
		cliente_horas.connect() #Realizamos la conexion
		return cliente_horas
	except:
		print("Problemas con la conexion del cliente")
	
#Metodo que hace la configuracion final, la conexion con el cliene
# y obtener el nodo para poder acceder a la variable de las horas del server
def configurar_cliente(url,node_id):
	cliente_horas = conectar_cliente(url) #Creamos la conexion con el cliente
	node = cliente_horas.get_node(node_id) #Obtenemos el nodo de las horas que nos da el cliente
	return cliente_horas,node


#---- Metodos de creación del server ------#

#Este metodo crear un servidor en la url pasada a la funcion, e importa
# el tipo de datos Caudalimetro que le pasamos en la ruta
def crear_server(url,ruta_xml,uri):
	servidor= Server()
	servidor.set_endpoint((url)) #Creamos el server en la URL correspondiente
	servidor.import_xml(ruta_xml) #Importamos el XML que contiene el tipo de objeto "Caudalimetro"
	idx = crear_namespace(servidor,uri) #Creamos el namespace con la URI
	return servidor,idx

#Este metodo crea el objeto, del tipo importado en el xml y nos lo devuelve
def crear_objeto(servidor, idx,node_type):
	my_object_type = servidor.get_node(node_type) #Obtenemos el tipo "Caudalimetro"
	my_obj = servidor.nodes.objects.add_object(idx, "Embalse", my_object_type) #Creamos un objeto del tipo "Caudalimetro"
	return my_obj

#Este metodo nos registra un namspace con una URI personal
def crear_namespace(servidor,uri):
	return servidor.register_namespace(uri) #Creamos el namespace con la URI

#Este metodo guarda el objeto, y sus propiedades en un diccionario
def buscar_objeto(my_obj,id_obj,id_est,id_fec):
	caudal = my_obj.get_child(id_obj) #Buscamos la variable "Caudalimetro" a la cual le pondremos los valores
	estado = caudal.get_child(id_est) #Buscamos la propiedad del "Caudalimetro" que nos indica el estado
	fecha = caudal.get_child(id_fec) #Buscamos la propiedad del "Caudalimetro" que nos indica la fecha
	return {"Caudal":caudal,"Estado":estado,"Fecha":fecha} #Devolvemos un diccionario con las variables


#---- Metodo de cargar el excel ------#
#Metodo para cargar el CSV, donde lo transformamos tambien a tipo diccionario
def cargar_csv():
	file = pd.read_csv("poyo.csv") #Cargamos el CSV en el codigo y lo guardamos en la variable
	file = dict(file) #Convertimos lo que hemos leido en diccionario
	return file


#------Proceso ---#
#Proceso de buscar en el diccionario, la fecha para escribir en las varaibles correspondientes
# el valor de ese momento del excel
def Proceso(file,var,value_client):
	#Buscamos el valor dentro del diccionario que hemos cargado anteriormente
	for x in range(len(file["Fecha"])):
		#Cuando la fecha coincide con el valor del cliente escribimos nuestros valores
		if file["Fecha"][x]== str(value_client.replace(tzinfo=None)): 
			try:	
				var["Fecha"].write_value(datetime.strptime(file["Fecha"][x], "%Y-%m-%d %H:%M:%S").replace(tzinfo=None)) #Escribimos la hora obtenida del excel
				#Si el estado que leemos de esa hora es un fallo, escribimos:
				#	-1.0 en caudal
				#	"FALLO" en estado
				#Si el estado no es fallo, escribimos en la variable el caudal leido, y "Funcionando" en Estado
				if file["Estado"][x] == "FALLO":		
					var["Estado"].write_value(str(file["Estado"][x])) #Escribimos que el estado esta en FALLO
					var["Caudal"].write_value(-1.0) #Escribimos en el caudal el valor -1.0 indicando fallo
					print(file["Fecha"][x],str(file["Estado"][x]))
				else:
					var["Estado"].write_value("Funcionando") #Escribimos que el Caudalimetro esta funcionando
					var["Caudal"].write_value(float(file["Caudal"][x].replace(',','.'))) #Escribimos el valor del caudal obtenido del excel
					print(file["Fecha"][x],str(file["Estado"][x]),float(file["Caudal"][x].replace(',','.')))
			except Exception as e:
				print("Error: ", e) #Si surge algun error, que nos lo imprima 
						

#---- Lanzamiento del conjunto ------#
def launch():
	#Variables
	urlSer = "opc.tcp://0.0.0.0:4842/achu/embalse" #URL del servidor 
	urlCli = "opc.tcp://0.0.0.0:4840/achu/horas" #URL del cliene
	uri="http://www.achu.es/embalse" #URI del servidor
	rutXlm = "nodes_caudal.xml" #Si se encuentra en la misma ruta que el codigo se queda igual
	id_type = "ns=1;i=2001" #Id del tipo de objeto "Caudalimetro"
	id_obj = "2:Caudalimetro" #Nombre del objeto a buscar creado
	id_est = "2:Estado" #Nombre de la propiedad Estado del objeto Caudalimetro
	id_fec = "2:Fecha" #Nombre de la propiedad Fecha del objeto Caudalimetro
	id_nodoCli = "ns=2;i=2" #Id del nodo de las horas que es el que queremos del cliente
	
	#CONFIGURAMOS SERVER
	#Creamos el servidor, objeto y arrancamos el server
	print("Configurando servidor")
	servidor,idx = crear_server(urlSer,rutXlm,uri)
	my_obj = crear_objeto(servidor,idx,id_type)
	servidor.start()
	print("Servidor arrancado correctamente")

	#CONFIGURAMOS VARIABLES : Caudal, Estado, Fecha
	var = {}
	var = buscar_objeto(my_obj,id_obj,id_est,id_fec)
	
	#CONFIGURAMOS CLIENTE
	#Realizamos la conexion con el cliente a la URL que le pasamos, y obtenemos el nodo de las horas
	cliente,node = configurar_cliente(urlCli,id_nodoCli)
	
	#CONFIGURACION DEL EXCEL
	#Cargar excel en el codigo y lo transformamos a diccionario para acceder mas rapido
	file = cargar_csv()
	
	#Empezamos el proceso
	try:
		#Bucle donde comprobamos los valores del cliente constantemente y sincronizamos con nuestro
		# excel
		while cliente:
			#Obtenemos el valor de la fecha que nos proporciona el cliente
			value_client = node.get_value()	
			#Ejecutamos el proceso de buscar la fecha que nos sirve el cliente, para guardar en 
			# las variables correspondiente el valor de caudal, y estado del caudalimetro en ese momento
			Proceso(file,var,value_client)
			#print(file["Fecha"][str(value_client.replace(tzinfo=None))])			
			time.sleep(1.0)
			
	finally:
		servidor.stop()
		

if __name__ == "__main__":
	launch()
	
		
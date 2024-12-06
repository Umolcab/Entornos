from datetime import datetime
import time
import openpyxl

from asyncua.sync import Server, ua
from asyncua.sync import Client

#Author: Alejandro Rueda Plaza

#----------------------------------------------------------------------------------------------------------------------------
# A partir de una url, realiza una conecxion, devolviendo el 
# objecto Client en caso de conectar, en caso
# contrario cierra la sesion y devuelve None
def connectClient(url, tm_out=1):	
	client = Client(url, timeout=tm_out)
	try:
		client.connect()
		return client
	except TimeoutError as e:
		print(f"Error al conectar con el servidor: {url} {e}")
	except Exception as e:
		print(f"Connection Error: {e}")

	client.disconnect() #Asegura el cierre de la sesion en caso de error
	return None
	

#----------------------------------------------------------------------------------------------------------------------------
#Extrae el nodo a partir de un cliente e 
# indicando el node a buscar
def getNodeFromServer(client, nodeId):

	node = client.get_node(nodeId)

	return node

#----------------------------------------------------------------------------------------------------------------------------
#Guarda los datos en una lista.
#Guardandolos en data y leyendolos desde row.
def loadValues(row, data):
	for value in row:
		if (type(value) == datetime):
			date = value.replace(microsecond=0) # Limpia los microseconds de los ultimos datos
		else:
			data.append((date, value))
	return data

#----------------------------------------------------------------------------------------------------------------------------
#Espera ha leer un valor valido, indicando que a partir de ahi empiezan los datos.
#Indicando que son valirdos en start, guardandolos en data y leyendolos desde row.
def ignoreValues(row, data, start):
	for value in row:
		if (type(value) == datetime):
			start = True
			data.append(row)
	return start, data

#----------------------------------------------------------------------------------------------------------------------------
#Carga los datos de un archivo excel a partir del nombre del archivo(fileName)
def getExcel(fileName):
	try:
		workbook = openpyxl.load_workbook(fileName) 
	except FileNotFoundError as e:
		raise e
	
	sheet = workbook.active

	data = [] 
	start = False
	#Vuelca el contenido de las celdas con contenido en data, 
	# empezando a guardar desde donde empiezan los datos deseados
	for row in sheet.iter_rows(values_only=True):
		if (start):
			data = loadValues(row, data)
		else:
			start, data = ignoreValues(row, data, start)

	return data

#----------------------------------------------------------------------------------------------------------------------------
# Elimina el +00:00 de la cadena(zona horaria)
def clearDate(nodeHoras):
	return nodeHoras.get_value().replace(tzinfo=None)

#----------------------------------------------------------------------------------------------------------------------------
#Espera que la hora del server de horas y la local sean iguales
def waitSyncDate(value, nodeHoras, seg):
	value_client = clearDate(nodeHoras)
	while value != value_client:
		time.sleep(seg)
		#Actualiza la hora del server de horas
		value_client = nodeHoras.get_value().replace(tzinfo=None)


#----------------------------------------------------------------------------------------------------------------------------
#Carga el nodo correspondiente al dato de la fecha
def dateNode(value, varName, nodeHoras, seg):
	varName = varName.get_child("1:Date") 
	waitSyncDate(value, nodeHoras, seg)
	
	return varName

#----------------------------------------------------------------------------------------------------------------------------
#Actualiza el valor del estado del objeto
def updateStatus(value, stat):
	if (type(value) != datetime):#ignora la fecha
		if(type(value) == float):
			stat.write_value(True)
		else:
			stat.write_value(False)
		print(f"Status: {stat.get_value()}")
		print("-------------")


#----------------------------------------------------------------------------------------------------------------------------
#Escribe los valores en el servidor, basandose en el tipo del dato
def writeValue(value, varName):
	# Actualizacion del valor en el servidor
	if ( type(value) == type(varName.get_value())):
		varName.write_value(value)
		print(f"Valor nodo: {varName} - escrito: {value}")


#----------------------------------------------------------------------------------------------------------------------------
#Modifica los valores en el servidor
def writeValues(value, varName, stat):	
	writeValue(value, varName)
	updateStatus(value, stat)

#----------------------------------------------------------------------------------------------------------------------------
#Dependiendo del tipo de dato, busca su nodo correspondiente y los guarda
def checkData(value, nodeHoras, varName, seg=0.1):
	stat = varName.get_child("1:Status") 

	if (type(value) == datetime): 
		varName = dateNode(value, varName, nodeHoras, seg)

	#Ajusta el tipo a double
	if (type(value) == int or type(value) == float): 
		value = float(value) / 12 #Ajusta y pasa de mm cada 5 min a mm/h
	
	writeValues(value, varName, stat)

#----------------------------------------------------------------------------------------------------------------------------
#Crea un objeto en el servidor indicado. A partir de buscar un tipo con searchNode y
#crea un objeto heredado de ese tipo con el idx y nombre pasados y lo devuelve para utilizarlo.
def createObj(servidor, idx, searchNode, objName):
	my_object_type = servidor.get_node(searchNode) 
	my_obj = servidor.nodes.objects.add_object(idx, objName, my_object_type)

	return my_obj

#----------------------------------------------------------------------------------------------------------------------------
# Crea un servidor a partir de la url y un archivo de configuracion xml
def createServer(url, xmlName):	
	servidor = Server()
	servidor.set_endpoint(url)

	servidor.import_xml(xmlName)

	return servidor

#----------------------------------------------------------------------------------------------------------------------------
#Inicia el cuerpo principal del programa, es decir, crea un objeto con el nombre indicado, extrae 
#el nodo del objeto y va modificando los respectivos campos
def iniProgram(servidor, idx, client, data, nodeH, nodeObj, nameObj):
	try: 
		nodeHoras = getNodeFromServer(client, nodeH)

		my_obj = createObj(servidor, idx, nodeObj, nameObj)
		preci = my_obj.get_child("1:Precipitacion") 

		for row in data: # Recorre las filas
			for value in row: # Recorre las celdas
				checkData(value, nodeHoras, preci)

	except Exception as e:
		print(f"ERROR: {e}")

	finally:
		client.disconnect() #Asegura el cierre de la sesion

#----------------------------------------------------------------------------------------------------------------------------
#Carga e inicia la conexion con el excel y el cliente(server de horas)
def startConnections(excelFile, serverUrl):
	# Carga los datos del Excel
	data = getExcel(excelFile)

	# Inicia y carga la conexion con el servidor de horas
	client = connectClient(serverUrl)

	return data, client

#----------------------------------------------------------------------------------------------------------------------------
#Lanza el programa principal, en el cual se configuran
# las variables al principio y luego empieza
def launchMain():
	#Declaracion y asignacion de variables
	excelFile = 'Pluvi_metroChiva_29octubre2024.xlsx'
	url = "opc.tcp://localhost:4841/achu/Pluviometro"
	uri = "http://www.achu.es/pluviometro"
	xmlName = "nodes.xml"
	svrHoras = "opc.tcp://localhost:4840/achu/horas"
	nodeH = "ns=2;i=2"
	nodeObj = "ns=1;i=2001"
	nameObj = "Rambla"

	# Inicio del programa
	servidor = createServer(url, xmlName)
	idx = servidor.register_namespace(uri)

	servidor.start() # Lanza el servidor

	try:
		
		data, client = startConnections(excelFile, svrHoras)

		if client:
			iniProgram(servidor, idx, client, data, nodeH, nodeObj, nameObj)
		
	except ua.UaStatusCodeError as e: 
		print(f"Error en la sesión: {e}")

	except FileNotFoundError as e: 
		print(f"Error en al abrir el archivo: {e}")

	except Exception as e:
		print(f"ERROR: {e}")

	finally:
		servidor.stop() #Asegura el cierre del servidor
	
#----------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
	launchMain()

#----------------------------------------------------------------------------------------------------------------------------
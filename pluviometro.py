from datetime import datetime
import time
import openpyxl

from asyncua.sync import Server, ua
from asyncua.sync import Client

#Author: Alejandro Rueda Plaza

#----------------------------------------------------------------------------------------------------------------------------

#Conecta con el server de horas, a partir de la url y el nodeId
def getServerHoras(url, nodeId):
	client = Client(url)
	client.connect()

	node = client.get_node(nodeId)

	return node

#----------------------------------------------------------------------------------------------------------------------------

#Carga los datos de un archivo excel a partir del nombre del archivo(fileName)
def getExcel(fileName):
	workbook = openpyxl.load_workbook(fileName) 
	sheet = workbook.active

	data = [] 
	start = False
	#Vuelca el contenido de las celdas con contenido en data, 
	# empezando a guardar desde donde empiezan los datos
	for row in sheet.iter_rows(values_only=True):
		if (start):
			data.append(row)
		else:
			for value in row:
				if (type(value) == datetime):
					start = True
					data.append(row)

	return data

#----------------------------------------------------------------------------------------------------------------------------

#Comprueba el tipo de valor a partir de un valor(value) y una tupla tipos(type).
#Cuando encuentra el valor actualiza dicho valor en el servidor, mediante el nodo(node) y
#la referencia a su correspondiente nodeId(varName). Ademas se pude configurar el tiempo
#de peticion al servidor de horas, mediante la variable seg, por defecto 0.5.
def checkData(value, nodeHoras, varName, seg=0.5):
	stat = varName.get_child("1:Status") 
	# Implementa el tiempo de solicitud de actualizacion de fecha al servidor de horas
	if (type(value) == datetime): 
		varName = varName.get_child("1:Date") 
		# Elimina el +00:00 de la cadena(zona horaria)
		value_client = nodeHoras.get_value().replace(tzinfo=None) 
		
		#Espera la hora del server de horas y la local sean iguales
		while value != value_client:
			time.sleep(seg)
			#Actualiza hora del server de horas
			value_client = nodeHoras.get_value().replace(tzinfo=None)

	#Ajusta el tipo a double
	if (type(value) == int): 
		value = float(value)
	
	# Actualizacion del valor en el servidor
	if ( type(value) == type(varName.get_value())):
		varName.write_value(value)
		print(f"Valor nodo: {varName} - escrito: {value}")
	if (type(value) != datetime):
		if(type(value) == float):
			stat.write_value(True)
		else:
			stat.write_value(False)
		print(f"Status: {stat.get_value()}")
		print("-------------")

#----------------------------------------------------------------------------------------------------------------------------
def createObj(servidor, idx):

	my_object_type = servidor.get_node("ns=1;i=2001") 
	my_obj = servidor.nodes.objects.add_object(idx, "Rambla", my_object_type)

	return my_obj

#----------------------------------------------------------------------------------------------------------------------------
#Crea un servidor a partir de una direccion url, una uri, un nombre para el nodo(nodeName),
#una tupla que contiene el nombre de los objetos y una lista que contiene la inicializacion
#de cada objeto.
def createServer(url, uri, nodeName, objs, varIni):
	variables = {} #Conjunto para las variables utilizadas para la escritura en el servidor
	varTypes = [] #Diccionario que guarda el tipo del contenido de las variables en el servidor
	servidor= Server()
	servidor.set_endpoint(url)
	idx=servidor.register_namespace(uri)

	'''
	type = servidor.nodes.base_object_type.add_object_type(idx, "Pluviometro")
	type.add_variable(idx, "Precipitation", 0.0).set_modelling_rule(True)
	preci = type.add_object(idx, "data")
	preci.add_property(idx, "Units", "mm/h").set_modelling_rule(True)
	preci.add_property(idx, "Status", False).set_modelling_rule(True)
	preci.add_property(idx, "Date", datetime.now()).set_modelling_rule(True)
	'''
	#myfolder = servidor.nodes.objects.add_folder(idx, "myFolder")
	mydev = servidor.nodes.objects.add_object(idx, "Rambla", type)
	'''
	preci = mydev.add_variable(idx, "Precipitation", 0.0)
	preci.add_property(idx, "Units", "mm/h")
	status = preci.add_property(idx, "Status", False)
	date = preci.add_property(idx, "Date", datetime.now())
	preci.set_writable()
	status.set_writable()
	date.set_writable()
	'''

	mi_obj=servidor.nodes.objects.add_object(idx, nodeName)
	#mi_obj=servidor.nodes.base_object_type.add_object_type(idx, nodeName)
	'''
	ini = None
	
	# Recorre los objetos a crear y los inicializa con su correspondite valor
	for i, obj in enumerate(objs):
		ini = varIni[i]
		varTypes.append(type(ini).__name__)
		# Nombre de las keys del diccionario "mi_X", donde X es el contenido de 
		# la tupla que se a pasado como parametro en minuscula
		variable = mi_obj.add_variable(idx, obj, ini)
		variable.set_writable() # Se configura la variable para que se pueda modificar
		
		if i==1:
			unit_id = 50667988 
			# ID de la unidad de milímetros (puedes ajustarlo según tu necesidad) 
			unit_range = ua.Range(0.0, 1000.0) 
			# Rango de la variable como float (puedes ajustarlo según tu necesidad) 
			eu_information = ua.EUInformation() 
			eu_information.DisplayName = ua.LocalizedText("millimeters") 
			eu_information.Description = ua.LocalizedText("Length in millimeters") 
			eu_information.UnitId = unit_id 
			eu_information.EngineeringUnits = ua.QualifiedName("mm", idx) 

			#variable.add_property(ua.QualifiedName("EURange", idx), ua.Variant(unit_range, ua.VariantType.ExtensionObject))
		
		variables[f"mi_{obj.lower()}"] = variable
		'''

	# Devuelve el servidor para lanzarlo, variables para modificar el contenido 
	# y varTypes para comprobar que tipo de contenido contiene
	return servidor, variables, varTypes

#----------------------------------------------------------------------------------------------------------------------------

def launchMain():
	'''
	url = "opc.tcp://localhost:4841/achu/Pluviometro"
	uri="http://www.achu.es/pluviometro"
	nodeName= "pluviometro"
	objs = ("Hora", "Lluvia") # Objetos a crear
	varIni = [datetime.now(), 0.0] # Inicializacion de los objetos

	# Cargar la configuracion del servidor y por ultimo se lanza
	variables = {} # variables del servidor
	varTypes = [] # tipo de contenido de las variables del servidor
	#servidor, variables, varTypes= createServer(url, uri, nodeName, objs, varIni)
	
	'''
	# Valores a introducir en el servidor
	servidor = Server()
	servidor.set_endpoint("opc.tcp://localhost:4841/achu/Pluviometro")
	uri="http://www.achu.es/pluviometro"
	idx = servidor.register_namespace(uri)
	servidor.import_xml("nodes.xml")
	servidor.start() # Lanza el servidor
	my_obj = createObj(servidor, idx)

	preci = my_obj.get_child("1:Precipitacion") 
	time.sleep(155)
	'''
	date = preci.get_child("1:Date") 
	stat = preci.get_child("1:Status") 
	print(f"AAAAAAAAAAAAAA  {(date.read_display_name())} - {type(123.45)}")
	preci.write_value(123.45)
	date.write_value(datetime.now())
	stat.write_value(True)
	'''
	#servidor.nodes.objects.add_object(idx, "Rambla").read_display_name

	# Inicia y carga la conexion con el servidor de horas
	nodeHoras = getServerHoras("opc.tcp://localhost:4840/achu/horas", "ns=2;i=2")

	# Carga los datos del Excel
	data = getExcel('Pluvi_metroChiva_29octubre2024.xlsx')

	try:
		for row in data: # Recorre las filas
			#print(f"WWWWWWWWWWW - {type(row)}")
			#print("-------------")
			for value in row: # Recorre las celdas
				#print(f"VVVVVVVVVV - {type(value)}, {value}")
				#print(f"SSSSSS -  {datetime}")
				#print(f"RRRRRRRR - {type(value)}, {value}")
				checkData(value, nodeHoras, preci)
				time.sleep(0.5)
				# Obtiene la posicion y el nombre de la variable de la configuracion del servidor cargada anteriormente
				#for i, name in enumerate(variables.keys()): 
					
					#Comprueba que tipo de dato se encuentra en el servidor y le aplica su configuración
					#if (varTypes[i] == str(datetime.__name__)):
					#	checkData(value, (datetime, ), nodeHoras, variables[name], 0.5)
					#if (varTypes[i] == str(float.__name__)):
					#	checkData(value, (int, float ), nodeHoras, variables[name])

	#except ua.UaStatusCodeError as e: 
	#	print(f"Error en la sesión: {e}")

	finally:
	    servidor.stop() #Asegura el cierre del servidor
	



#----------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
	launchMain()

#----------------------------------------------------------------------------------------------------------------------------
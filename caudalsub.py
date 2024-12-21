import time
import pandas as pd
from asyncua.sync import Server
from asyncua.sync import Client
from datetime import datetime
from opcua import ua

# Autor: Carlos Mira Lopez

# ---- Metodos de conexion cliente ------#

# Metodo al que le pasamos la URL del cliente para establecer la conexion
def conectar_cliente(url):
    # Creamos la conexion al cliente
    try:
        cliente_horas = Client(url)  # Iniciamos la conexion con el cliente a la URL que le pasamos
        cliente_horas.connect()  # Realizamos la conexion
        return cliente_horas
    except:
        time.sleep(1.0)
        print("Problemas con la conexion del cliente")
        

# Metodo que hace la configuracion final, la conexion con el cliente
# y obtener el nodo para poder acceder a la variable de las horas del server
def configurar_cliente(url, node_id):
    cliente_horas = None
    while not cliente_horas :
        cliente_horas = conectar_cliente(url)  # Creamos la conexion con el cliente  
    node = cliente_horas.get_node(node_id)  # Obtenemos el nodo de las horas que nos da el cliente
    return cliente_horas, node


# ---- Metodos de creacion del server ------#

# Este metodo crear un servidor en la url pasada a la funcion, e importa
# el tipo de datos Caudalimetro que le pasamos en la ruta
def crear_server(url, ruta_xml, uri):
    servidor = Server()
    servidor.set_endpoint((url))  # Creamos el server en la URL correspondiente
    servidor.import_xml(ruta_xml)  # Importamos el XML que contiene el tipo de objeto "Caudalimetro"
    idx = crear_namespace(servidor, uri)  # Creamos el namespace con la URI
    return servidor, idx

# Este metodo crea el objeto, del tipo importado en el xml y nos lo devuelve
def crear_objeto(servidor, idx, node_type):
    my_object_type = servidor.get_node(node_type)  # Obtenemos el tipo "Caudalimetro"
    my_obj = servidor.nodes.objects.add_object(idx, "Embalse", my_object_type)  # Creamos un objeto del tipo "Caudalimetro"
    return my_obj

# Este metodo nos registra un namspace con una URI personal
def crear_namespace(servidor, uri):
    return servidor.register_namespace(uri)  # Creamos el namespace con la URI

# Este metodo guarda el objeto, y sus propiedades en un diccionario
def buscar_objeto(my_obj, id_obj, id_est, id_fec):
    caudal = my_obj.get_child(id_obj)  # Buscamos la variable "Caudalimetro" a la cual le pondremos los valores
    estado = caudal.get_child(id_est)  # Buscamos la propiedad del "Caudalimetro" que nos indica el estado
    fecha = caudal.get_child(id_fec)  # Buscamos la propiedad del "Caudalimetro" que nos indica la fecha
    return {"Caudal": caudal, "Estado": estado, "Fecha": fecha}  # Devolvemos un diccionario con las variables


# ---- Metodo de cargar el excel ------#
# Metodo para cargar el CSV, donde lo transformamos tambien a tipo diccionario
def cargar_csv(rutaCSV):
    file = pd.read_csv(rutaCSV)  # Cargamos el CSV en el codigo y lo guardamos en la variable
    file = dict(file)  # Convertimos lo que hemos leido en diccionario
    return file


# ------Proceso ---#
# Proceso de buscar en el diccionario, la fecha para escribir en las variables correspondientes
# el valor de ese momento del excel
def Proceso(file, var, val):
    value_client = val
    # Buscamos el valor dentro del diccionario que hemos cargado anteriormente
    for x in range(len(file["Fecha"])):
        # Cuando la fecha coincide con el valor del cliente escribimos nuestros valores
        if file["Fecha"][x] == str(value_client.replace(tzinfo=None)):
            try:
                var["Fecha"].write_value(datetime.strptime(file["Fecha"][x], "%Y-%m-%d %H:%M:%S").replace(tzinfo=None))  # Escribimos la hora obtenida del excel
                
                # Si el estado que leemos de esa hora es un fallo, escribimos:
                # -1.0 en caudal
                # "FALLO" en estado
                # Si el estado no es fallo, escribimos en la variable el caudal leido, y "Funcionando" en Estado
                if file["Estado"][x] == "FALLO":
                    var["Estado"].write_value(str(file["Estado"][x]))  # Escribimos que el estado esta en FALLO
                    var["Caudal"].write_value(-1.0)  # Escribimos en el caudal el valor -1.0 indicando fallo
                    print(file["Fecha"][x], str(file["Estado"][x]))
                else:
                    var["Estado"].write_value("Funcionando")  # Escribimos que el Caudalimetro esta funcionando
                    var["Caudal"].write_value(float(file["Caudal"][x].replace(",", ".")))  # Escribimos el valor del caudal obtenido del excel
                    print(file["Fecha"][x], str(file["Estado"][x]), float(file["Caudal"][x].replace(",", ".")))
            except Exception as e:
                print("Error: ", e)  # Si surge algun error, que nos lo imprima

#CONFIGURACION DE LA CLASE SUBCRIPTOR
#Esta clase, se la asignamos a una variable para que cuando haya un cambio en una variable se llame a esta automaticamente
class SubHandler:
    def __init__(self, file, var,node2):
        self.file = file
        self.var = var
        self.node2 = node2
        self.dis_cli = False #Variable que cuando se desconecta el cliente detenemos el codigo

    #Funcion que incorporamos a la clase para que cuando haya un cambio la llame, ya que es una funcion que cuando se recibe un cambio
    # se llama automaticamente 
    def datachange_notification(self, node, val, attr):
        print(f"Cambio detectado en el cliente: {val}")
        Proceso(self.file, self.var,  val) #Cuando tenemos un nuevo valor, llamamos a la funcion proceos que escribira los valores
    
    def status_change_notification(self, status):
        print(f"Cliente desconectado")
        self.dis_cli = True

# ---- Lanzamiento del conjunto ------#
def launch():
    # Variables
    urlSer = "opc.tcp://localhost:4842/achu/embalse" #URL del servidor
    urlCli = "opc.tcp://localhost:4840/achu/temporal" #URL del cliene
    uri = "http://www.achu.es/embalse" #URI del servidor
    rutXlm = "nodes_caudal.xml" #Si se encuentra en la misma ruta que el codigo se queda igual
    rutCSV = "poyo.csv"#Ruta donde se encuentra el archivo CSV
    id_type = "ns=1;i=2001" #Id del tipo de objeto "Caudalimetro"
    id_obj = "2:Caudalimetro" #Nombre del objeto a buscar creado
    id_est = "2:Estado" #Nombre de la propiedad Estado del objeto Caudalimetro
    id_fec = "2:Fecha" #Nombre de la propiedad Fecha del objeto Caudalimetro
    id_nodoCli = "ns=2;i=2" #Id del nodo de las horas que es el que queremos del cliente

    #CONFIGURAMOS SERVER
	#Creamos el servidor, objeto y arrancamos el server
    print("Configurando servidor")
    servidor, idx = crear_server(urlSer, rutXlm, uri)
    my_obj = crear_objeto(servidor, idx, id_type)
    servidor.start()
    print("Servidor arrancado correctamente")

    #CONFIGURAMOS VARIABLES : Caudal, Estado, Fecha
    var = buscar_objeto(my_obj, id_obj, id_est, id_fec) #Esta funcion nos devuelve un diccionario con las 3 variables

    #CONFIGURACION DEL EXCEL
	#Cargar excel en el codigo y lo transformamos a diccionario para acceder mas rapido
    file = cargar_csv(rutCSV)

    
	#CONFIGURAMOS CLIENTE
	#Realizamos la conexion con el cliente a la URL que le pasamos, y obtenemos el nodo de las horas
    print("Iniciando conexion con el cliente")
    cliente, node2 = configurar_cliente(urlCli, id_nodoCli)
    print("Conexion realizada")

    #CONFIGURACION DEL SUBSCRIPTOR
    handler = SubHandler(file,var,node2) #Creamos una variable del tipo SubHandel, que es la clase que contiene el metodo para poder realizar los cambios
    sub = cliente.create_subscription(1000, handler) #Creamos la subscripcion con el cliente, asignandole el handler
    sub.subscribe_data_change(node2) #Le decimos que se subscriba al nodo del cliente, que en este caso es el de las horas

    try:
        print("Servidor y cliente configurados. Esperando cambios...")
        while handler.dis_cli == False :
            time.sleep(1) #Mantenemos el servidor conectado
    except ConnectionError as e:
        print(f"Se perdio la conexion con el cliente: {e}")
    finally:
        print("Cerrando servidor")
        servidor.stop()



if __name__ == "__main__":
    launch()

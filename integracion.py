import time
from asyncua.sync import Server, Client
from datetime import datetime

#Autor: Herminio Navarro Murcia

#---- Metodos de conexion cliente ------#

# Conexión a un cliente existente
def conectar_cliente(url):
    cliente = Client(url)
    try:
        cliente.connect()
        return cliente
    except Exception as e:
        print(f"Error al conectar con {url}: {e}")
        return None

# Se procura la obtención del valor del nodo del servidor cliente:
def obtener_valor_cliente(cliente, node_id):
    nodo = cliente.get_node(node_id)
    return nodo.get_value()

# Se realiza la configuración del servidor de integración
def configurar_servidor():
    servidor = Server()
    servidor.set_endpoint(("opc.tcp://localhost:4843/achu/integracion"))
    servidor.import_xml("nodes_integration.xml")
    print("Archivo XML de nodos cargado correctamente.")
    idx = servidor.register_namespace("http://www.achu.es/integracion")
    return servidor, idx

# Inspección de nodos en el espacio de direcciones
    print("Nodos disponibles en el espacio de direcciones:")
    for node in servidor.nodes.objects.get_children():
        print(f"Node: {node}, BrowseName: {node.get_browse_name()}")

# Lógica de cálculo del estado de alerta
def calcular_estado(precipitacion, caudal):
    if precipitacion > 50.0 and (caudal > 150.0 or caudal == -1.0):
        return "Alerta"
    return "No Alerta"

# Programa principal
def main():
    # URLs de los servidores a integrar
    url_caudal = "opc.tcp://localhost:4842/achu/embalse"
    url_precipitacion = "opc.tcp://localhost:4841/achu/Pluviometro"
    node_caudal = "ns=2;i=2"  # Nodo de caudal en servidor de caudal
    node_precipitacion = "ns=2;i=2"  # Nodo de precipitación en servidor de pluviómetro

    # Configuración del servidor de integración
    servidor, idx = configurar_servidor()

    obj = servidor.nodes.objects.add_object(idx, "SistemaIntegrado")
    var_caudal = obj.add_variable(idx, "Caudal", 0.0)
    var_precipitacion = obj.add_variable(idx, "Precipitacion", 0.0)
    var_estado = obj.add_variable(idx, "Estado", "No Alerta")
    
    servidor.start()
    print("Servidor de integración iniciado.")

    try:
        # Conexión a los clientes
        cliente_caudal = conectar_cliente(url_caudal)
        cliente_precipitacion = conectar_cliente(url_precipitacion)
        
        while True:
            # Leer valores de los clientes
            caudal = obtener_valor_cliente(cliente_caudal, node_caudal)
            precipitacion = obtener_valor_cliente(cliente_precipitacion, node_precipitacion)
            
            # Calcular estado de alerta
            estado = calcular_estado(precipitacion, caudal)
            
            # Escribir valores en el servidor de integración
            var_caudal.write_value(caudal)
            var_precipitacion.write_value(precipitacion)
            var_estado.write_value(estado)
            
            print(f"Caudal: {caudal}, Precipitación: {precipitacion}, Estado: {estado}")
            time.sleep(1)
    
    finally:
        servidor.stop()
        print("Servidor detenido.")

#---------------------------------------------------------------

if __name__ == "__main__":
	main()



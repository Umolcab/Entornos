import time
from collections import deque
from asyncua.sync import Server, Client
from datetime import datetime

# Autor: Herminio Navarro Murcia

# ---- Métodos de conexión cliente ------#

# Conexión a un cliente existente
def conectar_cliente(url):
    cliente = Client(url)
    try:
        cliente.connect()
        return cliente
    except Exception as e:
        print(f"Error al conectar con {url}: {e}")
        return None

# Obtención del valor del nodo del servidor cliente
def obtener_valor_cliente(cliente, node_id):
    try:
        nodo = cliente.get_node(node_id)
        return nodo.get_value()
    except Exception as e:
        print(f"Error al obtener el valor del nodo {node_id}: {e}")
        return None

# Configuración del servidor de integración
def configurar_servidor():
    # Creación del servidor de integración
    servidor = Server()
    servidor.set_endpoint("opc.tcp://localhost:4843/achu/integracion")

    # Importar el archivo XML de nodos
    try:
        servidor.import_xml("nodes_integration.xml")
        print("Archivo XML de nodos cargado correctamente.")
    except Exception as e:
        print(f"Error al cargar el archivo XML: {e}")

    idx = servidor.register_namespace("http://www.achu.es/integracion")
    return servidor, idx

# Lógica de cálculo del estado de alerta con acumulación
def calcular_estado_acumulado(precipitacion_actual, caudal, precipitaciones_acumuladas):
    # Añadir la nueva lectura de precipitaciones al historial
    precipitaciones_acumuladas.append(precipitacion_actual)
    if len(precipitaciones_acumuladas) > 12:  # Máximo 12 lecturas (1 hora de datos)
        precipitaciones_acumuladas.popleft()

    # Calcular promedio de las últimas 12 lecturas
    promedio_precipitacion = sum(precipitaciones_acumuladas) / len(precipitaciones_acumuladas)

    # Verificar condiciones de alerta
    if promedio_precipitacion > (50.0*12/3600) and (caudal > 150.0 or caudal == -1.0):
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

    # Crear variables en el servidor de integración
    obj = servidor.nodes.objects.add_object(idx, "SistemaIntegrado")
    var_caudal = obj.add_variable(idx, "Caudal", 0.0)
    var_precipitacion = obj.add_variable(idx, "Precipitacion", 0.0)
    var_estado = obj.add_variable(idx, "Estado", "No Alerta")

    # Permitir escritura de las variables
    var_caudal.set_writable()
    var_precipitacion.set_writable()
    var_estado.set_writable()

    # Iniciar el servidor
    servidor.start()
    print("Servidor de integración iniciado.")

    precipitaciones_acumuladas = deque(maxlen=12)  # Última hora de datos (12 lecturas de 5 minutos)

    try:
        # Conexión a los clientes
        cliente_caudal = conectar_cliente(url_caudal)
        cliente_precipitacion = conectar_cliente(url_precipitacion)

        if not cliente_caudal or not cliente_precipitacion:
            print("No se pudo conectar a uno o ambos clientes.")
            return

        while True:
            # Leer valores de los clientes
            caudal = obtener_valor_cliente(cliente_caudal, node_caudal)
            precipitacion = obtener_valor_cliente(cliente_precipitacion, node_precipitacion)

            if caudal is None or precipitacion is None:
                print("Error al leer datos de los clientes.")
                time.sleep(0.5)
                continue

            # Calcular estado con acumulación de precipitaciones
            estado = calcular_estado_acumulado(precipitacion, caudal, precipitaciones_acumuladas)

            # Escribir valores en el servidor de integración
            var_caudal.write_value(caudal)
            var_precipitacion.write_value(precipitacion)
            var_estado.write_value(estado)

            # Log de datos procesados
            print(f"[{datetime.now()}] Caudal: {caudal}, Precipitación: {precipitacion}, Estado: {estado}")
            
            time.sleep(1)  # Intervalo de 1 segundos

    finally:
        servidor.stop()
        print("Servidor detenido.")

#---------------------------------------------------------------

if __name__ == "__main__":
    main()
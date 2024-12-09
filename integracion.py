from opcua import Server, Client
import time

# Conexión con los servidores
cliente_pluviometro = Client("opc.tcp://localhost:4841/achu/Pluviometro")
cliente_caudal = Client("opc.tcp://localhost:4842/achu/embalse")
cliente_pluviometro.connect()
cliente_caudal.connect()

# Iniciar servidor integrado
server = Server()
server.set_endpoint("opc.tcp://localhost:4843/integrado/server/")
uri = "http://example.org/integrado"
idx = server.register_namespace(uri)

# Creación de las variables de monitorización
estado_alerta = server.nodes.objects.add_variable(idx, "EstadoAlerta", "No Alerta")
estado_alerta.set_writable()
cantidad_caudal = server.nodes.objects.add_variable(idx, "CantidadCaudal", 0.0)
cantidad_caudal.set_writable()
cantidad_lluvia = server.nodes.objects.add_variable(idx, "CantidadLluvia", 0.0)
cantidad_lluvia.set_writable()

# Inicio del servidos
server.start()
print("Servidor Integrado iniciado...")

# Cambio de estado de alerta dependiendo de la comparación de cantidades 
try:
    while True:
        lluvia = cliente_pluviometro.get_node("ns=2;i=3").get_value()
        caudal = cliente_caudal.get_node("ns=2;i=2").get_value()
        print(lluvia)
        print(caudal)
        cantidad_caudal = caudal
        cantidad_lluvia = lluvia
        time.sleep(1)
finally:
    server.stop()
    cliente_pluviometro.disconnect()
    cliente_caudal.disconnect()


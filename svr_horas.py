import time
from datetime import datetime, timedelta
from asyncua.sync import Server

if __name__ == "__main__":
    servidor = Server()
    servidor.set_endpoint(("opc.tcp://localhost:4840/achu/horas"))
    uri = "http://www.achu.es/horas"
    idx = servidor.register_namespace(uri)
    obj_horas = servidor.nodes.objects.add_object(idx,"srvHoras")
    hora_ini = datetime(2024, 10, 28, 22, 25, 00)
    var_horas = obj_horas.add_variable(idx, "Hora", hora_ini)
    var_horas.set_writable()
    servidor.start()

    try:
        hora = hora_ini
        while True:
            hora += timedelta(minutes=5.0)
            var_horas.write_value(hora)
            time.sleep(1)
            if hora == datetime(2024, 11, 1, 10, 25, 00):
                break

    finally:
        servidor.stop()

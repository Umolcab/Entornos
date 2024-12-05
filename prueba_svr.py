#800 m^3/segundo de caudal en la rambla
#2000 medidos

import time
from asyncua.sync import Server

if __name__ == "__main__":
	servidor= Server()
	servidor.set_endpoint(("opc.tcp://PC_Unai:53530/OPCUA/SimulationServer"))
	uri="http://www.achu.upv.es/entornos"
	idx=servidor.register_namespace(uri)
	mi_obj=servidor.nodes.objects.add_object(idx,"miObjeto")
	mi_var = mi_obj.add_variable(idx, "numero", -10.0)
	mi_var.set_writable()
	servidor.start()

	servidor.import_xml("nodes.xml")

	try:
		contador = 0
		while True:
			time.sleep(1)
			contador = mi_var.read_value()
			contador += 0.1
			mi_var.write_value(contador)

	finally:
		servidor.stop()
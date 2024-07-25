import base64
from constants import *
import logging
import paho.mqtt.client as mqtt
import subprocess
import json
import os

#Incializamos los loggers, uno por cada diferente nivel que se va a usar
debugLogger = logging.getLogger("debugLogger")
infoLogger = logging.getLogger("infoLogger")
warningLogger = logging.getLogger("warningLogger")
errorLogger = logging.getLogger("errorLogger")
criticalLogger = logging.getLogger("criticalLogger")

#Se establece el nivel de cada uno de ellos:
debugLogger.setLevel(logging.DEBUG)
infoLogger.setLevel(logging.INFO)
warningLogger.setLevel(logging.WARNING)
errorLogger.setLevel(logging.ERROR)
criticalLogger.setLevel(logging.CRITICAL)

#Formateador:
formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
#Manejador de archivo:
file_h = logging.FileHandler('logging_FUSE.log')
file_h.setFormatter(formatter)

#El formateador y el archivo de logs va a ser comun a todos los niveles, ya que los logs se van a gestionar con kibana:
debugLogger.addHandler(file_h)
infoLogger.addHandler(file_h)
warningLogger.addHandler(file_h)
errorLogger.addHandler(file_h)
criticalLogger.addHandler(file_h)

#Para que imprima por consola:
debugLogger.addHandler(logging.StreamHandler())
infoLogger.addHandler(logging.StreamHandler())
warningLogger.addHandler(logging.StreamHandler())
errorLogger.addHandler(logging.StreamHandler())
criticalLogger.addHandler(logging.StreamHandler())

#Los cambios sobre MQTT se registran este log:
mqttInfoLogger = logging.getLogger("mqttInfoLogger")
mqttInfoLogger.setLevel(logging.INFO)

file_h_mqtt = logging.FileHandler('logging_MQTT.log')
file_h_mqtt.setFormatter(formatter)
mqttInfoLogger.addHandler(file_h_mqtt)



def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Se ha conectado al broker con exito\n")
        client.subscribe(LOGGING_FS_TOPIC)

def on_message(client, userdata, msg):

    topic = msg.topic.split('/')
    logging_response = msg.payload.decode()
    logging_info = json.loads(logging_response)

    if topic[-1] == "filesystem":

        topic = logging_info["topic"]
        topic_operation = logging_info["topic"].split('/')[-1]
        func = logging_info["func"]
        argumentos = logging_info["args"]
        os_result = logging_info["os_result"]
        response_loaded = json.loads(os_result)

        if isinstance(response_loaded, int):
            if os_result == "0":
                infoLogger.info("La operacion " + topic_operation + " ha tenido exito")
                debugLogger.debug("topic : " + topic + " - func : " + func + " - args : " + str(argumentos))

            else:
                errorLogger.error("Ha habido un error a la hora de ejecutar la operacion: '" + func + "' con los argumentos: "
                                  + str(argumentos) + " - Codigo de error: " + str(os_result) + " - Mensaje de error: " + os.strerror(int(os_result)))


        else:
            # Operaciones con respuesta:
            if isinstance(response_loaded, dict):

                #Aqui se capturan todos los logs de las operaciones que tienen un unico resultado y cuya clave es "os_result" para diferenciarlo de aquellos que no tienen resultado
                if len(response_loaded) == 1:
                    key, v = response_loaded.popitem()

                    if topic_operation == "read":
                        v = base64.b64decode(v) #Hace falta la deserializacion que hicimos con base64 en file_m.py
                    if topic_operation == "write":
                        argumentos = list(argumentos)
                        argumentos[1] = base64.b64decode(argumentos[1])

                    infoLogger.info("La operacion " + topic_operation + " ha tenido exito")
                    debugLogger.debug("topic : " + topic + " - func : " + func + " - args : " + str(argumentos) + " - os_result: " + str(v))

                #para el getattr:
                else:
                    infoLogger.info("La operacion " + topic_operation + " ha tenido exito")
                    debugLogger.debug("topic : " + topic + " - func : " + func + " - args : " + str(argumentos) + " - os_result: " + str(response_loaded))

            #readdir:
            if isinstance(response_loaded, list):
                infoLogger.info("La operacion " + topic_operation + " ha tenido exito")
                debugLogger.debug("topic : " + topic + " - func : " + func +
                                  " - args : " + str(argumentos) + " - os_result: " + str(response_loaded))
    

    if topic[-1] == "mqtt":
        mqttInfoLogger.info("Modulo" + logging_info["module_name"] + ", mensaje: " + logging_info["buffer"])


def on_log(client, userdata, level, buf):
    mqttInfoLogger.info(buf)



def main():
    client = mqtt.Client(client_id="logger")
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_log = on_log
    client.connect("localhost", 1883, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()
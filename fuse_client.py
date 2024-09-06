import sys
import paho.mqtt.client as mqtt
import json
import time
from fuse import FUSE, FuseOSError, Operations, fuse_exit
import base64
from constants import *
from config import *
from functools import partial
from signal import signal, SIGINT
from os import O_RDONLY, O_DIRECTORY
import errno

CLIENT_PATH = ""


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Se ha conectado al broker con exito")
        client.subscribe(READ_TOPIC)  # Para recibir los datos de vuelta
        client.subscribe(READDIR_TOPIC)
        client.subscribe(GETATTR_TOPIC)
        client.subscribe(OPEN_TOPIC)
        client.subscribe(WRITE_TOPIC)
        client.subscribe(CREATE_TOPIC)
        client.subscribe(RENAME_TOPIC)
        client.subscribe(UNLINK_TOPIC)
        client.subscribe(FLUSH_FSYNC_TOPIC)
        client.subscribe(RELEASE_TOPIC)
        client.subscribe(CHMOD_TOPIC)
        client.subscribe(MKDIR_TOPIC)
        client.subscribe(RMDIR_TOPIC)
        client.subscribe(TRUNCATE_TOPIC)
        client.subscribe(ACCESS_TOPIC)
        client.subscribe(SYMLINK_TOPIC)
        client.subscribe(LINK_TOPIC)
        client.subscribe(READLINK_TOPIC)
    else:
        print("Error al intentar conectase al broker")


def on_message(client, userdata, msg):
    topic = msg.topic.split('/')
    userdata[topic[-1]] = msg.payload.decode()


def sync(op, pending_requests):
    while pending_requests.get(op) is None:
        time.sleep(0.1)
    return pending_requests.pop(op)


def response_handler(op, key, pending_requests):
    response = sync(op, pending_requests)
    response_loaded = json.loads(response)

    if isinstance(response_loaded, dict):
        return response_loaded[key]
    elif isinstance(response_loaded, list):
        return response_loaded  #para readdir
    else:
        raise FuseOSError(response_loaded)


def no_response_handler(data, topic, op, client, pending_requests):
    #si data no es un str, lo serializamos con json
    if not isinstance(data, str):
        data = json.dumps(data)
    client.publish(topic, data, qos=2)

    response = sync(op, pending_requests)
    if response == "0":
        return None
    else:
        raise FuseOSError(int(response))


class MqttFS(Operations):
    def __init__(self):
        # Para manejar la asincronia:
        self.pending_requests = {}

        self.client = mqtt.Client(client_id="fuse_client", userdata=self.pending_requests)

        #Se puede comentar/borrar la siguiente linea si se permiten conexiones anonimas
        self.client.username_pw_set(username=MQTT_BROKER_USER, password=MQTT_BROKER_PASSWORD)

        self.client.on_connect = on_connect
        self.client.on_message = on_message
        self.client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, MQTT_BROKER_KEEPALIVE)

        self.no_response_handler = partial(no_response_handler, client=self.client,
                                           pending_requests=self.pending_requests)
        self.response_handler = partial(response_handler, pending_requests=self.pending_requests)

        self.client.loop_start()

    def readdir(self, path, fh):
        self.client.publish(REQUEST_READ_DIR_TOPIC, path, qos=1)
        return self.response_handler("readDir", "")

    def getattr(self, path, fh=None):

        if path in ["/.xdg-volume-info", "/autorun.inf", "/.hidden", "/.Trash"]:
            raise FuseOSError(errno.ENOENT)

        self.client.publish(REQUEST_GETATTR_TOPIC, path, qos=1)

        response = sync("getattr", self.pending_requests)

        list_stat = json.loads(response)
        if isinstance(list_stat, dict):
            return list_stat
        else:
            raise FuseOSError(list_stat)


    def open(self, path, flags):
        open_data = {"path": path, "flags": flags}
        json_open = json.dumps(open_data)
        self.client.publish(REQUEST_OPEN_TOPIC, json_open, qos=1)

        return self.response_handler("open", "os_result")

    def read(self, path, size, offset, fh):

        read_data = {"fh": fh, "size": size}
        json_read = json.dumps(read_data)

        self.client.publish(REQUEST_READ_TOPIC, json_read,
                            qos=1)  # Solicitamos lectura.      Para el read considero qos --> At least once

        response = sync("read", self.pending_requests)
        read_value = json.loads(response)

        if isinstance(read_value, dict):
            datos_leidos = base64.b64decode(read_value["datos_b64"])

            # necesario devolver literal de bytes:
            return datos_leidos
        else:
            raise FuseOSError(read_value)

    def write(self, path, data, offset, fh):
        write_data = {}
        write_data["file_handle"] = fh
        write_data["text"] = base64.b64encode(data).decode()

        datos_json = json.dumps(write_data)
        self.client.publish(REQUEST_WRITE_TOPIC, datos_json,
                            qos=2)  # Solicito escritura.        qos --> "Exactly once" para evitar duplicados

        return self.response_handler("write", "num_bytes_written")

    def truncate(self, path, length, fh=None):
        truncate_data = {"path": path, "length": length}
        self.no_response_handler(truncate_data, REQUEST_TRUNCATE_TOPIC, "truncate")

    def create(self, path, mode, fi=None):
        create_data = {"path": path, "mode": mode}
        json_create = json.dumps(create_data)
        self.client.publish(REQUEST_CREATE_TOPIC, json_create, qos=2)

        return self.response_handler("create", "os_result")

    def rename(self, old, new):

        self.no_response_handler({"old": old, "new": new}, REQUEST_RENAME_TOPIC, "rename")

    # PARA EL CIERRE DE ARCHIVOS:

    # necesario para los .swp que se generan despues del create
    def unlink(self, path):

        self.no_response_handler(path, REQUEST_UNLINK_TOPIC, "unlink")

    # Para realizar tareas de limpieza asociadas con el cierre del archivo
    def flush(self, path, fh):
        #lo implemento como fsync al no existir implementacion de flush con "os"
        self.no_response_handler(fh, REQUEST_FLUSH_FSYNC_TOPIC, "flush_fsync")

    # Para forzar la escritura de todos los cambios pendientes del archivo
    def fsync(self, path, datasync, fh):
        self.no_response_handler(fh, REQUEST_FLUSH_FSYNC_TOPIC, "flush_fsync")

    # El kernel no tiene una llamada "fsyncdir", llama a fsync con el descriptor de un directorio. Esto es asi en fuse por si quieres implementar
    # logica extra
    def fsyncdir(self, path, datasync, fh):

        return self.fsync(path, datasync, fh)

    def release(self, path, fh):

        self.no_response_handler(fh, REQUEST_RELEASE_TOPIC, "release")

    #PERMISOS:

    def chmod(self, path, mode):

        self.no_response_handler({"path": path, "mode": mode}, REQUEST_CHMOD_TOPIC, "chmod")

    def mkdir(self, path, mode):

        self.no_response_handler({"path": path, "mode": mode}, REQUEST_MKDIR_TOPIC, "mkdir")

    def rmdir(self, path):

        self.no_response_handler(path, REQUEST_RMDIR_TOPIC, "rmdir")

    #Verifica los permisos antes de ejecutar una operacion del sistema de ficheros
    def access(self, path, mode):
        access_data = {"path": path, "mode": mode}
        json_access = json.dumps(access_data)
        self.client.publish(REQUEST_ACCESS_TOPIC, json_access, qos=1)

        response = sync("access", self.pending_requests)
        access_value = json.loads(response)

        if access_value == True:
            return 0
        elif access_value == False:
            return -1 #Op not permitted
        else:
            raise (FuseOSError(access_value))

    def opendir(self, path):

        #O_DIRECTORY comprueba que se esta abriendo directorio
        return self.open(path, O_RDONLY | O_DIRECTORY)

    def releasedir(self, path, fh):

        self.release(path, fh)

    #ENLACES:

    def symlink(self, target, source):

        symlink_data = {"source": source, "target": target}
        self.no_response_handler(symlink_data, REQUEST_SYMLINK_TOPIC, "symlink")

    def link(self, target, source):
        #Crea enlace duro
        link_data = {"source": source, "target": target}
        self.no_response_handler(link_data, REQUEST_LINK_TOPIC, "link")


    def readlink(self, path):

        self.client.publish(REQUEST_READLINK_TOPIC, path, qos=2)
        return self.response_handler("readlink", "os_result")



#Esta funcion permite desmontar de una forma limpia el sistema de ficheros cuando se interrumpe el programa desde la terminal.
def unmount():
    fuse_exit()


def main():
    global CLIENT_PATH

    #EL SEGUNDO ARGUMENTO NO ES NECESARIO SI SE DECIDE INTRODUCIRLO COMO VARIABLE GLOBAL DENTRO DE ESTE FICHERO
    if len(sys.argv) > 2:
        print("Solo se aceptan 1 o 2 argumentos, el nombre del script y el path absoluto donde se va montar el sistema de ficheros")
        sys.exit(1)


    if len(sys.argv) == 2:
        CLIENT_PATH = sys.argv[1]

    signal(SIGINT, unmount)
    FUSE(MqttFS(), CLIENT_PATH, foreground=True, nothreads=True, debug=True)

if __name__ == "__main__":
    main()

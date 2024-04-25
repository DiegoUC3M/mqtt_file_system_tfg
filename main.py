import errno
import os
import paho.mqtt.client as mqtt
import json
import time
from fuse import FUSE, FuseOSError, Operations
from stat import S_IFDIR
import subprocess #para el comando de fusermount
import re #TODO ver si se elimina, es para el regex del getattr
import base64 #TODO ver si se elimina


REQUEST_WRITE_TOPIC = "/topic/request/write"
WRITE_TOPIC = "/topic/write"

REQUEST_READ_TOPIC = "/topic/request/read"
READ_TOPIC = "/topic/read"

READDIR_TOPIC = "/topic/readDir"
REQUEST_READ_DIR_TOPIC = "/topic/request/readDir"

GETATTR_TOPIC = "/topic/getattr"
REQUEST_GETATTR_TOPIC = "/topic/request/getattr"

OPEN_TOPIC = "/topic/open"
REQUEST_OPEN_TOPIC = "/topic/request/open"

TRUNCATE_TOPIC = "/topic/truncate"
REQUEST_TRUNCATE_TOPIC = "/topic/request/truncate"

CREATE_TOPIC = "/topic/create"
REQUEST_CREATE_TOPIC = "/topic/request/create"

RENAME_TOPIC = "/topic/rename"
REQUEST_RENAME_TOPIC = "/topic/request/rename"

UNLINK_TOPIC = "/topic/unlink"
REQUEST_UNLINK_TOPIC = "/topic/request/unlink"

FLUSH_FSYNC_TOPIC = "/topic/flush_fsync"
REQUEST_FLUSH_FSYNC_TOPIC = "/topic/request/flush_fsync"

RELEASE_TOPIC = "/topic/release"
REQUEST_RELEASE_TOPIC = "/topic/request/release"

CLIENT_PATH = "/home/diego/PycharmProjects/mqtt_test/directorio_cliente"



def on_connect(client, userdata, flags, rc):
    global ack
    if rc == 0:
        print("Se ha conectado al broker con exito\n")
        client.subscribe(READ_TOPIC)  # Para recibir los datos de vuelta
        client.subscribe(READDIR_TOPIC)
        client.subscribe(GETATTR_TOPIC)
        client.subscribe(OPEN_TOPIC)
        client.subscribe(WRITE_TOPIC)
        client.subscribe(TRUNCATE_TOPIC)
        client.subscribe(CREATE_TOPIC)
        client.subscribe(RENAME_TOPIC)
        client.subscribe(UNLINK_TOPIC)
        client.subscribe(FLUSH_FSYNC_TOPIC)
        client.subscribe(RELEASE_TOPIC)
    else:
        print("Error al intentar conectase al broker")


def on_publish(client, userdata, mid):
    #print(f"El mensaje se ha publicado con exito. MESSAGE_ID: {mid}")
    pass


def on_message(client, userdata, msg):
    topic = msg.topic.split('/')

    userdata[topic[-1]] = msg.payload

def sync(op, pending_requests): #TODO:  cambiar el nombre del pending_requests

    while pending_requests.get(op) is None:
        time.sleep(0.1)
    return pending_requests.pop(op)

class MqttFS(Operations):
    def __init__(self):
        # Para manejar la asincronia:
        self.pending_requests = {}

        self.client = mqtt.Client(client_id="main", userdata=self.pending_requests)
        self.client.on_connect = on_connect
        self.client.on_message = on_message
        self.client.on_publish = on_publish
        self.client.connect("localhost", 1883, 60)
        self.client.loop_start()

    #TODO: ESTUDIAR EL POSIBLE MANEJO DE ERRORES DE TODAS LAS OPERACIONES

    def readdir(self, path, fh):
        self.client.publish(REQUEST_READ_DIR_TOPIC, CLIENT_PATH + path, qos=2)  # TODO: decidir que qos implemento ?
                                                                                # TODO: que mensaje mando? ese no se utiliza para nada

        response = sync("readDir", self.pending_requests)

        listdir = json.loads(response)

        return listdir


    def getattr(self, path, fh=None):


        if re.compile(r"^\/\.Trash").match(path) or path in ["/.xdg-volume-info", "/autorun.inf", "/.hidden"]:
            raise FuseOSError(errno.ENOENT)

        #Esta linea esta implementada por que el sistema llama mucho a "/" al principio (no se por que), y mi gestor FUSE no es capaz de procesar secuencialmente todas
        #estas peticiones, este trozo de codigo habria que cambiarlo. Se queda de momento para pruebas

        elif path == "/":
            return dict(st_mode=(S_IFDIR | 0o755), st_nlink=2) #La implementacion de fuse.py original

        else:
            self.client.publish(REQUEST_GETATTR_TOPIC, path, qos=2)  # TODO: decidir que qos implemento ?

            response = sync("getattr", self.pending_requests)

            decoded_response = response.decode()
            if decoded_response != "1":
                list_stat = json.loads(response)
                return list_stat
            else:
                raise FuseOSError(errno.ENOENT)

    def open(self, path, flags):
        open_data = {}
        open_data["path"] = path
        open_data["flags"] = flags
        json_open = json.dumps(open_data)
        self.client.publish(REQUEST_OPEN_TOPIC, json_open, qos=2) # TODO: decidir que qos implemento ?

        response = sync("open", self.pending_requests)

        file_handle = json.loads(response)

        return file_handle

    def read(self, path, size, offset, fh):

        read_data = {}
        read_data["fh"] = fh
        read_data["size"] = size
        json_read = json.dumps(read_data)

        self.client.publish(REQUEST_READ_TOPIC, json_read, qos=2)  # Solicitamos lectura.      Para el read considero qos --> At least once

        response = sync("read", self.pending_requests)

        #necesario devolver literal de bytes:
        return response



    def write(self, path, data, offset, fh):
        #TODO: poner todos los diccionarios en una linea
        write_data = {}

        #write_data["text"] = data.decode()
        write_data["file_handle"] = fh


        write_data["text"] = data


        datos_json = json.dumps(write_data)

        self.client.publish(REQUEST_WRITE_TOPIC, datos_json, qos=2)  # Solicito escritura.        qos --> "Exactly once" para evitar duplicados

        response = sync("write", self.pending_requests)

        num_bytes_written = json.loads(response)

        return num_bytes_written

    def truncate(self, path, length, fh):

        truncate_data = {}
        truncate_data["fh"] = fh
        truncate_data["length"] = length
        json_read = json.dumps(truncate_data)

        self.client.publish(REQUEST_TRUNCATE_TOPIC, json_read, qos=2)  # TODO: ver que qos implemento

        #TODO: realmente esto se podria quitar y hacer un return None????
        response = sync("truncate", self.pending_requests)

        return response

    def create(self, path, mode, fi=None):
        create_data = {}

        create_data["path"] = path
        create_data["mode"] = mode
        json_read = json.dumps(create_data)

        self.client.publish(REQUEST_CREATE_TOPIC, json_read,
                            qos=2)  # Solicitamos lectura.      Para el read considero qos --> At least once

        response = sync("create", self.pending_requests)

        file_handle = json.loads(response) #si no  haces esto no detecta el integer

        return file_handle

    def rename(self, old, new):
        rename_data = {"old": old, "new": new}

        json_rename = json.dumps(rename_data)

        self.client.publish(REQUEST_RENAME_TOPIC, json_rename,
                            qos=2)  #TODO ver que qos implemento

        response = sync("rename", self.pending_requests)

        decoded_response = response.decode()
        if decoded_response != "1":
            return None
        else:
            #Si ha fallado la operacion de rename mandaremos error de que el sistema es read-only file system por defecto
            #TODO: implementar una forma mas generica del rollo:        except OSError as e:    raise FuseOSError(e.errno)
            raise FuseOSError(errno.EROFS)

    # PARA EL CIERRE DE ARCHIVOS:

    # necesario para los .swp que se generan despues del create
    def unlink(self, path):

        self.client.publish(REQUEST_UNLINK_TOPIC, path, qos=2)  #TODO ver que qos implemento

        response = sync("unlink", self.pending_requests)

        decoded_response = response.decode()
        if decoded_response != "1":
            return None
        else:
            #Si ha fallado la operacion de unlink mandaremos error de que el sistema es read-only file system por defecto
            #TODO: implementar una forma mas generica del rollo:        except OSError as e:    raise FuseOSError(e.errno)
            raise FuseOSError(errno.EROFS)



    # Para realizar tareas de limpieza asociadas con el cierre del archivo
    def flush(self, path, fh):

        # TODO: estudiar si de verdad es una buena idea dejar la implementacion como en el fsync
        self.client.publish(REQUEST_FLUSH_FSYNC_TOPIC, fh, qos=2)  # TODO ver que qos implemento

        return 0



    # Para forzar la escritura de todos los cambios pendientes del archivo
    def fsync(self, path, datasync, fh):

        self.client.publish(REQUEST_FLUSH_FSYNC_TOPIC, fh, qos=2)  # TODO ver que qos implemento

        return 0

    def release(self, path, fh):

        self.client.publish(REQUEST_RELEASE_TOPIC, fh, qos=2)  # TODO ver que qos implemento

        return 0




if __name__ == "__main__":

    subprocess.run(['fusermount', '-uz', CLIENT_PATH], capture_output=False, text=True) #A veces no se desmonta el volumen aunque detengas el script
    #TODO: borrar estas lineas para ejecutar comandos en bash
    #subprocess.run(['rmdir', CLIENT_PATH], capture_output=False, text=True)
    #subprocess.run(['mkdir', CLIENT_PATH], capture_output=False, text=True)
    fuse = FUSE(MqttFS(), CLIENT_PATH, foreground=True, nothreads=True, debug = True)

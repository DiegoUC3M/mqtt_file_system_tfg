import paho.mqtt.client as mqtt
import json
import time
from fuse import FUSE, FuseOSError, Operations
import subprocess #para el comando de fusermount
import base64
from constants import *

CLIENT_PATH = "/home/diego/PycharmProjects/mqtt_test/directorio_cliente"



def on_connect(client, userdata, flags, rc):

    if rc == 0:
        print("Se ha conectado al broker con exito\n")
        client.subscribe(READ_TOPIC)  # Para recibir los datos de vuelta
        client.subscribe(READDIR_TOPIC)
        client.subscribe(GETATTR_TOPIC)
        client.subscribe(OPEN_TOPIC)
        client.subscribe(WRITE_TOPIC)
        client.subscribe(FTRUNCATE_TOPIC)
        client.subscribe(CREATE_TOPIC)
        client.subscribe(RENAME_TOPIC)
        client.subscribe(UNLINK_TOPIC)
        client.subscribe(FLUSH_FSYNC_TOPIC)
        client.subscribe(RELEASE_TOPIC)
        client.subscribe(CHOWN_TOPIC)
        client.subscribe(CHMOD_TOPIC)
        client.subscribe(MKDIR_TOPIC)
        client.subscribe(RMDIR_TOPIC)
        client.subscribe(TRUNCATE_TOPIC)
    else:
        print("Error al intentar conectase al broker")


def on_publish(client, userdata, mid):
    #print(f"El mensaje se ha publicado con exito. MESSAGE_ID: {mid}")
    pass


def on_message(client, userdata, msg):
    topic = msg.topic.split('/')

    userdata[topic[-1]] = msg.payload.decode()

def sync(op, pending_requests):

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
        self.client.publish(REQUEST_READ_DIR_TOPIC, path, qos=1)
        response = sync("readDir", self.pending_requests)

        listdir = json.loads(response)

        return listdir


    def getattr(self, path, fh=None):

        self.client.publish(REQUEST_GETATTR_TOPIC, path, qos=1)

        response = sync("getattr", self.pending_requests)

        #list_stat sera un diccionario si la operacion tuvo exito, y un integer en caso de error
        list_stat = json.loads(response)
        if not isinstance(list_stat, int):
            return list_stat
        else:
            raise FuseOSError(list_stat)

    #TODO: implementar manejo de excepciones, para el caso en el que no tengas ficheros de apertura para un fichero
    #TODO: hacer lo mismo para write/read
    def open(self, path, flags):
        open_data = {"path": path, "flags": flags}

        json_open = json.dumps(open_data)
        self.client.publish(REQUEST_OPEN_TOPIC, json_open, qos=1)

        response = sync("open", self.pending_requests)

        file_handle = json.loads(response)
        if isinstance(file_handle, int):
            return file_handle
        else:
            raise FuseOSError(file_handle["error"])

    def read(self, path, size, offset, fh):

        read_data = {"fh": fh, "size": size}
        json_read = json.dumps(read_data)

        self.client.publish(REQUEST_READ_TOPIC, json_read, qos=1)  # Solicitamos lectura.      Para el read considero qos --> At least once

        response = sync("read", self.pending_requests)

        read_dict = json.loads(response)
        datos_leidos = base64.b64decode(read_dict["datos_b64"])

        #necesario devolver literal de bytes:
        return datos_leidos



    def write(self, path, data, offset, fh):
        write_data = {}
        write_data["file_handle"] = fh
        write_data["text"] = base64.b64encode(data).decode()


        datos_json = json.dumps(write_data)

        self.client.publish(REQUEST_WRITE_TOPIC, datos_json, qos=2)  # Solicito escritura.        qos --> "Exactly once" para evitar duplicados

        response = sync("write", self.pending_requests)
        num_bytes_written = json.loads(response)

        if isinstance(num_bytes_written, int):
            return num_bytes_written
        else:
            raise FuseOSError(num_bytes_written["error"])


    def ftruncate(self, path, length, fh):

        ftruncate_data = {"fh": fh, "length": length}
        json_ftruncate = json.dumps(ftruncate_data)
        self.client.publish(REQUEST_FTRUNCATE_TOPIC, json_ftruncate, qos=1)

        return None

    def truncate(self, path, length):
        truncate_data = {"path": path, "length": length}
        json_truncate = json.dumps(truncate_data)
        self.client.publish(REQUEST_TRUNCATE_TOPIC, json_truncate, qos=1)

        return None


    def create(self, path, mode, fi=None):
        create_data = {"path": path, "mode":mode}

        json_create = json.dumps(create_data)

        self.client.publish(REQUEST_CREATE_TOPIC, json_create, qos=2)

        response = sync("create", self.pending_requests)
        file_handle = json.loads(response) #si no  haces esto no detecta el integer

        return file_handle

    def rename(self, old, new):

        rename_data = {"old": old, "new": new}
        json_rename = json.dumps(rename_data)

        self.client.publish(REQUEST_RENAME_TOPIC, json_rename, qos=2)

        response = sync("rename", self.pending_requests)

        if response == "0":
            return None
        else:
            raise FuseOSError(int(response))

    # PARA EL CIERRE DE ARCHIVOS:

    # necesario para los .swp que se generan despues del create
    def unlink(self, path):

        self.client.publish(REQUEST_UNLINK_TOPIC, path, qos=2)

        response = sync("unlink", self.pending_requests)

        if response == "0":
            return None
        else:
            raise FuseOSError(int(response))



    # Para realizar tareas de limpieza asociadas con el cierre del archivo
    def flush(self, path, fh):

        #lo implemento comom fsync al no existir implementacion de flush con "os"
        self.client.publish(REQUEST_FLUSH_FSYNC_TOPIC, fh, qos=2)

        return 0


    # Para forzar la escritura de todos los cambios pendientes del archivo
    def fsync(self, path, datasync, fh):

        self.client.publish(REQUEST_FLUSH_FSYNC_TOPIC, fh, qos=2)

        return 0

    def release(self, path, fh):

        self.client.publish(REQUEST_RELEASE_TOPIC, fh, qos=2)

        return 0

    #PERMISOS:

    def chmod(self, path, mode):
        chmod_data = {"path": path, "mode": mode}
        json_chmod = json.dumps(chmod_data)

        self.client.publish(REQUEST_CHMOD_TOPIC, json_chmod, qos=1)

        response = sync("chmod", self.pending_requests)

        if response == "0":
            return None
        else:
            raise FuseOSError(int(response))

    '''
    #TODO: comprobar si no funciona al estar el directorio montado en mi espacio de usuario, y por ese motivo otro usuario
    #TODO: no puede acceder, al no tener permisos de r-x para mi directorio home
    def chown(self, path, uid, gid):
        chown_data = {"path": path, "uid": uid, "gid": gid}
        json_chown = json.dumps(chown_data)

        self.client.publish(REQUEST_CHOWN_TOPIC, json_chown, qos=1)

        response = sync("chown", self.pending_requests)

        if response == "0":
            return None
        else:
           raise FuseOSError(int(response))
    '''

    #OPERACIONES PARA DIRECTORIO:

    '''
    #TODO: comprobar si se puede sustituir por ACCESS, o si es necesario realmente
    
    def opendir(self, path):

        return 0
    
    def releasedir(self, path, fh):
        return 0
    '''
    def mkdir(self, path, mode):
        mkdir_data = {"path": path, "mode": mode}
        json_mkdir = json.dumps(mkdir_data)

        self.client.publish(REQUEST_MKDIR_TOPIC, json_mkdir, qos=2)
        response = sync("mkdir", self.pending_requests)

        if response == "0":
            return None
        else:
            raise FuseOSError(int(response))

    def rmdir(self, path):

        self.client.publish(REQUEST_RMDIR_TOPIC, path, qos=2)
        response = sync("rmdir", self.pending_requests)

        if response == "0":
            return None
        else:
            raise FuseOSError(int(response))


if __name__ == "__main__":

    subprocess.run(['fusermount', '-uz', CLIENT_PATH], capture_output=False, text=True) #A veces no se desmonta el volumen aunque detengas el script
    #TODO: borrar estas lineas para ejecutar comandos en bash
    #subprocess.run(['rmdir', CLIENT_PATH], capture_output=False, text=True)
    #subprocess.run(['mkdir', CLIENT_PATH], capture_output=False, text=True)
    fuse = FUSE(MqttFS(), CLIENT_PATH, foreground=True, nothreads=True, debug = True)

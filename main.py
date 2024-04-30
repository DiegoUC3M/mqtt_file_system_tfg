import paho.mqtt.client as mqtt
import json
import time
from fuse import FUSE, FuseOSError, Operations
import subprocess #para el comando de fusermount
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

FTRUNCATE_TOPIC = "/topic/ftruncate"
REQUEST_FTRUNCATE_TOPIC = "/topic/request/ftruncate"

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

CHOWN_TOPIC = "/topic/chown"
REQUEST_CHOWN_TOPIC = "/topic/request/chown"

CHMOD_TOPIC = "/topic/chmod"
REQUEST_CHMOD_TOPIC = "/topic/request/chmod"

MKDIR_TOPIC = "/topic/mkdir"
REQUEST_MKDIR_TOPIC = "/topic/request/mkdir"

RMDIR_TOPIC = "/topic/rmdir"
REQUEST_RMDIR_TOPIC = "/topic/request/rmdir"

TRUNCATE_TOPIC = "/topic/truncate"
REQUEST_TRUNCATE_TOPIC = "/topic/request/truncate"

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
        self.client.publish(REQUEST_READ_DIR_TOPIC, path, qos=2)  # TODO: decidir que qos implemento ?
                                                                                # TODO: que mensaje mando? ese no se utiliza para nada

        response = sync("readDir", self.pending_requests)

        listdir = json.loads(response)

        return listdir


    def getattr(self, path, fh=None):

        self.client.publish(REQUEST_GETATTR_TOPIC, path, qos=2)  # TODO: decidir que qos implemento ?

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
        self.client.publish(REQUEST_OPEN_TOPIC, json_open, qos=2) # TODO: decidir que qos implemento ?

        response = sync("open", self.pending_requests)

        file_handle = json.loads(response)

        return file_handle

    def read(self, path, size, offset, fh):

        read_data = {"fh": fh, "size": size}
        json_read = json.dumps(read_data)

        self.client.publish(REQUEST_READ_TOPIC, json_read, qos=2)  # Solicitamos lectura.      Para el read considero qos --> At least once

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

        return num_bytes_written

    def ftruncate(self, path, length, fh):

        ftruncate_data = {"fh": fh, "length": length}
        json_ftruncate = json.dumps(ftruncate_data)
        self.client.publish(REQUEST_FTRUNCATE_TOPIC, json_ftruncate, qos=2)  # TODO: ver que qos implemento

        return None

    def truncate(self, path, length):
        truncate_data = {"path": path, "length": length}
        json_truncate = json.dumps(truncate_data)
        self.client.publish(REQUEST_TRUNCATE_TOPIC, json_truncate, qos=2)  # TODO: ver que qos implemento

        return None


    def create(self, path, mode, fi=None):
        create_data = {"path": path, "mode":mode}

        json_create = json.dumps(create_data)

        self.client.publish(REQUEST_CREATE_TOPIC, json_create,
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

        if response == "0":
            return None
        else:
            #Si ha fallado la operacion de rename mandaremos error de que el sistema es read-only file system por defecto
            raise FuseOSError(int(response))

    # PARA EL CIERRE DE ARCHIVOS:

    # necesario para los .swp que se generan despues del create
    def unlink(self, path):

        self.client.publish(REQUEST_UNLINK_TOPIC, path, qos=2)  #TODO ver que qos implemento

        response = sync("unlink", self.pending_requests)

        if response == "0":
            return None
        else:
            #Si ha fallado la operacion de unlink mandaremos error de que el sistema es read-only file system por defecto
            raise FuseOSError(int(response))



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

    #PERMISOS:

    def chmod(self, path, mode):
        chmod_data = {"path": path, "mode": mode}
        json_chmod = json.dumps(chmod_data)

        self.client.publish(REQUEST_CHMOD_TOPIC, json_chmod,
                            qos=2)  # TODO ver que qos implemento

        response = sync("chmod", self.pending_requests)

        if response == "0":
            return None
        else:
            raise FuseOSError(int(response))


    #TODO: comprobar si no funciona al estar el directorio montado en mi espacio de usuario, y por ese motivo otro usuario
    #TODO: no puede acceder, al no tener permisos de r-x para mi directorio home
    def chown(self, path, uid, gid):
        chown_data = {"path": path, "uid": uid, "gid": gid}
        json_chown = json.dumps(chown_data)

        self.client.publish(REQUEST_CHOWN_TOPIC, json_chown,
                            qos=2)  # TODO ver que qos implemento

        response = sync("chown", self.pending_requests)

        if response == "0":
            return None
        else:
           raise FuseOSError(int(response))

    #OPERACIONES PARA DIRECTORIO:
    
    #TODO: comprobar si se puede sustituir por ACCESS, o si es necesario realmente
    '''
    def opendir(self, path):

        return 0
    
    def releasedir(self, path, fh):
        return 0
    '''
    def mkdir(self, path, mode):
        mkdir_data = {"path": path, "mode": mode}
        json_mkdir = json.dumps(mkdir_data)

        self.client.publish(REQUEST_MKDIR_TOPIC, json_mkdir,
                            qos=2)  # TODO ver que qos implemento

        response = sync("mkdir", self.pending_requests)

        if response == "0":
            return None
        else:
            raise FuseOSError(int(response))

    def rmdir(self, path):

        self.client.publish(REQUEST_RMDIR_TOPIC, path,
                            qos=2)  # TODO ver que qos implemento

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

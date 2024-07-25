import errno
import os
import paho.mqtt.client as mqtt
import json
import subprocess #para el comando de mosquitto
import base64
from constants import *
from functools import partial

SERVER_PATH = "/home/diego/PycharmProjects/mqtt_test/directorio_servidor"


# Descartados
# mknod (crear i-nodos, tuberias fifo, etc)
# ioctl
# init. Inicializar fuse --> Use it instead of __init__ if you start threads on initialization.

# Duda
# utimens. Con stat parece que hay implementacion por defecto, menos para la fecha de creacion
# destroy. Sirve para desmontar el sistema de archivos, entiendo que esta implementado por defecto
# statfs. No tiene implementacion por defecto, aunque no muestra error. se puede comprobar con --> df -h \. ------------- PUEDE SER INTERESANTE IMPLEMENTARLA
        #muestra estadisticas del sistema de ficheros montados (espacio total, espacio libre, etc)
# getxattr, listxattr, removexattr, setxattr --> kernel llama a getxattr para capabilities/selinux, pero se lanza una excepcion y continua la ejecucion. Es llamado por ejemplo
                                                                                                                                                    #despues de un ls -lah
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Se ha conectado al broker con exito\n")
        client.subscribe(REQUEST_WRITE_TOPIC)
        client.subscribe(REQUEST_READ_TOPIC)
        client.subscribe(REQUEST_READ_DIR_TOPIC)
        client.subscribe(REQUEST_GETATTR_TOPIC)
        client.subscribe(REQUEST_OPEN_TOPIC)
        client.subscribe(REQUEST_CREATE_TOPIC)
        client.subscribe(REQUEST_RENAME_TOPIC)
        client.subscribe(REQUEST_UNLINK_TOPIC)
        client.subscribe(REQUEST_FLUSH_FSYNC_TOPIC)
        client.subscribe(REQUEST_RELEASE_TOPIC)
        client.subscribe(REQUEST_CHOWN_TOPIC)
        client.subscribe(REQUEST_CHMOD_TOPIC)
        client.subscribe(REQUEST_MKDIR_TOPIC)
        client.subscribe(REQUEST_RMDIR_TOPIC)
        client.subscribe(REQUEST_TRUNCATE_TOPIC)
        client.subscribe(REQUEST_ACCESS_TOPIC)
        client.subscribe(REQUEST_SYMLINK_TOPIC)
        client.subscribe(REQUEST_LINK_TOPIC)
        client.subscribe(REQUEST_READLINK_TOPIC)
    else:
        print("Error al intentar conectase al broker")


def os_func(client, topic, func, *args):

    os_result = None

    try:
        os_result = func(*args)

        if os_result != None:
            os_result = json.dumps({"os_result": os_result})
        # Si la funcion de la libreria 'os' no retorna nada, se manda un 0 para manejo de excepciones:
        else:
            os_result = "0"
    except OSError as e:
        os_result = json.dumps(e.errno)

    finally:

        client.publish(topic, os_result, qos=1)


        #Cliente MQTT Logging:
        if len(args)==1:
            args = args[0] #para no dejar tuplas con una posicion vacia. Pe: al hacer close este valor era (file_descriptor, )
        info_logging = {"topic": topic, "func": func.__name__, "args": args}

        #Cuando os_result es directamente un diccionario
        try:
            info_logging.update(os_result)
        except ValueError:
            #cuando os_result es 0 o codigo de error:
            info_logging["os_result"] = os_result

        client.publish(LOGGING_FS_TOPIC, json.dumps(info_logging), qos=2)


#TODO: organizar mejor las funciones, darles un orden
#TODO: mejorar los tiempos. Sobre todo al principio de la ejecucion del programa que busca archivos que no necesita. Tambien sospecho que puede ser por una papelera muy grande --> Trash-1000



def on_message(client, userdata, msg):
    topic = msg.topic.split('/')

    if topic[-1] == "open":

        json_open = msg.payload.decode()
        open_data = json.loads(json_open)
        os_func(client, OPEN_TOPIC, os.open, SERVER_PATH + open_data["path"], open_data["flags"])


    if topic[-1] == "write":

        json_data = msg.payload.decode()
        dict_data = json.loads(json_data)
        fh = dict_data["file_handle"]
        write_data = base64.b64decode(dict_data["text"])

        try:
            os_result = os.write(fh, write_data)
            client.publish(WRITE_TOPIC, json.dumps({"num_bytes_written": os_result}), qos=1)
        except OSError as e:
            os_result = json.dumps(e.errno)
            client.publish(WRITE_TOPIC, os_result, qos=1)

        finally:
            info_logging = {"topic": WRITE_TOPIC, "func": "write", "args": (fh,dict_data["text"]), "os_result": json.dumps({"os_result": os_result})}
            client.publish(LOGGING_FS_TOPIC, json.dumps(info_logging), qos=2)


    if topic[-1] == "truncate":
        json_truncate = msg.payload.decode()
        truncate_data = json.loads(json_truncate)

        os_func(client, TRUNCATE_TOPIC, os.truncate, SERVER_PATH + truncate_data["path"], truncate_data["length"])

    if topic[-1] == "create":
        json_create = msg.payload.decode()
        create_data = json.loads(json_create)

        os_func(client, CREATE_TOPIC, os.open, SERVER_PATH + create_data["path"], os.O_WRONLY | os.O_CREAT, create_data["mode"])


    if topic[-1] == "read":
        json_read = msg.payload.decode()
        read_data = json.loads(json_read)

        # Para el caso de lectura no quiero crear el fichero si no existe, por lo que hago manejo de excepciones:
        try:
            datos_leidos = os.read(read_data["fh"], read_data["size"])

            #Mandamos la informacion codificada en base64 para aquellos ficheros que no tienen codifcacion ascii (los ficheros .swp por ejemplo)
            os_result = base64.b64encode(datos_leidos).decode()
            json_read = json.dumps({"datos_b64": os_result})

            # Publicamos los datos para que los reciba el cliente que ha solicitado la lectura:
            client.publish(READ_TOPIC, json_read, qos=2)

        except OSError as e:
            os_result = json.dumps(e.errno)
            client.publish(READ_TOPIC, os_result, qos=1)

        finally:
            info_logging = {"topic": READ_TOPIC, "func": "read", "args": (read_data["fh"], read_data["size"]), "os_result": json.dumps({"os_result": os_result})}
            client.publish(LOGGING_FS_TOPIC, json.dumps(info_logging), qos=2)


    if topic[-1] == "readDir":

        file_path = msg.payload.decode()
        parentPath = [".", ".."]

        try:
            files = os.listdir(SERVER_PATH + file_path)
            parentPath.extend(files)
            os_result = json.dumps(parentPath)
            client.publish(READDIR_TOPIC, os_result, qos=1)
        except OSError as e:
            os_result = json.dumps(e.errno)
            client.publish(READDIR_TOPIC, os_result, qos=1)

        finally:
            info_logging = {"topic": READDIR_TOPIC, "func": "listdir", "args": SERVER_PATH + file_path, "os_result": os_result}
            client.publish(LOGGING_FS_TOPIC, json.dumps(info_logging), qos=2)


    if topic[-1] == "getattr":
        file_path = msg.payload.decode()

        #Importante hacer manejo de excepciones en getattr. Cuando se escribe, se genera un fichero ".goutputstream", y sin embargo se llama al getattr antes que al create
        try :
            file_attr_struct = os.lstat(SERVER_PATH + file_path)  # lstat devuelve información sobre el enlace simbólico mismo, no el archivo o directorio al que apunta
            file_attr_dict = {}

            for k in ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'):
                file_attr_dict[k] = getattr(file_attr_struct, k)

            os_result = json.dumps(file_attr_dict)
            client.publish(GETATTR_TOPIC, os_result, qos=1)

        except OSError as e:
            #no se manda el error porque    -->    TypeError: Object of type type is not JSON serializable
            os_result = json.dumps(e.errno)
            client.publish(GETATTR_TOPIC, os_result, qos=1)

        finally:
            info_logging = {"topic": GETATTR_TOPIC, "func": "lstat", "args": SERVER_PATH + file_path, "os_result": os_result}
            client.publish(LOGGING_FS_TOPIC, json.dumps(info_logging), qos=2)


    if topic[-1] == "rename":
        json_rename = msg.payload.decode()
        rename_data = json.loads(json_rename)
        os_func(client, RENAME_TOPIC, os.rename, SERVER_PATH + rename_data["old"], SERVER_PATH + rename_data["new"])



    if topic[-1] == "unlink":
        path = msg.payload.decode()
        os_func(client, UNLINK_TOPIC, os.unlink, SERVER_PATH + path)


    if topic[-1] == "flush_fsync":
        fh = msg.payload.decode()
        os_func(client, FLUSH_FSYNC_TOPIC, os.fsync, int(fh))


    if topic[-1] == "release":

        fh = msg.payload.decode()
        os_func(client, RELEASE_TOPIC, os.close, int(fh))

    if topic[-1] == "chown":
        json_chown = msg.payload.decode()
        chown_data = json.loads(json_chown)
        os_func(client, CHOWN_TOPIC, os.chown, SERVER_PATH + chown_data["path"], chown_data["uid"], chown_data["gid"])



    if topic[-1] == "chmod":
        json_chmod = msg.payload.decode()
        chmod_data = json.loads(json_chmod)
        os_func(client, CHMOD_TOPIC, os.chmod, SERVER_PATH + chmod_data["path"], chmod_data["mode"])

    if topic[-1] == "mkdir":
        json_mkdir = msg.payload.decode()
        mkdir_data = json.loads(json_mkdir)
        os_func(client, MKDIR_TOPIC, os.mkdir, SERVER_PATH + mkdir_data["path"], mkdir_data["mode"])

    if topic[-1] == "rmdir":
        path = msg.payload.decode()
        os_func(client, RMDIR_TOPIC, os.rmdir, SERVER_PATH + path)


    if topic[-1] == "access":
        json_access = msg.payload.decode()
        access_data = json.loads(json_access)

        try:
            os_result = os.access(SERVER_PATH + access_data["path"], access_data["mode"])
            client.publish(ACCESS_TOPIC, json.dumps(os_result), qos=1)
        except OSError as e:
            os_result = e.errno
            client.publish(ACCESS_TOPIC, json.dumps(os_result), qos=1)

        finally:
            info_logging = {"topic": ACCESS_TOPIC, "func": "access", "args": (SERVER_PATH + access_data["path"], access_data["mode"]), "os_result": json.dumps({"os_result": os_result})}
            client.publish(LOGGING_FS_TOPIC, json.dumps(info_logging), qos=2)


    if topic[-1] == "symlink":
        json_symlink = msg.payload.decode()
        symlink_data = json.loads(json_symlink)
        #El source aparece sin el path relativo, por eso el "/"
        os_func(client, SYMLINK_TOPIC, os.symlink, SERVER_PATH + "/" + symlink_data["source"], SERVER_PATH + symlink_data["target"])

    if topic[-1] == "link":
        json_link = msg.payload.decode()
        link_data = json.loads(json_link)
        os_func(client, LINK_TOPIC, os.link, SERVER_PATH + link_data["source"], SERVER_PATH + link_data["target"])

    if topic[-1] == "readlink":
        path = msg.payload.decode()
        os_func(client, READLINK_TOPIC, os.readlink, SERVER_PATH + path)

def main():
    subprocess.run(['gnome-terminal', '--', 'bash', '-c', 'mosquitto', '-v'], capture_output=True, text=True)
    #subprocess.Popen(['xfce4-terminal', '-e', 'bash -c "mosquitto -v"'])
    client = mqtt.Client(client_id="file_manager")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("localhost", 1883, 60)
    client.loop_forever()


if __name__ == "__main__":
    main()

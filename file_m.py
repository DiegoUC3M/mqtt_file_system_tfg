import errno
import os
import paho.mqtt.client as mqtt
import json
import subprocess #para el comando de mosquitto
import base64
from constants import *
from functools import partial

SERVER_PATH = "/home/diego/PycharmProjects/mqtt_test/directorio_servidor"



def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Se ha conectado al broker con exito\n")
        client.subscribe(REQUEST_WRITE_TOPIC)
        client.subscribe(REQUEST_READ_TOPIC)
        client.subscribe(REQUEST_READ_DIR_TOPIC)
        client.subscribe(REQUEST_GETATTR_TOPIC)
        client.subscribe(REQUEST_OPEN_TOPIC)
        client.subscribe(REQUEST_FTRUNCATE_TOPIC)
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
    else:
        print("Error al intentar conectase al broker")

#TODO: pensar una forma mas generica de implementar algunas de las funciones, o pensar si merece
                                            #la pena, ya que muchas de ellas se parecen bastante

def os_func(client, topic, func, *args):

    try:
        os_result = func(*args)

        if os_result != None:
            os_result_json = json.dumps({"os_result": os_result})
            client.publish(topic, os_result_json, qos=1)
        #Si la funcion de la libreria os no funciona nada, se manda un 0 para manejo de excepciones:
        else:
            client.publish(topic, "0", qos=1)
    except OSError as e:
        err_code = json.dumps(e.errno)
        client.publish(topic, err_code, qos=1)

def on_message(client, userdata, msg):
    topic = msg.topic.split('/')

    if topic[-1] == "open":

        json_open = msg.payload.decode()
        open_data = json.loads(json_open)
        '''
        try:
            fd = os.open(SERVER_PATH + open_data["path"], open_data["flags"])
            fd_json = json.dumps({"fd": fd})
            client.publish(OPEN_TOPIC, fd_json, qos=1)
        except OSError as e:
            err_code = json.dumps(e.errno)
            client.publish(OPEN_TOPIC, err_code, qos=1)
        '''
        os_func(client, OPEN_TOPIC, os.open, SERVER_PATH + open_data["path"], open_data["flags"])


    if topic[-1] == "write":

        json_data = msg.payload.decode()
        dict_data = json.loads(json_data)
        fh = dict_data["file_handle"]

        write_data = base64.b64decode(dict_data["text"])

        try:
            num_bytes_written = os.write(fh, write_data)
            client.publish(WRITE_TOPIC, json.dumps({"num_bytes_written": num_bytes_written}), qos=1)
        except OSError as e:
            err_code = json.dumps(e.errno)
            client.publish(WRITE_TOPIC, err_code, qos=1)



    if topic[-1] == "ftruncate":
        json_ftruncate = msg.payload.decode()
        ftruncate_data = json.loads(json_ftruncate)

        os_func(client, FTRUNCATE_TOPIC, os.ftruncate, ftruncate_data["fh"], ftruncate_data["length"])


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

            #Mandamos la informaicon codificada en base64 para aquellos ficheros que no tienen codifcacion ascii (los ficheros .swp por ejemplo)
            datos_b64 = base64.b64encode(datos_leidos).decode()
            json_read = json.dumps({"datos_b64": datos_b64})

            # Publicamos los datos para que los reciba el cliente que ha solicitado la lectura:
            client.publish(READ_TOPIC, json_read, qos=2)

        except OSError as e:
            err_code = json.dumps(e.errno)
            client.publish(READ_TOPIC, err_code, qos=1)


    if topic[-1] == "readDir":

        file_path = msg.payload.decode()
        parentPath = [".", ".."]

        try:
            files = os.listdir(SERVER_PATH + file_path)
            parentPath.extend(files)
            files_json = json.dumps(parentPath)
            client.publish(READDIR_TOPIC, files_json, qos=1)
        except OSError as e:
            err_code = json.dumps(e.errno)
            client.publish(READDIR_TOPIC, err_code, qos=1)


    if topic[-1] == "getattr":
        file_path = msg.payload.decode()

        #Importante hacer manejo de excepciones en getattr. Cuando se escribe, se genera un fichero ".goutputstream", y sin embargo se llama al getattr antes que al create
        try :
            file_attr_struct = os.stat(SERVER_PATH + file_path)  # lstat devuelve información sobre el enlace simbólico mismo, no el archivo o directorio al que apunta
            file_attr_dict = {}

            for k in ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'): #TODO: citar esto ?? (copiado de internet)
                file_attr_dict[k] = getattr(file_attr_struct, k)

            files_attr_json = json.dumps(file_attr_dict)
            client.publish(GETATTR_TOPIC, files_attr_json, qos=1)

        except OSError as e:
            #no se manda el error porque    -->    TypeError: Object of type type is not JSON serializable
            err_code = json.dumps(e.errno)
            client.publish(GETATTR_TOPIC, err_code, qos=1)


    if topic[-1] == "rename":
        json_rename = msg.payload.decode()
        rename_data = json.loads(json_rename)

        '''
        try:
            os.rename(SERVER_PATH + rename_data["old"], SERVER_PATH + rename_data["new"])
            #Hay que publicar igualmente aunque en caso de exito no devuelva nada, para que en el sync no se quede en el bucle indefinidamente y poder cubrir el caso de error (el de poder
                                                                                                                                                              #mandar el 1 si falla)
            client.publish(RENAME_TOPIC, "0", qos=1)
        except OSError as e:
            client.publish(RENAME_TOPIC, e.errno, qos=1)
        '''
        os_func(client, RENAME_TOPIC, os.rename, SERVER_PATH + rename_data["old"], SERVER_PATH + rename_data["new"])



    if topic[-1] == "unlink":
        path = msg.payload.decode()
        '''
        try:
            os.unlink(SERVER_PATH + path)
            client.publish(UNLINK_TOPIC, "0", qos=1)
        except OSError as e:
            client.publish(UNLINK_TOPIC, e.errno, qos=1)
        '''
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

        '''
        try:
            os.chown(SERVER_PATH + chown_data["path"], chown_data["uid"], chown_data["gid"])
            client.publish(CHOWN_TOPIC, "0", qos=1)
        except OSError as e:
            client.publish(CHOWN_TOPIC, e.errno, qos=1)
        '''
        os_func(client, CHOWN_TOPIC, os.chown, SERVER_PATH + chown_data["path"], chown_data["uid"], chown_data["gid"])



    if topic[-1] == "chmod":
        json_chmod = msg.payload.decode()
        chmod_data = json.loads(json_chmod)

        '''
        try:
            os.chmod(SERVER_PATH + chmod_data["path"], chmod_data["mode"])
            client.publish(CHMOD_TOPIC, "0", qos=1)
        except OSError as e:
            client.publish(CHMOD_TOPIC, e.errno, qos=1)
        '''
        os_func(client, CHMOD_TOPIC, os.chmod, SERVER_PATH + chmod_data["path"], chmod_data["mode"])

    if topic[-1] == "mkdir":
        json_mkdir = msg.payload.decode()
        mkdir_data = json.loads(json_mkdir)

        '''
        try:
            os.mkdir(SERVER_PATH + mkdir_data["path"], mkdir_data["mode"])
            client.publish(MKDIR_TOPIC, "0", qos=1)

        #por ejemplo, si tratas de crear un directorio con el nombre de un directorio ya existente
        except OSError as e:
            client.publish(MKDIR_TOPIC, e.errno, qos=1)
        '''
        os_func(client, MKDIR_TOPIC, os.mkdir, SERVER_PATH + mkdir_data["path"], mkdir_data["mode"])

    if topic[-1] == "rmdir":
        path = msg.payload.decode()

        '''
        try:
            os.rmdir(SERVER_PATH + path)
            client.publish(RMDIR_TOPIC, "0", qos=1)

        # por ejemplo, si tratas de eliminar un directorio con el nombre de un directorio no existente
        # o si tratas de eliminar un directorio que aun contiene ficheros dentro
        except OSError as e:
            client.publish(RMDIR_TOPIC, e.errno, qos=1)
        '''
        os_func(client, RMDIR_TOPIC, os.rmdir, SERVER_PATH + path)



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

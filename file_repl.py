import errno
import os
import paho.mqtt.client as mqtt
import json
import subprocess #para el comando de mosquitto
import base64
from constants import *
from functools import partial

SERVER_PATH = "/home/diego/PycharmProjects/mqtt_test/directorio_replicacion"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Se ha conectado al broker con exito\n")
        client.subscribe(REQUEST_WRITE_TOPIC)
        client.subscribe(REQUEST_OPEN_TOPIC)
        client.subscribe(REQUEST_CREATE_TOPIC)
        client.subscribe(REQUEST_RENAME_TOPIC)
        client.subscribe(REQUEST_UNLINK_TOPIC)
        client.subscribe(REQUEST_FLUSH_FSYNC_TOPIC)
        client.subscribe(REQUEST_RELEASE_TOPIC)
        client.subscribe(REQUEST_CHMOD_TOPIC)
        client.subscribe(REQUEST_MKDIR_TOPIC)
        client.subscribe(REQUEST_RMDIR_TOPIC)
        client.subscribe(REQUEST_TRUNCATE_TOPIC)
        client.subscribe(REQUEST_SYMLINK_TOPIC)
        client.subscribe(REQUEST_LINK_TOPIC)
    else:
        print("Error al intentar conectase al broker")


# SCRIPT REDUCIDO DE FILE_M PARA HACER REPLICACION. COMO SU UNICA FUNCIONALIDAD ES LA DE REPLICAR LOS DATOS, DESDE
# ESTE SCRIPT NO SE PUBLICA EN NINGUN OTRO TOPIC, Y ADEMAS LAS OPERACIONES RELACIONADAS CON LECTURA SE HAN ELIMINADO.
# COMO RESULTADO SE OBTIENE ESTE SCRIPT PEQUEÑITO PERO QUE ES DE GRAN AYUDA PARA LA TOLERANCIA A FALLOS

def on_message(client, userdata, msg):
    topic = msg.topic.split('/')

    try:
        if topic[-1] == "open":

            json_open = msg.payload.decode()
            open_data = json.loads(json_open)
            os.open(SERVER_PATH + open_data["path"], open_data["flags"])


        if topic[-1] == "write":

            json_data = msg.payload.decode()
            dict_data = json.loads(json_data)
            fh = dict_data["file_handle"]
            write_data = base64.b64decode(dict_data["text"])
            os.write(fh, write_data)



        if topic[-1] == "truncate":
            json_truncate = msg.payload.decode()
            truncate_data = json.loads(json_truncate)

            os.truncate(SERVER_PATH + truncate_data["path"], truncate_data["length"])

        if topic[-1] == "create":
            json_create = msg.payload.decode()
            create_data = json.loads(json_create)
            os.open(SERVER_PATH + create_data["path"], os.O_WRONLY | os.O_CREAT, create_data["mode"])


        if topic[-1] == "rename":
            json_rename = msg.payload.decode()
            rename_data = json.loads(json_rename)
            os.rename(SERVER_PATH + rename_data["old"], SERVER_PATH + rename_data["new"])


        if topic[-1] == "unlink":
            path = msg.payload.decode()
            os.unlink(SERVER_PATH + path)


        if topic[-1] == "flush_fsync":
            fh = msg.payload.decode()
            os.fsync(int(fh))


        if topic[-1] == "release":

            fh = msg.payload.decode()
            os.close(int(fh))

        if topic[-1] == "chmod":
            json_chmod = msg.payload.decode()
            chmod_data = json.loads(json_chmod)
            os.chmod(SERVER_PATH + chmod_data["path"], chmod_data["mode"])

        if topic[-1] == "mkdir":
            json_mkdir = msg.payload.decode()
            mkdir_data = json.loads(json_mkdir)
            os.mkdir(SERVER_PATH + mkdir_data["path"], mkdir_data["mode"])

        if topic[-1] == "rmdir":
            path = msg.payload.decode()
            os.rmdir(SERVER_PATH + path)


        if topic[-1] == "symlink":
            json_symlink = msg.payload.decode()
            symlink_data = json.loads(json_symlink)
            os.symlink(SERVER_PATH + "/" + symlink_data["source"], SERVER_PATH + symlink_data["target"])

        if topic[-1] == "link":
            json_link = msg.payload.decode()
            link_data = json.loads(json_link)
            os.link(SERVER_PATH + link_data["source"], SERVER_PATH + link_data["target"])

    except OSError as e:
        pass
        print("Ha pasado por aqui")
        print(e)


def main():
    client = mqtt.Client(client_id="file_repl")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("localhost", 1883, 60)
    client.loop_forever()


if __name__ == "__main__":
    main()

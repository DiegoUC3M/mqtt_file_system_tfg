import os
import paho.mqtt.client as mqtt
import json
import subprocess #para el comando de mosquitto
import re #TODO ver si se elimina, es para el regex del write para los .swp
import base64 #TODO ver si se elimina

REQUEST_WRITE_TOPIC = "/topic/request/write"
WRITE_TOPIC = "/topic/write"
READ_TOPIC = "/topic/read"
REQUEST_READ_TOPIC = "/topic/request/read"
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

SERVER_PATH = "/home/diego/PycharmProjects/mqtt_test/directorio_servidor"


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Se ha conectado al broker con exito\n")
        client.subscribe(REQUEST_WRITE_TOPIC)
        client.subscribe(REQUEST_READ_TOPIC)
        client.subscribe(REQUEST_READ_DIR_TOPIC)
        client.subscribe(REQUEST_GETATTR_TOPIC)
        client.subscribe(REQUEST_OPEN_TOPIC)
        client.subscribe(REQUEST_TRUNCATE_TOPIC)
        client.subscribe(REQUEST_CREATE_TOPIC)
        client.subscribe(REQUEST_RENAME_TOPIC)
        client.subscribe(REQUEST_UNLINK_TOPIC)
        client.subscribe(REQUEST_FLUSH_FSYNC_TOPIC)
        client.subscribe(REQUEST_RELEASE_TOPIC)
    else:
        print("Error al intentar conectase al broker")

#TODO: limpiar todos los msg.payload.decode() que realmente no son necesarios

def on_message(client, userdata, msg):
    topic = msg.topic.split('/')

    if topic[-1] == "open":

        json_open = msg.payload.decode()
        open_data = json.loads(json_open)

        fd = os.open(SERVER_PATH + open_data["path"], open_data["flags"]) #TODO: Como hago para los flags a la hora de determinar si lo quiero para leer o escribir ???

        client.publish(OPEN_TOPIC, fd, qos=2)  # TODO: decidir que qos implemento ?

    if topic[-1] == "write":

        json_data = msg.payload.decode()
        dict_data = json.loads(json_data)
        fh = dict_data["file_handle"]

        write_data = base64.b64decode(dict_data["text"])

        num_bytes_written = os.write(fh, write_data)
        client.publish(WRITE_TOPIC, num_bytes_written, qos=2)
        #os.close(fd) #TODO: implementar el close y quitar esta linea

    if topic [-1] == "truncate" :
        json_read = msg.payload.decode()
        truncate_data = json.loads(json_read)

        truncate_op = os.ftruncate(truncate_data["fh"], truncate_data["length"])
        client.publish(TRUNCATE_TOPIC, truncate_op, qos=2)

    if topic[-1] == "create":
        print("llama a create")
        json_create = msg.payload.decode()
        create_data = json.loads(json_create)

        file_handle = os.open(SERVER_PATH + create_data["path"],  os.O_WRONLY | os.O_CREAT, create_data["mode"])
        client.publish(CREATE_TOPIC, file_handle, qos=2)


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

        #TODO: manejar las excepciones de la v1
        except FileNotFoundError:
            error_message = f"No existe ningun fichero con nombre: read_data[fh]. Por favor, introduzca un file_name correcto"
            client.publish(READ_TOPIC, error_message, qos=1)
            print(error_message)

    if topic[-1] == "readDir":
        parentPath = [".", ".."]
        files = os.listdir(SERVER_PATH)
        parentPath.extend(files)

        files_json = json.dumps(parentPath)

        client.publish(READDIR_TOPIC, files_json, qos=2)  # TODO: decidir que qos implemento ?


    if topic[-1] == "getattr":
        file_path = msg.payload.decode()

        #Importante hacer manejo de excepciones en getattr. Cuando se escribe, se genera un fichero ".goutputstream", y sin embargo se llama al getattr antes que al create
        try :
            file_attr_struct = os.stat(SERVER_PATH + file_path)  # lstat devuelve información sobre el enlace simbólico mismo, no el archivo o directorio al que apunta
            file_attr_dict = {}

            for k in ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'): #TODO: citar esto ?? (copiado de internet)
                file_attr_dict[k] = getattr(file_attr_struct, k)

            files_attr_json = json.dumps(file_attr_dict)
            client.publish(GETATTR_TOPIC, files_attr_json, qos=2)  # TODO: decidir que qos implemento ?

        except FileNotFoundError:
            #no se manda el error porque    -->    TypeError: Object of type type is not JSON serializable
            client.publish(GETATTR_TOPIC,"1", qos=2)


    if topic[-1] == "rename":
        json_rename = msg.payload.decode()
        rename_data = json.loads(json_rename)

        try:
            os.rename(SERVER_PATH + rename_data["old"], SERVER_PATH + rename_data["new"])
            #Hay que publicar igualmente aunque en caso de exito no devuelva nada, para que en el sync no se quede en el bucle indefinidamente y poder cubrir el caso de error (el de poder
                                                                                                                                                              #mandar el 1 si falla)
            client.publish(RENAME_TOPIC, "0", qos=2)
        except OSError:
            client.publish(RENAME_TOPIC, "1", qos=2)


    if topic[-1] == "unlink":
        path = msg.payload.decode()
        try:
            os.unlink(SERVER_PATH + path)
            client.publish(UNLINK_TOPIC, "0", qos=2)
        except OSError:
            client.publish(UNLINK_TOPIC, "1", qos=2)

    if topic[-1] == "flush_fsync":

        fh = msg.payload.decode()
        os.fsync(int(fh))

    if topic[-1] == "release":

        fh = msg.payload.decode()
        os.close(int(fh))





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
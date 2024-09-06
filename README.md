# MQTT-FS

## Descripción

El objetivo de este proyecto es montar un sistema de ficheros distribuido donde el lado cliente y el lado servidor se comunican mediante MQTT. Además hay dos clientes MQTT adicionales: uno que muestra como se podrían replicar los datos, y otro que actúa como logger. A continuación se explica brevemente el propósito de los ficheros principales:

- **fuse_client.py:** este es el lado cliente del sistema de ficheros distribuido. Para poder ofrecer una interfaz al usuario con la que interactuar con el sistema de ficheros, se hace uso de FUSE.
- **file_manager.py:** el lado servidor del sistema de ficheros distribuido.
- **file_replicator.py:** cliente que se puede utilizar para replicar los datos.
- **logging_client/logging_mqtt.py**: cliente que registra una variedad de registros en ficheros log.

### Ficheros adicionales

En el repositorio también se encuentran los siguientes ficheros:

- **constants.py:** aquí se encuentran disponibles todos los tópicos donde se intercambian los mensajes MQTT.
- **config.py:** las variables de configuración necesarias para establecer la conexión con el Broker MQTT.

## Implantación

### Instalación de dependencias

Para poder desplegar la solución simplemente es necesario tener python instalado, así como la biblioteca fusepy y paho-mqtt.

```bash
pip install fusepy
pip install "paho-mqtt<2.0.0"
```

### Despliegue

Después de configurar el broker y sus variables mediante el fichero **config.py,** basta con desplegarlo y ejecutar los ficheros Python (los clientes MQTT en este caso). Estos se deben ejecutar con un argumento donde se indica la ubicación del directorio donde se quiere montar el sistema de ficheros. También se pueden establecer como variables globales dentro del script. Ejemplo:

`python3 ./fuse_client.py "/path/absoluto”`

---

## Description

The goal of this project is to set up a distributed file system where the client and server communicate via MQTT. Additionally, there are two other MQTT clients: one responsible for data replication, and another that functions as a logger. Below is a brief explanation of the purpose of the main files:

- **fuse_client.py:** this is the client side of the distributed file system. It uses FUSE to provide a user interface for interacting with the file system.
- **file_manager.py:** the server side of the distributed file system.
- **file_replicator.py:** a client that can be used for data replication.
- **logging_client/logging_mqtt.py**: a client that logs various records into log files.

### Additional Files

The repository also contains the following files:

- **constants.py:** contains all the MQTT topics where messages are exchanged.
- **config.py:** the configuration variables needed to establish a connection with the MQTT broker.

## Deployment

### Dependency Installation

To deploy the solution, you simply need Python installed along with the fusepy and paho-mqtt libraries.

```bash
pip install fusepy
pip install "paho-mqtt<2.0.0"
```

### Installation

After configuring the broker and its variables using the **config.py** file, you can proceed to deploy and run the Python files (in this case, the MQTT clients). These need to be executed with an argument indicating the location of the directory where the file system will be mounted. Alternatively, these can be set as global variables within the script. Example:

`python3 ./fuse_client.py "/absolute/path”`
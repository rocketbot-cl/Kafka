



# Kafka

Módulo para producir, consumir y administrar topics de Apache Kafka. Soporta autenticación SASL/SSL para conectarse a Confluent Cloud, Azure Event Hubs, AWS MSK y clusters self-hosted.

*Read this in other languages: [English](README.md), [Português](README.pr.md), [Español](README.es.md)*

## Como instalar este módulo

Para instalar el módulo en Rocketbot Studio, se puede hacer de dos formas:
1. Manual: __Descargar__ el archivo .zip y descomprimirlo en la carpeta modules. El nombre de la carpeta debe ser el mismo al del módulo y dentro debe tener los siguientes archivos y carpetas: \__init__.py, package.json, docs, example y libs. Si tiene abierta la aplicación, refresca el navegador para poder utilizar el nuevo modulo.
2. Automática: Al ingresar a Rocketbot Studio sobre el margen derecho encontrara la sección de **Addons**, seleccionar **Install Mods**, buscar el modulo deseado y presionar install.


## Overview


1. Conectar a Kafka
Configura la conexión al clúster Kafka (servidores, protocolo de seguridad, usuario, contraseña). Puedes usar un identificador para cambiar entre otras conexiones

2. Probar Conexión
Prueba si Rocketbot puede conectarse a Kafka con la configuración indicada, devolviendo true o false; si falla, el motivo se imprime en el log de ejecución

3. Listar Topics
Lista los topics disponibles en Kafka. Un topic es como un canal donde se publican y leen mensajes

4. Crear Topic
Crea un nuevo topic en Kafka, indicando nombre, cantidad de particiones y factor de replicación si corresponde

5. Eliminar Topic
IRREVERSIBLE: elimina un topic de Kafka. Según la configuración del clúster, también borra los mensajes asociados. Verifique bien el nombre del tópico antes de correrlo en producción

6. Describir Topic
Muestra información detallada de un topic: particiones, líder, réplicas, ISR, estado de error y configuración. Sirve para diagnóstico

7. Obtener Offsets
Consulta el offset más viejo y el más nuevo disponible por cada partición de un tópico. Sirve para saber cuántos mensajes hay disponibles o desde qué punto puede leer un consumer

8. Producir Mensaje
Envía un mensaje individual a un topic. Por ejemplo, publicar un JSON con datos de una factura, orden o evento

9. Producir Lote de Mensajes
Envía varios mensajes juntos a un topic. Sirve cuando el bot tiene una lista de registros y quiere publicarlos todos en Kafka

10. Consumir Mensajes
Lee mensajes de uno o varios topics, cada uno con su propio campo 'topic'. Tiene límite de cantidad y de tiempo. El consumer (topics, group ID, offset inicial, max poll interval) se crea en la primera llamada de la sesión y se reutiliza después; ejecute 'Cerrar Consumidor' primero para cambiar esos valores

11. Commit Offsets
Confirma que los mensajes consumidos fueron procesados e indica a Kafka desde dónde continuar la próxima lectura. Por defecto confirma todo el lote de 'Consumir Mensajes'; use 'Mensajes procesados' para confirmar solo hasta el último exitoso

12. Cerrar Consumidor
Cierra la sesión del consumidor y libera sus recursos. Es necesario antes de cambiar la configuración del consumer (Group ID, Offset inicial, Max poll interval) en la próxima llamada a 'Consumir Mensajes'




----
### OS

- windows
- mac
- linux
- docker

### Dependencies

### License

![MIT](https://camo.githubusercontent.com/107590fac8cbd65071396bb4d04040f76cde5bde/687474703a2f2f696d672e736869656c64732e696f2f3a6c6963656e73652d6d69742d626c75652e7376673f7374796c653d666c61742d737175617265)
[MIT](http://opensource.org/licenses/mit-license.ph)
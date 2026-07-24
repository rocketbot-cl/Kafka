## How to use this module

Use this module to produce, consume and administer Apache Kafka topics from Rocketbot. It supports SASL/SSL authentication for Confluent Cloud, Azure Event Hubs, AWS MSK and self-hosted clusters.

Basic usage:
1. Run Connect Kafka with the bootstrap servers and, if required, the security protocol and SASL credentials. Optionally set a Session identifier to keep several connections open at once.
2. Run Test Connection to confirm Rocketbot can reach the cluster before building the rest of the flow.
3. Use the topic commands (List Topics, Create Topic, Delete Topic, Describe Topic, Get Offsets) to manage topics.
4. Use Produce Message / Produce Batch to publish, and Consume Messages / Commit Offsets / Close Consumer to read.

Connection examples:
- Local or self-hosted Kafka without authentication: Bootstrap servers `localhost:9092` or `broker1:9092,broker2:9092`; Security protocol `PLAINTEXT`; leave SASL mechanism, Username and Password empty.
- Self-hosted Kafka with SASL: Bootstrap servers `broker1:9093,broker2:9093`; Security protocol `SASL_SSL` or `SASL_PLAINTEXT` according to the cluster; SASL mechanism `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`; Username and Password from your Kafka user.
- Confluent Cloud: Bootstrap servers from Cluster settings, usually `pkc-xxxxx.region.provider.confluent.cloud:9092`; Security protocol `SASL_SSL`; SASL mechanism `PLAIN`; Username = API key; Password = API secret.
- Azure Event Hubs using Kafka API: Bootstrap servers `<namespace>.servicebus.windows.net:9093`; Security protocol `SASL_SSL`; SASL mechanism `PLAIN`; Username exactly `$ConnectionString`; Password = the full Event Hubs connection string, for example `Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<policy>;SharedAccessKey=<key>`. If the connection string includes `EntityPath`, use the same Event Hub name as the Kafka topic.
- AWS MSK: Use the broker list and authentication mode provided by the cluster. For IAM/OAUTHBEARER authentication this module is not enough as-is; use SCRAM or PLAIN credentials exposed by the cluster.

Sessions:
- Every command takes a Session identifier. Leave it empty to use the default connection, or set one to keep several independent connections/consumers open in the same flow (for example, one per cluster or per consumer group).
- Connect Kafka only stores the connection settings; it does not create the consumer yet.

Producing messages:
- Produce Message sends a single message (key optional; the value can be plain text or JSON).
- Produce Batch sends a JSON list of messages in one call, for example `[{"key": "1", "value": "hello"}, {"key": "2", "value": {"total": 100}}]`. Each item can be a plain value or a `{key, value}` object. Useful when the bot already has a list of records to publish together.

Consuming messages (consumer group flow):
1. Run Consume Messages with Topics (comma-separated), Group ID and Initial offset. These, together with Max poll interval, are only applied the first time the consumer is created for that session; to change them later, run Close Consumer first and call Consume Messages again.
2. Process the returned messages in the bot. Each message includes its own `topic`, `partition`, `offset`, `key` and `value`, so you can tell which topic it came from when subscribed to more than one.
3. Run Commit Offsets to tell Kafka the messages were processed. By default it commits the whole batch; if the bot only processed some of the messages, pass the successful ones in "Processed messages" so only those (and earlier ones) are committed, and the rest are redelivered on the next read.
4. Run Close Consumer when the flow finishes, or before changing Topics, Group ID, Initial offset or Max poll interval on the same session.

Important notes:
- Test Connection never stops the flow: it returns true or false so the bot can branch on the result. On failure, the reason is printed to the execution log, not returned in the result variable.
- Commit Offsets returns false, without raising an error, when there was nothing new to confirm since the last commit -- this is normal, not a failure. If the commit itself fails (for example, the consumer was evicted from the group), the command raises an error instead of returning false.
- Message values are stored and returned exactly as sent, including any whitespace, line breaks or formatting from the original text (Produce Message/Batch do not trim or reformat them, and Consume Messages returns them unchanged).
- Group ID, Initial offset and Max poll interval apply equally to every topic listed in a single Consume Messages call. To use different settings per topic, use a separate session for each one.
- If Commit Offsets runs later than the configured Max poll interval after the last Consume Messages call, Kafka may have already evicted the consumer from the group; run Consume Messages again or increase Max poll interval.
- Delete Topic is irreversible and, depending on the cluster configuration, also deletes the topic's messages. Double-check the topic name before running it in production.
- The first time the module runs on a machine, it installs the `confluent-kafka` client library automatically if a compatible version is not already bundled; this requires internet access on that first run.

References:
- https://kafka.apache.org/documentation/
- https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html

---

## Como usar este modulo

Use este modulo para producir, consumir y administrar topics de Apache Kafka desde Rocketbot. Soporta autenticacion SASL/SSL para Confluent Cloud, Azure Event Hubs, AWS MSK y clusters self-hosted.

Uso basico:
1. Ejecute Conectar a Kafka con los bootstrap servers y, si corresponde, el protocolo de seguridad y las credenciales SASL. Opcionalmente defina un identificador de Sesion para mantener varias conexiones abiertas a la vez.
2. Ejecute Probar Conexion para confirmar que Rocketbot puede llegar al cluster antes de armar el resto del flujo.
3. Use los comandos de topics (Listar Topics, Crear Topic, Eliminar Topic, Describir Topic, Obtener Offsets) para administrarlos.
4. Use Producir Mensaje / Producir Batch para publicar, y Consumir Mensajes / Commit Offsets / Cerrar Consumidor para leer.

Ejemplos de conexion:
- Kafka local o self-hosted sin autenticacion: Bootstrap servers `localhost:9092` o `broker1:9092,broker2:9092`; Protocolo de seguridad `PLAINTEXT`; deje vacios Mecanismo SASL, Usuario y Contraseña.
- Kafka self-hosted con SASL: Bootstrap servers `broker1:9093,broker2:9093`; Protocolo de seguridad `SASL_SSL` o `SASL_PLAINTEXT` segun el cluster; Mecanismo SASL `PLAIN`, `SCRAM-SHA-256` o `SCRAM-SHA-512`; Usuario y Contraseña del usuario Kafka.
- Confluent Cloud: Bootstrap servers desde la configuracion del cluster, normalmente `pkc-xxxxx.region.provider.confluent.cloud:9092`; Protocolo de seguridad `SASL_SSL`; Mecanismo SASL `PLAIN`; Usuario = API key; Contraseña = API secret.
- Azure Event Hubs usando Kafka API: Bootstrap servers `<namespace>.servicebus.windows.net:9093`; Protocolo de seguridad `SASL_SSL`; Mecanismo SASL `PLAIN`; Usuario exactamente `$ConnectionString`; Contraseña = connection string completa de Event Hubs, por ejemplo `Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<policy>;SharedAccessKey=<key>`. Si la connection string incluye `EntityPath`, use ese mismo nombre de Event Hub como topic Kafka.
- AWS MSK: Use la lista de brokers y el modo de autenticacion provistos por el cluster. Para autenticacion IAM/OAUTHBEARER este modulo no alcanza tal como esta; use credenciales SCRAM o PLAIN expuestas por el cluster.

Sesiones:
- Todos los comandos reciben un identificador de Sesion. Dejelo vacio para usar la conexion por defecto, o defina uno para mantener varias conexiones/consumers independientes en el mismo flujo (por ejemplo, uno por cluster o por consumer group).
- Conectar a Kafka solo guarda la configuracion de la conexion; todavia no crea el consumer.

Publicar mensajes:
- Producir Mensaje envia un unico mensaje (clave opcional; el valor puede ser texto plano o JSON).
- Producir Batch envia una lista JSON de mensajes en una sola llamada, por ejemplo `[{"key": "1", "value": "hola"}, {"key": "2", "value": {"total": 100}}]`. Cada item puede ser un valor simple o un objeto `{key, value}`. Util cuando el robot ya tiene una lista de registros para publicar juntos.

Consumir mensajes (flujo de consumer group):
1. Ejecute Consumir Mensajes con Topicos (separados por coma), Group ID y Offset inicial. Estos valores, junto con Max poll interval, solo se aplican la primera vez que se crea el consumer para esa sesion; para cambiarlos despues, ejecute primero Cerrar Consumidor y vuelva a llamar Consumir Mensajes.
2. Procese los mensajes devueltos en el robot. Cada mensaje incluye su propio `topic`, `partition`, `offset`, `key` y `value`, asi puede distinguir de que topico vino cuando esta suscripto a mas de uno.
3. Ejecute Commit Offsets para indicarle a Kafka que los mensajes fueron procesados. Por defecto confirma todo el lote; si el robot solo proceso algunos, pase los exitosos en "Mensajes procesados" para confirmar solo esos (y los anteriores), dejando el resto para reintentar en la proxima lectura.
4. Ejecute Cerrar Consumidor al terminar el flujo, o antes de cambiar Topicos, Group ID, Offset inicial o Max poll interval en la misma sesion.

Notas importantes:
- Probar Conexion nunca corta el flujo: devuelve true o false para que el robot decida segun el resultado. Si falla, el motivo se imprime en el log de ejecucion, no en la variable de resultado.
- Commit Offsets devuelve false, sin lanzar error, cuando no habia nada nuevo que confirmar desde el ultimo commit -- esto es normal, no una falla. Si el commit en si falla (por ejemplo, el consumer fue expulsado del grupo), el comando lanza un error en vez de devolver false.
- Los valores de los mensajes se guardan y devuelven tal cual se enviaron, incluyendo espacios, saltos de linea o formato del texto original (Producir Mensaje/Batch no los recortan ni reformatean, y Consumir Mensajes los devuelve sin cambios).
- Group ID, Offset inicial y Max poll interval aplican por igual a todos los topicos de una misma llamada a Consumir Mensajes. Para usar configuracion distinta por topico, use una sesion separada para cada uno.
- Si Commit Offsets se ejecuta mas tarde que el Max poll interval configurado desde el ultimo Consumir Mensajes, Kafka puede haber expulsado al consumer del grupo; vuelva a ejecutar Consumir Mensajes o aumente el Max poll interval.
- Eliminar Topic es irreversible y, segun la configuracion del cluster, tambien borra los mensajes del topico. Verifique bien el nombre antes de correrlo en produccion.
- La primera vez que el modulo corre en una maquina, instala automaticamente la libreria `confluent-kafka` si no hay una version compatible ya incluida; esto requiere acceso a internet en esa primera ejecucion.

Referencias:
- https://kafka.apache.org/documentation/
- https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html

---

## Como usar este modulo

Use este modulo para produzir, consumir e administrar topics do Apache Kafka a partir do Rocketbot. Suporta autenticacao SASL/SSL para Confluent Cloud, Azure Event Hubs, AWS MSK e clusters self-hosted.

Uso basico:
1. Execute Conectar ao Kafka com os bootstrap servers e, se necessario, o protocolo de seguranca e as credenciais SASL. Opcionalmente defina um identificador de Sessao para manter varias conexoes abertas ao mesmo tempo.
2. Execute Testar Conexao para confirmar que o Rocketbot consegue alcancar o cluster antes de montar o resto do fluxo.
3. Use os comandos de topics (Listar Topics, Criar Topic, Excluir Topic, Descrever Topic, Obter Offsets) para administra-los.
4. Use Produzir Mensagem / Produzir Batch para publicar, e Consumir Mensagens / Commit Offsets / Fechar Consumidor para ler.

Exemplos de conexao:
- Kafka local ou self-hosted sem autenticacao: Bootstrap servers `localhost:9092` ou `broker1:9092,broker2:9092`; Protocolo de seguranca `PLAINTEXT`; deixe vazios Mecanismo SASL, Usuario e Senha.
- Kafka self-hosted com SASL: Bootstrap servers `broker1:9093,broker2:9093`; Protocolo de seguranca `SASL_SSL` ou `SASL_PLAINTEXT` conforme o cluster; Mecanismo SASL `PLAIN`, `SCRAM-SHA-256` ou `SCRAM-SHA-512`; Usuario e Senha do usuario Kafka.
- Confluent Cloud: Bootstrap servers da configuracao do cluster, geralmente `pkc-xxxxx.region.provider.confluent.cloud:9092`; Protocolo de seguranca `SASL_SSL`; Mecanismo SASL `PLAIN`; Usuario = API key; Senha = API secret.
- Azure Event Hubs usando Kafka API: Bootstrap servers `<namespace>.servicebus.windows.net:9093`; Protocolo de seguranca `SASL_SSL`; Mecanismo SASL `PLAIN`; Usuario exatamente `$ConnectionString`; Senha = connection string completa do Event Hubs, por exemplo `Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<policy>;SharedAccessKey=<key>`. Se a connection string incluir `EntityPath`, use o mesmo nome do Event Hub como topic Kafka.
- AWS MSK: Use a lista de brokers e o modo de autenticacao fornecidos pelo cluster. Para autenticacao IAM/OAUTHBEARER este modulo nao e suficiente como esta; use credenciais SCRAM ou PLAIN expostas pelo cluster.

Sessoes:
- Todos os comandos recebem um identificador de Sessao. Deixe vazio para usar a conexao padrao, ou defina um para manter varias conexoes/consumers independentes no mesmo fluxo (por exemplo, um por cluster ou por consumer group).
- Conectar ao Kafka apenas guarda a configuracao da conexao; ainda nao cria o consumer.

Publicar mensagens:
- Produzir Mensagem envia uma unica mensagem (chave opcional; o valor pode ser texto simples ou JSON).
- Produzir Batch envia uma lista JSON de mensagens em uma unica chamada, por exemplo `[{"key": "1", "value": "ola"}, {"key": "2", "value": {"total": 100}}]`. Cada item pode ser um valor simples ou um objeto `{key, value}`. Util quando o robo ja tem uma lista de registros para publicar juntos.

Consumir mensagens (fluxo de consumer group):
1. Execute Consumir Mensagens com Topicos (separados por virgula), Group ID e Offset inicial. Esses valores, junto com o Max poll interval, so se aplicam na primeira vez que o consumer e criado para essa sessao; para altera-los depois, execute primeiro Fechar Consumidor e chame Consumir Mensagens novamente.
2. Processe as mensagens retornadas no robo. Cada mensagem inclui seu proprio `topic`, `partition`, `offset`, `key` e `value`, assim voce identifica de qual topico ela veio ao assinar mais de um.
3. Execute Commit Offsets para informar ao Kafka que as mensagens foram processadas. Por padrao confirma todo o lote; se o robo processou apenas algumas, passe as bem-sucedidas em "Mensagens processadas" para confirmar somente ate essas (e as anteriores), deixando o restante para a proxima leitura tentar de novo.
4. Execute Fechar Consumidor ao terminar o fluxo, ou antes de alterar Topicos, Group ID, Offset inicial ou Max poll interval na mesma sessao.

Notas importantes:
- Testar Conexao nunca interrompe o fluxo: retorna true ou false para que o robo decida com base no resultado. Se falhar, o motivo e impresso no log de execucao, nao na variavel de resultado.
- Commit Offsets retorna false, sem lancar erro, quando nao havia nada novo para confirmar desde o ultimo commit -- isso e normal, nao uma falha. Se o commit em si falhar (por exemplo, o consumer foi removido do grupo), o comando lanca um erro em vez de retornar false.
- Os valores das mensagens sao armazenados e retornados exatamente como foram enviados, incluindo espacos, quebras de linha ou formatacao do texto original (Produzir Mensagem/Batch nao os recortam nem reformatam, e Consumir Mensagens os retorna sem alteracoes).
- Group ID, Offset inicial e Max poll interval se aplicam igualmente a todos os topicos de uma mesma chamada de Consumir Mensagens. Para usar configuracao diferente por topico, use uma sessao separada para cada um.
- Se Commit Offsets rodar mais tarde que o Max poll interval configurado desde o ultimo Consumir Mensagens, o Kafka pode ja ter removido o consumer do grupo; execute Consumir Mensagens novamente ou aumente o Max poll interval.
- Excluir Topic e irreversivel e, dependendo da configuracao do cluster, tambem apaga as mensagens do topico. Confira bem o nome antes de rodar isso em producao.
- Na primeira vez que o modulo roda em uma maquina, ele instala automaticamente a biblioteca `confluent-kafka` caso nao haja uma versao compativel ja incluida; isso requer acesso a internet nessa primeira execucao.

Referencias:
- https://kafka.apache.org/documentation/
- https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html

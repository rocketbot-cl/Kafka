



# Kafka

Módulo para produzir, consumir e administrar topics do Apache Kafka. Suporta autenticação SASL/SSL para Confluent Cloud, Azure Event Hubs, AWS MSK e clusters self-hosted.

*Read this in other languages: [English](README.md), [Português](README.pr.md), [Español](README.es.md)*

## Como instalar este módulo

Para instalar o módulo no Rocketbot Studio, pode ser feito de duas formas:
1. Manual: __Baixe__ o arquivo .zip e descompacte-o na pasta módulos. O nome da pasta deve ser o mesmo do módulo e dentro dela devem ter os seguintes arquivos e pastas: \__init__.py, package.json, docs, example e libs. Se você tiver o aplicativo aberto, atualize seu navegador para poder usar o novo módulo.
2. Automático: Ao entrar no Rocketbot Studio na margem direita você encontrará a seção **Addons**, selecione **Install Mods**, procure o módulo desejado e aperte instalar.


## Overview


1. Conectar ao Kafka
Configura a conexão com o cluster Kafka. Para Apache Kafka/self-hosted use broker:9092 com PLAINTEXT ou sua configuracao SSL/SASL. Para Confluent Cloud use SASL_SSL + PLAIN, usuario=API key e senha=API secret. Para Azure Event Hubs pelo endpoint Kafka use <namespace>.servicebus.windows.net:9093, SASL_SSL + PLAIN, usuario=$ConnectionString e senha=connection string completa do Event Hubs. Pode usar um identificador de sessao para alternar entre varias conexoes

2. Testar Conexão
Testa se o Rocketbot consegue se conectar ao Kafka com a configuração indicada, retornando true ou false; se falhar, o motivo é impresso no log de execução

3. Listar Topics
Lista os topics disponíveis no Kafka. Um topic é como um canal onde as mensagens são publicadas e lidas

4. Criar Topic
Cria um novo topic no Kafka, indicando nome, quantidade de partições e fator de replicação se aplicável

5. Excluir Topic
IRREVERSÍVEL: exclui um topic do Kafka. Dependendo da configuração do cluster, também apaga as mensagens associadas. Confira bem o nome do tópico antes de rodar isso em produção

6. Descrever Topic
Mostra informações detalhadas de um topic: partições, líder, réplicas, ISR, estado de erro e configuração. Serve para diagnóstico

7. Obter Offsets
Consulta o offset mais antigo e o mais novo disponível de cada partição de um tópico. Serve para saber quantas mensagens estão disponíveis ou a partir de que ponto um consumer pode ler

8. Produzir Mensagem
Envia uma mensagem individual a um topic. Por exemplo, publicar um JSON com dados de uma fatura, pedido ou evento

9. Produzir Lote de Mensagens
Envia várias mensagens juntas a um topic. Útil quando o bot tem uma lista de registros e quer publicá-los todos no Kafka

10. Consumir Mensagens
Lê mensagens de um ou vários topics, cada uma com seu próprio campo 'topic'. Tem limite de quantidade e de tempo. O consumer (topics, group ID, offset inicial, max poll interval) é criado na primeira chamada da sessão e reutilizado depois; execute 'Fechar Consumidor' primeiro para alterar esses valores

11. Commit Offsets
Confirma que as mensagens consumidas foram processadas e indica ao Kafka de onde continuar a próxima leitura. Por padrão confirma todo o lote de 'Consumir Mensagens'; use 'Mensagens processadas' para confirmar somente até a última bem-sucedida

12. Fechar Consumidor
Fecha a sessão do consumidor e libera seus recursos. É necessário antes de alterar a configuração do consumer (Group ID, Offset inicial, Max poll interval) na próxima chamada de 'Consumir Mensagens'




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
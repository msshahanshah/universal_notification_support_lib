const { LRUCache } = require("lru-cache");
const RabbitMQClient = require("./RabbitMQClient");

class RabbitMQManager {
  #logger = console;
  #clientConfigs = [];

  constructor(fetchConfigsCallback, logger = console) {
    this.fetchConfigsCallback = fetchConfigsCallback;
    this.#logger = logger;

    this.cache = new LRUCache({
      max: 50,
      ttl: 1000 * 60 * 60,
      dispose: async (client, key) => {
        this.#logger.info(`Evicting RabbitMQ client for ${key}`);
        try {
          await client.close();
        } catch (e) {
          this.#logger.error("Error closing evicted client", e);
        }
      },
    });
  }

  async getClient(clientId) {
    if (this.cache.has(clientId)) {
      return this.cache.get(clientId);
    }

    const clientConfig = await this.#loadClientConfig(clientId);
    const url = this.#buildUrl(clientConfig.RABBITMQ);

    const client = new RabbitMQClient({
      url,
      config: clientConfig,
      logger: this.#logger,
    });

    await client.connect();
    this.cache.set(clientId, client);
    return client;
  }

  async #loadClientConfig(clientId) {
    let client = this.#clientConfigs.find((c) => c.ID === clientId);

    if (!client) {
      this.#clientConfigs = await this.fetchConfigsCallback();
      client = this.#clientConfigs.find((c) => c.ID === clientId);
    }

    if (!client) {
      throw new Error(`No config found for client ${clientId}`);
    }

    return client;
  }

  #buildUrl(rabbitConfig) {
    const { HOST, PORT, USER, PASSWORD } = rabbitConfig;
    return `amqp://${USER}:${PASSWORD}@${HOST}:${PORT}`;
  }

  async close(clientId) {
    const client = this.cache.get(clientId);
    if (client) {
      await client.close();
      this.cache.delete(clientId);
    }
  }

  async closeAll() {
    for (const [, client] of this.cache) {
      await client.close();
    }
    this.cache.clear();
  }
}

module.exports = RabbitMQManager;

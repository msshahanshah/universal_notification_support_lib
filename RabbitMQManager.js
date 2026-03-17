const { LRUCache } = require("lru-cache");
const RabbitMQClient = require("./RabbitMQClient");

class RabbitMQManager {
  #logger = console;
  #clientConfigs = [];

  constructor(fetchConfigsCallback, logger = console) {
    this.fetchConfigsCallback = fetchConfigsCallback;
    this.#logger = logger;

    this.#logger.info(
      "[RabbitMQManager] Initializing with LRU cache (max=50, ttl=1h)",
    );

    this.cache = new LRUCache({
      max: 50,
      ttl: 1000 * 60 * 60,
      dispose: async (client, key) => {
        this.#logger.info(
          `[RabbitMQManager] Evicting client from cache for clientId=${key}`,
        );
        try {
          await client.close();
          this.#logger.debug(
            `[RabbitMQManager] Evicted client closed for clientId=${key}`,
          );
        } catch (error) {
          this.#logger.error(
            `[RabbitMQManager] Error closing evicted client for clientId=${key}, ${JSON.stringify(error)}`,
          );
        }
      },
    });

    this.#logger.info("[RabbitMQManager] Initialized successfully");
  }

  async getClient(clientId) {
    this.#logger.debug(
      `[RabbitMQManager] getClient called for clientId=${clientId}`,
    );

    if (this.cache.has(clientId)) {
      this.#logger.info(`[RabbitMQManager] Cache hit for clientId=${clientId}`);
      return this.cache.get(clientId);
    }

    this.#logger.info(
      `[RabbitMQManager] Cache miss for clientId=${clientId}, loading config`,
    );
    const clientConfig = await this.#loadClientConfig(clientId);
    const url = this.#buildUrl(clientConfig.RABBITMQ);

    this.#logger.debug(
      `[RabbitMQManager] Creating new RabbitMQClient for clientId=${clientId}`,
    );
    const client = new RabbitMQClient({
      url,
      config: clientConfig,
      logger: this.#logger,
    });

    this.#logger.info(
      `[RabbitMQManager] Connecting client for clientId=${clientId}`,
    );
    await client.connect();
    this.cache.set(clientId, client);
    this.#logger.info(
      `[RabbitMQManager] Client connected and cached for clientId=${clientId}`,
    );

    return client;
  }

  async #loadClientConfig(clientId) {
    this.#logger.debug(
      `[RabbitMQManager] Looking up config for clientId=${clientId} in local configs`,
    );
    let client = this.#clientConfigs.find((c) => c.ID === clientId);

    if (!client) {
      this.#logger.info(
        `[RabbitMQManager] Config not found locally for clientId=${clientId}, fetching via callback`,
      );
      this.#clientConfigs = await this.fetchConfigsCallback();
      this.#logger.debug(
        `[RabbitMQManager] Fetched ${this.#clientConfigs.length} config(s)`,
      );
      client = this.#clientConfigs.find((c) => c.ID === clientId);
    }

    if (!client) {
      this.#logger.error(
        `[RabbitMQManager] No config found for clientId=${clientId}`,
      );
      throw new Error(`No config found for client ${clientId}`);
    }

    this.#logger.debug(
      `[RabbitMQManager] Config resolved for clientId=${clientId}`,
    );
    return client;
  }

  #buildUrl(rabbitConfig) {
    const { HOST, PORT, USER, PASSWORD } = rabbitConfig;
    const url = `amqp://${USER}:${PASSWORD}@${HOST}:${PORT}`;
    this.#logger.debug(
      `[RabbitMQManager] Built AMQP URL → host=${HOST}, port=${PORT}`,
    );
    return url;
  }

  async close(clientId) {
    this.#logger.info(
      `[RabbitMQManager] Closing client for clientId=${clientId}`,
    );
    const client = this.cache.get(clientId);
    if (client) {
      await client.close();
      this.cache.delete(clientId);
      this.#logger.info(
        `[RabbitMQManager] Client closed and removed from cache for clientId=${clientId}`,
      );
    } else {
      this.#logger.warn(
        `[RabbitMQManager] No cached client found for clientId=${clientId}, nothing to close`,
      );
    }
  }

  async closeAll() {
    const count = this.cache.size;
    this.#logger.info(
      `[RabbitMQManager] Closing all ${count} cached client(s)`,
    );
    for (const [key, client] of this.cache) {
      this.#logger.debug(
        `[RabbitMQManager] Closing client for clientId=${key}`,
      );
      await client.close();
    }
    this.cache.clear();
    this.#logger.info("[RabbitMQManager] All clients closed and cache cleared");
  }
}

module.exports = RabbitMQManager;

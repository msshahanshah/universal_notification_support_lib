const amqp = require("amqplib");

class RabbitMQClient {
  constructor({ url, config, logger = console }) {
    this.url = url;
    this.clientConfig = config;
    this.logger = logger;

    this.connection = null;
    this.channel = null;
    this.consumerTag = null;
    this.isShuttingDown = false;
  }

  async connect() {
    if (this.connection) {
      this.logger.debug("[RabbitMQClient] Already connected, skipping connect");
      return;
    }

    this.logger.info("[RabbitMQClient] Connecting to RabbitMQ...");
    this.connection = await amqp.connect(this.url);
    this.channel = await this.connection.createConfirmChannel();
    this.logger.info(
      "[RabbitMQClient] Connection and confirm channel established",
    );

    this.connection.on("error", (err) => {
      this.logger.error("[RabbitMQClient] Connection error", err);
    });

    this.connection.on("close", () => {
      if (!this.isShuttingDown) {
        this.logger.warn("[RabbitMQClient] Connection closed unexpectedly");
      }
    });
  }

  async publishMessage(serviceType, message) {
    this.logger.info(
      `[RabbitMQClient] publishMessage → serviceType=${serviceType}`,
    );

    if (!this.channel) {
      this.logger.debug("[RabbitMQClient] No channel, connecting first");
      await this.connect();
    }

    const serviceKey = serviceType.toUpperCase();
    const rabbitConfig = this.clientConfig[serviceKey]?.RABBITMQ;

    if (!rabbitConfig) {
      this.logger.error(
        `[RabbitMQClient] No RabbitMQ config found for service: ${serviceType}`,
      );
      throw new Error(`No RabbitMQ config for service ${serviceType}`);
    }

    const { EXCHANGE_NAME, EXCHANGE_TYPE, ROUTING_KEY, QUEUE_NAME } =
      rabbitConfig;

    this.logger.debug(
      `[RabbitMQClient] Asserting exchange: ${EXCHANGE_NAME} (${EXCHANGE_TYPE})`,
    );
    await this.channel.assertExchange(EXCHANGE_NAME, EXCHANGE_TYPE, {
      durable: true,
    });

    this.logger.debug(`[RabbitMQClient] Asserting queue: ${QUEUE_NAME}`);
    await this.channel.assertQueue(QUEUE_NAME, { durable: true });

    this.logger.info(
      `[RabbitMQClient] Publishing → exchange=${EXCHANGE_NAME}, routingKey=${ROUTING_KEY}`,
    );

    this.channel.publish(
      EXCHANGE_NAME,
      ROUTING_KEY,
      Buffer.from(JSON.stringify(message)),
      { persistent: true },
    );

    this.logger.debug("[RabbitMQClient] Waiting for publish confirms...");
    await this.channel.waitForConfirms();
    this.logger.info(
      `[RabbitMQClient] Message published and confirmed → exchange=${EXCHANGE_NAME}, routingKey=${ROUTING_KEY}`,
    );
  }

  async consume({ service, sender, db, maxProcessAttemptCount = 3 }) {
    this.logger.info(`[RabbitMQClient] consume → service=${service}`);

    if (!this.channel) {
      this.logger.debug("[RabbitMQClient] No channel, connecting first");
      await this.connect();
    }

    if (this.consumerTag) {
      this.logger.warn(
        `[RabbitMQClient] Consumer already running (tag=${this.consumerTag}), skipping`,
      );
      return;
    }

    const serviceKey = service.toUpperCase();
    const rabbitConfig = this.clientConfig[serviceKey]?.RABBITMQ;

    if (!rabbitConfig) {
      this.logger.error(
        `[RabbitMQClient] No RabbitMQ config found for service: ${service}`,
      );
      throw new Error(`No RabbitMQ config for service ${service}`);
    }

    const { EXCHANGE_NAME, EXCHANGE_TYPE, QUEUE_NAME, ROUTING_KEY } =
      rabbitConfig;

    this.logger.debug(
      `[RabbitMQClient] Asserting exchange: ${EXCHANGE_NAME} (${EXCHANGE_TYPE})`,
    );
    await this.channel.assertExchange(EXCHANGE_NAME, EXCHANGE_TYPE, {
      durable: true,
    });

    this.logger.debug(`[RabbitMQClient] Asserting queue: ${QUEUE_NAME}`);
    const { queue } = await this.channel.assertQueue(QUEUE_NAME, {
      durable: true,
    });

    this.logger.debug(
      `[RabbitMQClient] Binding queue "${queue}" to exchange "${EXCHANGE_NAME}" with routingKey "${ROUTING_KEY}"`,
    );
    await this.channel.bindQueue(queue, EXCHANGE_NAME, ROUTING_KEY);
    await this.channel.prefetch(1);

    this.logger.info(
      `[RabbitMQClient] Start consuming → service=${service}, queue=${QUEUE_NAME}, routingKey=${ROUTING_KEY}`,
    );

    const { consumerTag } = await this.channel.consume(
      queue,
      (msg) =>
        this.processMessage(
          { service, msg, sender },
          db,
          maxProcessAttemptCount,
        ),
      { noAck: false },
    );

    this.consumerTag = consumerTag;
    this.logger.info(
      `[RabbitMQClient] Consumer registered with tag: ${consumerTag}`,
    );
  }

  async processMessage({ service, msg, sender }, db, maxProcessAttemptCount) {
    if (!msg) {
      this.logger.debug(
        "[RabbitMQClient] processMessage received null msg, skipping",
      );
      return;
    }

    this.logger.debug(
      `[RabbitMQClient] Processing message for service=${service}`,
    );

    // Parsing payload
    let payload;
    try {
      payload = JSON.parse(msg.content.toString());
      this.logger.debug(
        `[RabbitMQClient] Parsed payload for messageId=${payload?.messageId}`,
      );
    } catch (err) {
      this.logger.error(
        "[RabbitMQClient] Invalid JSON payload, dropping message",
        err,
      );
      return this.channel.nack(msg, false, false);
    }

    const { messageId, content, destination, provider } = payload;

    // Database processing for notification messages
    let record;
    if (db) {
      this.logger.debug(
        `[RabbitMQClient] Starting DB transaction for messageId=${messageId}`,
      );
      const transaction = await db.sequelize.transaction();
      try {
        record = await db.Notification.findOne({
          where: { messageId },
          lock: transaction.LOCK.UPDATE,
          transaction,
        });

        if (!record) {
          this.logger.warn(
            `[RabbitMQClient] No DB record found for messageId=${messageId}, nacking`,
          );
          await transaction.commit();
          return this.channel.nack(msg, false, false);
        }

        if (record.status === "sent") {
          this.logger.info(
            `[RabbitMQClient] Message already sent for messageId=${messageId}, acking`,
          );
          await transaction.commit();
          return this.channel.ack(msg);
        }

        if (
          record.status === "failed" &&
          record.attempts >= maxProcessAttemptCount
        ) {
          this.logger.warn(
            `[RabbitMQClient] Max attempts (${maxProcessAttemptCount}) reached for messageId=${messageId}, acking and discarding`,
          );
          await transaction.commit();
          return this.channel.ack(msg);
        }

        this.logger.debug(
          `[RabbitMQClient] Updating status to "processing" for messageId=${messageId} (attempt ${record.attempts + 1})`,
        );
        record.status = "processing";
        record.attempts += 1;
        await record.save({ transaction });

        await transaction.commit();
        this.logger.debug(
          `[RabbitMQClient] DB transaction committed for messageId=${messageId}`,
        );
      } catch (err) {
        await transaction.rollback();
        this.logger.error(
          `[RabbitMQClient] DB transaction failed for messageId=${messageId}, rolling back`,
          err,
        );
        return this.channel.nack(msg, false, true);
      }
    }

    // Message processing
    try {
      this.logger.info(
        `[RabbitMQClient] Sending message via sender for messageId=${messageId}`,
      );
      const result = await sender(payload, messageId);
      this.logger.info(
        `[RabbitMQClient] Message sent successfully for messageId=${messageId}`,
      );

      if (db) {
        this.logger.debug(
          `[RabbitMQClient] Updating DB status to "sent" for messageId=${messageId}`,
        );
        await db.Notification.update(
          {
            status: result.status ? `${provider}:${result.status}` : "sent",
            connectorResponse: JSON.stringify(result),
            referenceId: result?.referenceId,
          },
          { where: { messageId } },
        );
      }

      // Push message to webhook queue if allowed
      if (service?.toUpperCase() !== "WEBHOOK" && content.isWebhookEnabled) {
        const clientId = content?.clientId;

        if (!clientId) {
          this.logger.warn(
            `Skipping webhook publish: missing clientId for messageId=${messageId}`,
          );
        } else {
          this.publishMessage("webhook", {
            clientId,
            service,
            status: "failed",
            details: {
              messageId,
              connectorResponse: JSON.stringify(result),
            },
          })
            .then(() => {
              this.logger.info(`message published to webhook queue`);
            })
            .catch((error) => {
              this.logger.error(
                `failed to publish message to webhook queue. Error: ${JSON.stringify(error)}`,
              );
            });
        }
      }

      return this.channel.ack(msg);
    } catch (err) {
      const errorMessage =
        err?.message ||
        err?.errorMessage ||
        err?.details ||
        err?.description ||
        err?.reason ||
        err?.error ||
        JSON.stringify(err);

      this.logger.error(
        `[RabbitMQClient] Send failed for messageId=${messageId}`,
        err,
      );

      if (db) {
        this.logger.debug(
          `[RabbitMQClient] Updating DB status to "failed" for messageId=${messageId}`,
        );
        await db.Notification.update(
          {
            status: "failed",
            connectorResponse: errorMessage,
          },
          { where: { messageId } },
        );

        if (record.attempts >= maxProcessAttemptCount) {
          this.logger.warn(
            `[RabbitMQClient] Max attempts reached after failure for messageId=${messageId}, acking`,
          );
          return this.channel.ack(msg);
        }
      }

      this.logger.debug(
        `[RabbitMQClient] Nacking message for retry, messageId=${messageId}`,
      );

      // Push message to webhook queue if allowed
      if (service?.toUpperCase() !== "WEBHOOK" && content.isWebhookEnabled) {
        const clientId = content?.clientId;

        if (!clientId) {
          this.logger.warn(
            `Skipping webhook publish: missing clientId for messageId=${messageId}`,
          );
        } else {
          this.publishMessage("webhook", {
            clientId,
            service,
            status: "failed",
            details: {
              messageId,
              connectorResponse: JSON.stringify(err),
            },
          })
            .then(() => {
              this.logger.info(`message published to webhook queue`);
            })
            .catch((err) => {
              this.logger.error(
                `failed to publish message to webhook queue. Error: ${JSON.stringify(error)}`,
              );
            });
        }
      }

      return this.channel.nack(msg, false, true);
    }
  }

  async close() {
    this.logger.info("[RabbitMQClient] Closing connection...");
    this.isShuttingDown = true;
    try {
      if (this.consumerTag) {
        this.logger.debug(
          `[RabbitMQClient] Cancelling consumer tag: ${this.consumerTag}`,
        );
        await this.channel.cancel(this.consumerTag);
      }
      if (this.channel) {
        this.logger.debug("[RabbitMQClient] Closing channel");
        await this.channel.close();
      }
      if (this.connection) {
        this.logger.debug("[RabbitMQClient] Closing connection");
        await this.connection.close();
      }
    } finally {
      this.channel = null;
      this.connection = null;
      this.consumerTag = null;
    }
    this.logger.info("[RabbitMQClient] Closed cleanly");
  }
}

module.exports = RabbitMQClient;

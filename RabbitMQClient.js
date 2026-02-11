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
    if (this.connection) return;

    this.logger.info("Connecting to RabbitMQ...");
    this.connection = await amqp.connect(this.url);
    this.channel = await this.connection.createConfirmChannel();

    this.connection.on("error", (err) =>
      this.logger.error("RabbitMQ connection error", err),
    );

    this.connection.on("close", () => {
      if (!this.isShuttingDown) {
        this.logger.warn("RabbitMQ connection closed unexpectedly");
      }
    });
  }

  async publishMessage(serviceType, message) {
    if (!this.channel) await this.connect();

    const serviceKey = serviceType.toUpperCase();
    const rabbitConfig = this.clientConfig[serviceKey]?.RABBITMQ;

    if (!rabbitConfig) {
      throw new Error(`No RabbitMQ config for service ${serviceType}`);
    }

    const { EXCHANGE_NAME, EXCHANGE_TYPE, ROUTING_KEY, QUEUE_NAME } =
      rabbitConfig;

    await this.channel.assertExchange(EXCHANGE_NAME, EXCHANGE_TYPE, {
      durable: true,
    });

    await this.channel.assertQueue(QUEUE_NAME, { durable: true });

    this.logger.info(
      `Publishing → exchange=${EXCHANGE_NAME}, routingKey=${ROUTING_KEY}`,
    );

    this.channel.publish(
      EXCHANGE_NAME,
      ROUTING_KEY,
      Buffer.from(JSON.stringify(message)),
      { persistent: true },
    );

    await this.channel.waitForConfirms();
  }

  async consume({ service, sender, db, maxProcessAttemptCount = 3 }) {
    if (!this.channel) await this.connect();
    if (this.consumerTag) {
      this.logger.warn("Consumer already running");
      return;
    }

    const serviceKey = service.toUpperCase();
    const rabbitConfig = this.clientConfig[serviceKey]?.RABBITMQ;

    if (!rabbitConfig) {
      throw new Error(`No RabbitMQ config for service ${service}`);
    }

    const { EXCHANGE_NAME, EXCHANGE_TYPE, QUEUE_NAME, ROUTING_KEY } =
      rabbitConfig;

    await this.channel.assertExchange(EXCHANGE_NAME, EXCHANGE_TYPE, {
      durable: true,
    });

    const { queue } = await this.channel.assertQueue(QUEUE_NAME, {
      durable: true,
    });

    await this.channel.bindQueue(queue, EXCHANGE_NAME, ROUTING_KEY);
    await this.channel.prefetch(1);

    this.logger.info(
      `Consuming → service=${service}, queue=${QUEUE_NAME}, routingKey=${ROUTING_KEY}`,
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
  }

  async processMessage({ service, msg, sender }, db, maxProcessAttemptCount) {
    if (!msg) return;

    let payload;
    try {
      payload = JSON.parse(msg.content.toString());
    } catch (err) {
      this.logger.error("Invalid JSON payload", err);
      return this.channel.nack(msg, false, false); // drop bad message
    }

    const { messageId, content, destination, provider } = payload;

    let record;
    const transaction = await db.sequelize.transaction();

    try {
      record = await db.Notification.findOne({
        where: { messageId },
        lock: transaction.LOCK.UPDATE,
        transaction,
      });

      if (!record) {
        await transaction.commit();
        return this.channel.nack(msg, false, false);
      }

      if (record.status === "sent") {
        await transaction.commit();
        return this.channel.ack(msg);
      }

      if (
        record.status === "failed" &&
        record.attempts >= maxProcessAttemptCount
      ) {
        await transaction.commit();
        return this.channel.ack(msg);
      }

      record.status = "processing";
      record.attempts += 1;
      await record.save({ transaction });

      await transaction.commit();
    } catch (err) {
      await transaction.rollback();
      this.logger.error("DB transaction failed", err);
      return this.channel.nack(msg, false, true);
    }

    try {
      let msgData;

      if (service === "sms") {
        msgData = {
          to: destination,
          message: content.message,
          provider: provider,
        };
      } else if (service === "email") {
        msgData = {
          to: destination,
          subject: content.subject,
          html: content.body,
          from: content.fromEmail,
          cc: content.cc,
          bcc: content.bcc,
          attachments: content.attachments,
          fileId: content.fileId,
          extension: content.extension,
          provider: provider,
        };
      } else if (service === "slack" || service === "slackbot") {
        msgData = { to: destination, message: content.message };
      }

      if (!msgData) {
        throw new Error(`Unsupported service: ${service}`);
      }

      const result = await sender(msgData, messageId);

      await db.Notification.update(
        { status: "sent", connectorResponse: JSON.stringify(result) },
        { where: { messageId } },
      );

      return this.channel.ack(msg);
    } catch (err) {
      this.logger.error("Message send failed", err);

      await db.Notification.update(
        {
          status: "failed",
          connectorResponse: err.message,
        },
        { where: { messageId } },
      );

      if (record.attempts >= maxProcessAttemptCount) {
        return this.channel.ack(msg);
      }

      return this.channel.nack(msg, false, true);
    }
  }

  async close() {
    this.isShuttingDown = true;
    try {
      if (this.consumerTag) await this.channel.cancel(this.consumerTag);
      if (this.channel) await this.channel.close();
      if (this.connection) await this.connection.close();
    } finally {
      this.channel = null;
      this.connection = null;
      this.consumerTag = null;
    }
    this.logger.info("RabbitMQ closed cleanly");
  }
}

module.exports = RabbitMQClient;

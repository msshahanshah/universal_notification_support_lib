const {
  SecretsManagerClient,
  GetSecretValueCommand,
  ListSecretsCommand,
} = require("@aws-sdk/client-secrets-manager");
const RabbitMQClient = require("./RabbitMQClient");
const RabbitMQManager = require("./RabbitMQManager");
require("dotenv").config();

class SecretManager {
  constructor(logger = console) {
    this.logger = logger;

    this.NODE_ENV = process.env.NODE_ENV || "development";
    this.isProduction =
      process.env.NODE_ENV === "production" ||
      process.env.NODE_ENV === "staging";
    this.REGION = process.env.AWS_SECRET_REGION;
    this.ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID;
    this.SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY;
    this.SECRET_NAME = process.env.AWS_SECRET_NAME;

    this.logger.info(
      `[SecretManager] Initializing in environment: ${this.NODE_ENV}`,
    );

    if (
      !this.isProduction &&
      (!this.ACCESS_KEY_ID || !this.SECRET_ACCESS_KEY)
    ) {
      this.logger.error(
        "[SecretManager] Missing AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY",
      );
      throw new Error(
        "AWS SECRET ERROR: secret access key or access key id is missing!",
      );
    }

    if (!this.REGION) {
      this.logger.error("[SecretManager] Missing AWS_SECRET_REGION");
      throw new Error("AWS SECRET ERROR: region is missing!");
    }

    this.logger.debug(
      `[SecretManager] Connecting to AWS Secrets Manager in region: ${this.REGION}`,
    );

    this.CLIENT = new SecretsManagerClient({
      region: this.REGION,
      ...(!this.isProduction && {
        credentials: {
          accessKeyId: this.ACCESS_KEY_ID,
          secretAccessKey: this.SECRET_ACCESS_KEY,
        },
      }),
    });

    this.logger.info("[SecretManager] Initialized successfully");
  }

  async getSecrets(environment = this.NODE_ENV) {
    this.logger.info(
      `[SecretManager] Fetching all secrets with prefix "${this.SECRET_NAME}" for environment: ${environment}`,
    );
    try {
      this.logger.debug("[SecretManager] Sending ListSecretsCommand");
      const command = new ListSecretsCommand({});
      const { SecretList } = await this.CLIENT.send(command);

      this.logger.debug(
        `[SecretManager] Total secrets listed: ${SecretList.length}`,
      );

      let secrets = [];

      const filtered = SecretList.filter((secret) =>
        secret.Name?.startsWith(this.SECRET_NAME),
      );

      this.logger.debug(
        `[SecretManager] Secrets matching prefix "${this.SECRET_NAME}": ${filtered.length}`,
      );

      if (filtered.length === 0) {
        this.logger.warn(
          `[SecretManager] No secrets found with prefix: ${this.SECRET_NAME}`,
        );
        throw new Error(`No secrets found with prefix: ${this.SECRET_NAME}`);
      }

      secrets = await Promise.all(
        filtered.map(async (secret) => {
          this.logger.debug(
            `[SecretManager] Fetching secret value for: ${secret.Name}`,
          );
          const { SecretString } = await this.CLIENT.send(
            new GetSecretValueCommand({ SecretId: secret.Name }),
          );

          if (!SecretString) {
            this.logger.warn(
              `[SecretManager] SecretString is empty for: ${secret.Name}`,
            );
            return null;
          }

          const env = environment.trim();
          this.logger.info(
            `[SecretManager] env ${environment} length before trim: ${environment.length} after ${env.length}`,
          );

          const parsed = JSON.parse(SecretString);
          this.logger.info(
            `[SecretManager] parsing secret ${JSON.stringify(parsed)}`,
          );

          const result = JSON.parse(parsed[env]);
          this.logger.info(
            `[SecretManager] SecretString is: ${JSON.stringify(result)}`,
          );

          return result;
        }),
      );

      const validSecrets = secrets.filter(Boolean);

      if (validSecrets.length === 0) {
        this.logger.warn(
          "[SecretManager] All fetched secrets resolved to null",
        );
        throw new Error("AWS SECRET ERROR: no secrets found!");
      }

      this.logger.info(
        `[SecretManager] Successfully fetched ${validSecrets.length} secret(s)`,
      );
      return validSecrets;
    } catch (error) {
      this.logger.error(`[SecretManager] Error in getSecrets:, ${error}`);
      throw error;
    }
  }

  async getSecret(name, environment = this.NODE_ENV) {
    this.logger.info(
      `[SecretManager] Fetching secret "${name}" for environment: ${environment}`,
    );
    try {
      if (!name) {
        this.logger.error("[SecretManager] getSecret called without a name");
        throw new Error("AWS SECRET ERROR: No name provided!");
      }

      this.logger.debug(
        `[SecretManager] Sending GetSecretValueCommand for: ${name}`,
      );
      const command = new GetSecretValueCommand({
        SecretId: name,
      });
      const secret = await this.CLIENT.send(command);

      if (!secret) {
        this.logger.warn(`[SecretManager] No secret returned for: ${name}`);
        throw new Error(`AWS SECRET ERROR: No Secret Found for ${name}`);
      }

      const parseSecret = JSON.parse(secret.SecretString)[environment];

      if (!parseSecret) {
        this.logger.warn(
          `[SecretManager] Secret exists but missing key for environment "${environment}": ${name}`,
        );
        throw new Error(
          `AWS SECRET ERROR: No Secret Found for ${name}['${environment}']`,
        );
      }

      this.logger.info(`[SecretManager] Successfully fetched secret: ${name}`);
      return parseSecret;
    } catch (error) {
      this.logger.error(`[SecretManager] Error in getSecret: ${error}`);
      throw error;
    }
  }
}

module.exports = {
  SecretManager: new SecretManager(),
  RabbitMQClient: RabbitMQClient,
  RabbitMQManager: RabbitMQManager,
};

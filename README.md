# universal_notification_support_lib

A small CommonJS utility that exposes helpers to read AWS Secrets Manager secrets and lightweight RabbitMQ helpers used by the universal-notifier ecosystem.

This package exports a ready-made `SecretManager` instance and the `RabbitMQClient` and `RabbitMQManager` classes so you can integrate secrets + RabbitMQ clients consistently.

## Features

- Retrieve and parse secrets from AWS Secrets Manager (environment-scoped)
- Lightweight RabbitMQ client (publish/consume) and a caching manager for reusing clients
- CommonJS-friendly (exports via `module.exports`)

## Installation

```bash
npm install universal_notification_support_lib
```

You can also install directly from GitHub:

```bash

# Or install via the git url
npm install git+https://github.com/msshahanshah/universal_notification_support_lib.git

# Install a specific branch or tag
npm install git+https://github.com/msshahanshah/universal_notification_support_lib.git#main
```

## What this package exports

- `SecretManager` — an instantiated helper (require and call methods directly)
- `RabbitMQClient` — class, small AMQP client with `publishMessage`, `consume`, and `close`
- `RabbitMQManager` — class that caches `RabbitMQClient` instances and provides `getClient`, `close`, and `closeAll`

Require the package like:

```javascript
const {
  SecretManager,
  RabbitMQClient,
  RabbitMQManager,
} = require("universal_notification_support_lib");
```

## Environment variables

- `AWS_SECRET_NAME` — prefix used when listing secrets (the code filters secret names starting with this value)
- `AWS_SECRET_REGION` — AWS region for Secrets Manager (required)
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` — required in non-production environments (the package will throw if missing in development)
- `NODE_ENV` — defaults to `development`; `production` or `staging` toggles credential usage

Create a `.env` (for local/dev) and load it before requiring the package (this package calls `dotenv` internally, but ensure your env is set correctly):

```env
AWS_SECRET_NAME=your-secret-prefix
AWS_SECRET_REGION=us-east-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
NODE_ENV=development
```

Note: secret retrieval is environment-scoped — `SecretManager.getSecrets()` defaults to `process.env.NODE_ENV` and will return the parsed value stored under that environment key in each secret. For example, if `NODE_ENV=development` the package will parse and return the `development` object from each secret's JSON payload.

## Secret format

This package expects each secret's `SecretString` to be a JSON object whose top-level keys are environment names (for example `development`, `production`, `staging`). Each environment key's value should be a JSON string containing your service configuration. At runtime the package will:

1. list secrets and filter names that start with `AWS_SECRET_NAME`
2. fetch each secret's `SecretString`
3. parse the outer JSON, then parse the string stored for the requested environment (based on `NODE_ENV`)

Example of a single secret's `SecretString` stored in AWS Secrets Manager:

```json
{
  "ID": "CLIENT",
  "SERVER_PORT": 3001,
  "ENABLED_SERVERICES": ["slack", "email", "sms"],
  "SLACKBOT": {
    "TOKEN": "xoxb-",
    "RABBITMQ": {
      "EXCHANGE_NAME": "notifications_exchange",
      "EXCHANGE_TYPE": "direct",
      "QUEUE_NAME": "CLIENT.slackbot_queue",
      "ROUTING_KEY": "slack"
    }
  },
  "EMAIL": {
    "AWS": {
      "default": true,
      "USER_NAME": "AKXXXX",
      "PASSWORD": "BGFkXXXX",
      "SENDER_EMAIL": "tes@gmail.com",
      "REGION": "us-east-1"
    },
    "MAILGUN": {
      "API_KEY": "xxxx",
      "DOMAIN": "xxxx"
    },
    "RABBITMQ": {
      "EXCHANGE_NAME": "notifications_exchange",
      "EXCHANGE_TYPE": "direct",
      "QUEUE_NAME": "CLIENT.email_queue",
      "ROUTING_KEY": "email"
    }
  },
  "SMS": {
    "TWILIO": {
      "default": true,
      "ACCOUNT_SID": "ACXXXX",
      "AUTH_TOKEN": "XXXX",
      "FROM_NUMBER": "+111111111"
    },
    "RABBITMQ": {
      "EXCHANGE_NAME": "notifications_exchange",
      "EXCHANGE_TYPE": "direct",
      "QUEUE_NAME": "CLIENT.sms_queue",
      "ROUTING_KEY": "sms"
    }
  },
  "DBCONFIG": {
    "HOST": "localhost",
    "PORT": 5432,
    "NAME": "notifications_db",
    "USER": "dikshant",
    "PASSWORD": "postgres"
  },
  "RABBITMQ": {
    "HOST": "localhost",
    "PORT": 5672,
    "USER": "guest",
    "PASSWORD": "guest",
    "EXCHANGE_NAME": "notifications_exchange",
    "EXCHANGE_TYPE": "direct",
    "QUEUE_NAME": "notifications_queue",
    "ROUTING_KEY": "notifications"
  }
}
```

When `SecretManager.getSecrets()` runs with `NODE_ENV=development` it will return the parsed object for the `development` key. For example (after parsing) the returned object for the `development` environment would look like:

```json
{
  "ID": "GKMIT",
  "SERVER_PORT": 3001,
  "ENABLED_SERVERICES": ["slack", "email", "sms"],
  "SLACKBOT": {
    "TOKEN": "xoxb-",
    "RABBITMQ": {
      /* ... */
    }
  },
  "EMAIL": {
    /* ... */
  },
  "SMS": {
    /* ... */
  },
  "DBCONFIG": {
    /* ... */
  },
  "RABBITMQ": {
    /* ... */
  }
}
```

Note: the example preserves the key `ENABLED_SERVERICES` as used in the configuration shown above. If your project uses a different key name (for example `ENABLED_SERVICES`) update your secrets accordingly.

## API Reference

### SecretManager

- `SecretManager.getSecrets(environment = process.env.NODE_ENV || 'development')`
  - Returns: `Promise<Array>` — an array of parsed secret objects for the requested environment
  - Throws if region/credentials are missing (per environment), if no matching secrets are found, or on AWS API errors

- `SecretManager.getSecret(name, environment = process.env.NODE_ENV || 'development')`
  - Behavior: validates and fetches a single secret by name and will throw if the secret or the environment-specific key is missing. (Note: in the current implementation this method validates but does not return the parsed secret object.)

Example — read all secrets for the current environment:

```javascript
const { SecretManager } = require("universal_notification_support_lib");

async function load() {
  const secrets = await SecretManager.getSecrets();
  // secrets is an array of parsed objects for the current NODE_ENV
  console.log(secrets);
}

load().catch((err) => console.error(err));
```

### RabbitMQClient

Construct with an object: `{ url, config, logger = console }` where `config` is the client configuration that includes `.RABBITMQ` per service key.

Key methods:

- `connect()` — open connection & channel
- `publishMessage(serviceType, message)` — publish JSON message to the exchange/queue configured for `serviceType`
- `consume({ service, sender, db, maxProcessAttemptCount = 3 })` — start consuming messages; `sender` is an async function that actually sends the notification (e.g., call to SMTP/SMS connector). The client will update the DB `Notification` record status and attempts. Expects `db` to have `Notification` model and `sequelize` instance for transactions.
- `close()` — cleanly close consumer/channel/connection

The `publishMessage` and `consume` methods expect your configuration to include exchange/queue/routing keys under the service key (uppercased) and `RABBITMQ` object with `EXCHANGE_NAME`, `EXCHANGE_TYPE`, `ROUTING_KEY`, `QUEUE_NAME`.

Example (publish):

```javascript
const client = new RabbitMQClient({
  url: "amqp://user:pass@host:5672",
  config: myConfig,
});
await client.connect();
await client.publishMessage("email", {
  messageId: "123",
  content: {
    /* ... */
  },
});
await client.close();
```

Example (consume):

```javascript
await client.consume({
  service: "email",
  sender: async (msgData, messageId) => {
    // implement sending via connector (SMTP, SMS provider, etc.)
    return { ok: true };
  },
  db: myDbInstance,
});
```

### RabbitMQManager

Constructor: `new RabbitMQManager(fetchConfigsCallback, logger = console)`

- `fetchConfigsCallback()` should return a promise resolving to an array of client configs (each with `ID` and `RABBITMQ` settings)
- `getClient(clientId)` — returns a cached `RabbitMQClient` (creates and caches it if missing)
- `close(clientId)` — closes and evicts a single cached client
- `closeAll()` — closes and clears all cached clients

This manager uses an LRU cache to evict idle clients and closes them cleanly.

## Error handling & notes

- The package throws if required environment variables are missing (region, and for non-production, access key/secret)
- `SecretManager.getSecret` validates existence but currently does not return the parsed secret — use `getSecrets` to retrieve parsed secrets as an array. Consider calling `getSecret` only to validate presence or update it to return the parsed secret.
- The RabbitMQ client expects a Sequelize `db` with `Notification` model when consuming so it can update notification state.

## Troubleshooting

- "region is missing" or credential errors: verify `AWS_SECRET_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` (or use IAM role in production)
- "No secrets found with prefix": ensure `AWS_SECRET_NAME` matches the secret name prefix used in Secrets Manager
- RabbitMQ connection errors: verify `HOST`, `PORT`, `USER`, and `PASSWORD` in your client config and that the broker is reachable

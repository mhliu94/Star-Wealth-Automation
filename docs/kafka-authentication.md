# Kafka TLS and authentication

Set `KAFKA_BOOTSTRAP_SERVERS` to the authenticated listener and
`KAFKA_CLIENT_CONFIG_FILE` to a private JSON file containing librdkafka TLS/SASL
properties: `security.protocol`, `sasl.mechanism`, `sasl.username`, `sasl.password`,
and `ssl.ca.location`. The file must use SSL or SASL_SSL with certificate hostname
verification. Keep it outside Git and readable only by the service user.

Both reporting and command consumption use these settings. The loader rejects
consumer-offset and other non-security overrides. Omitting the file preserves
existing deployments.

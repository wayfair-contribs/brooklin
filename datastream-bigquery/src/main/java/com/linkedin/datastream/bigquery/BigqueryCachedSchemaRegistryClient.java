/*
 * Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.bigquery;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.kafka.common.config.ConfigException;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeGetResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProviderFactory;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProviderFactory;

/**
 * Thread-safe Schema Registry Client with client side caching.
 *
 * This implementation is a fork of Confluent's cached Schema Registry client.
 * The implementation was forked to gain control over the Avro schema parser, so we can validate schemas with invalid default values.
 *
 * Original source:
 * https://github.com/confluentinc/schema-registry/blob/v5.3.0/client/src/main/java/io/confluent/kafka/schemaregistry/client/CachedSchemaRegistryClient.java
 */
public class BigqueryCachedSchemaRegistryClient implements SchemaRegistryClient {

    private final RestService restService;
    private final int identityMapCapacity;
    private final Map<String, Map<Schema, Integer>> schemaCache;
    private final Map<String, Map<Integer, Schema>> idCache;
    private final Map<String, Map<Schema, Integer>> versionCache;
    private final boolean validateSchemaDefaults;
    private final boolean validateSchemaFieldNames;

    public static final Map<String, String> DEFAULT_REQUEST_PROPERTIES;

    static {
        final Map<String, String> properties = new HashMap<>();
        properties.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
        DEFAULT_REQUEST_PROPERTIES = Collections.unmodifiableMap(properties);
    }

    /**
     * Constructor.
     * @param baseUrl a comma delimited String pointing to schema registry endpoints
     * @param identityMapCapacity the maximum number of schemas to cache
     */
    public BigqueryCachedSchemaRegistryClient(String baseUrl, int identityMapCapacity) {
        this(new RestService(baseUrl), identityMapCapacity);
    }

    /**
     * Constructor.
     * @param baseUrls a list of Strings pointing to schema registry endpoints
     * @param identityMapCapacity the maximum number of schemas to cache
     */
    public BigqueryCachedSchemaRegistryClient(List<String> baseUrls, int identityMapCapacity) {
        this(new RestService(baseUrls), identityMapCapacity);
    }

    /**
     * Constructor.
     * @param restService a RestService
     * @param identityMapCapacity the maximum number of schemas to cache
     */
    public BigqueryCachedSchemaRegistryClient(RestService restService, int identityMapCapacity) {
        this(restService, identityMapCapacity, null);
    }

    /**
     * Constructor.
     * @param baseUrl a comma delimited String pointing to schema registry endpoints
     * @param identityMapCapacity the maximum number of schemas to cache
     * @param originals a configuration Map
     */
    public BigqueryCachedSchemaRegistryClient(
            String baseUrl,
            int identityMapCapacity,
            Map<String, ?> originals) {
        this(baseUrl, identityMapCapacity, originals, null);
    }

    /**
     * Constructor.
     * @param baseUrls a list of Strings pointing to schema registry endpoints
     * @param identityMapCapacity the maximum number of schemas to cache
     * @param originals a configuration Map
     */
    public BigqueryCachedSchemaRegistryClient(
            List<String> baseUrls,
            int identityMapCapacity,
            Map<String, ?> originals) {
        this(baseUrls, identityMapCapacity, originals, null);
    }

    /**
     * Constructor.
     * @param restService a RestService
     * @param identityMapCapacity the maximum number of schemas to cache
     * @param configs a configuration Map
     */
    public BigqueryCachedSchemaRegistryClient(
            RestService restService,
            int identityMapCapacity,
            Map<String, ?> configs) {
        this(restService, identityMapCapacity, configs, null);
    }

    /**
     * Constructor.
     * @param baseUrl a comma delimited String pointing to schema registry endpoints
     * @param identityMapCapacity the maximum number of schemas to cache
     * @param originals a configuration Map
     * @param httpHeaders a mapping of HTTP headers
     */
    public BigqueryCachedSchemaRegistryClient(
            String baseUrl,
            int identityMapCapacity,
            Map<String, ?> originals,
            Map<String, String> httpHeaders) {
        this(new RestService(baseUrl), identityMapCapacity, originals, httpHeaders);
    }

    /**
     * Constructor.
     * @param baseUrls a list of Strings pointing to schema registry endpoints
     * @param identityMapCapacity the maximum number of schemas to cache
     * @param originals a configuration Map
     * @param httpHeaders a mapping of HTTP headers
     */
    public BigqueryCachedSchemaRegistryClient(
            List<String> baseUrls,
            int identityMapCapacity,
            Map<String, ?> originals,
            Map<String, String> httpHeaders) {
        this(new RestService(baseUrls), identityMapCapacity, originals, httpHeaders);
    }

    /**
     * Constructor.
     * @param restService a RestService
     * @param identityMapCapacity the maximum number of schemas to cache
     * @param configs a configuration Map
     * @param httpHeaders a mapping of HTTP headers
     */
    public BigqueryCachedSchemaRegistryClient(
            RestService restService,
            int identityMapCapacity,
            Map<String, ?> configs,
            Map<String, String> httpHeaders) {
        this.identityMapCapacity = identityMapCapacity;
        this.schemaCache = new HashMap<>();
        this.idCache = new HashMap<>();
        this.versionCache = new HashMap<>();
        this.restService = restService;
        this.idCache.put(null, new HashMap<>());
        configureRestService(configs, httpHeaders);

        final Optional<Map<String, ?>> optionalConfigs = Optional.ofNullable(configs);
        validateSchemaFieldNames = optionalConfigs.map(c -> c.get(BigquerySchemaRegistryClientConfig.SCHEMA_REGISTRY_PARSER_VALIDATE_FIELD_NAMES))
                .map(v -> Boolean.valueOf(v.toString())).orElse(false);
        validateSchemaDefaults = optionalConfigs.map(c -> c.get(BigquerySchemaRegistryClientConfig.SCHEMA_REGISTRY_PARSER_VALIDATE_DEFAULTS))
                .map(v -> Boolean.valueOf(v.toString())).orElse(false);
    }

    private void configureRestService(Map<String, ?> configs, Map<String, String> httpHeaders) {
        if (httpHeaders != null) {
            restService.setHttpHeaders(httpHeaders);
        }

        if (configs != null) {
            String basicCredentialsSource =
                    (String) configs.get(BigquerySchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE);
            String bearerCredentialsSource =
                    (String) configs.get(BigquerySchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE);

            if (isNonEmpty(basicCredentialsSource) && isNonEmpty(bearerCredentialsSource)) {
                throw new ConfigException(String.format(
                        "Only one of '%s' and '%s' may be specified",
                        BigquerySchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
                        BigquerySchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE
                ));

            } else if (isNonEmpty(basicCredentialsSource)) {
                BasicAuthCredentialProvider basicAuthCredentialProvider =
                        BasicAuthCredentialProviderFactory.getBasicAuthCredentialProvider(
                                basicCredentialsSource,
                                configs
                        );
                restService.setBasicAuthCredentialProvider(basicAuthCredentialProvider);

            } else if (isNonEmpty(bearerCredentialsSource)) {
                BearerAuthCredentialProvider bearerAuthCredentialProvider =
                        BearerAuthCredentialProviderFactory.getBearerAuthCredentialProvider(
                                bearerCredentialsSource,
                                configs
                        );
                restService.setBearerAuthCredentialProvider(bearerAuthCredentialProvider);
            }
        }
    }

    private static boolean isNonEmpty(String s) {
        return s != null && !s.isEmpty();
    }

    private int registerAndGetId(String subject, Schema schema)
            throws IOException, RestClientException {
        return restService.registerSchema(schema.toString(), subject);
    }

    private int registerAndGetId(String subject, Schema schema, int version, int id)
            throws IOException, RestClientException {
        return restService.registerSchema(schema.toString(), subject, version, id);
    }

    private Schema getSchemaByIdFromRegistry(int id) throws IOException, RestClientException {
        SchemaString restSchema = restService.getId(id);
        return new Schema.Parser().setValidate(validateSchemaFieldNames).setValidateDefaults(validateSchemaDefaults).parse(restSchema.getSchemaString());
    }

    private int getVersionFromRegistry(String subject, Schema schema)
            throws IOException, RestClientException {
        io.confluent.kafka.schemaregistry.client.rest.entities.Schema response =
                restService.lookUpSubjectVersion(schema.toString(), subject, true);
        return response.getVersion();
    }

    private int getIdFromRegistry(String subject, Schema schema)
            throws IOException, RestClientException {
        io.confluent.kafka.schemaregistry.client.rest.entities.Schema response =
                restService.lookUpSubjectVersion(schema.toString(), subject, false);
        return response.getId();
    }

    @Override
    public synchronized int register(String subject, Schema schema)
            throws IOException, RestClientException {
        return register(subject, schema, 0, -1);
    }

    @Override
    public synchronized int register(String subject, Schema schema, int version, int id)
            throws IOException, RestClientException {
        final Map<Schema, Integer> schemaIdMap =
                schemaCache.computeIfAbsent(subject, k -> new HashMap<>());

        final Integer cachedId = schemaIdMap.get(schema);
        if (cachedId != null) {
            if (id >= 0 && id != cachedId) {
                throw new IllegalStateException("Schema already registered with id "
                        + cachedId + " instead of input id " + id);
            }
            return cachedId;
        }

        if (schemaIdMap.size() >= identityMapCapacity) {
            throw new IllegalStateException("Too many schema objects created for " + subject + "!");
        }

        final int retrievedId = id >= 0
                ? registerAndGetId(subject, schema, version, id)
                : registerAndGetId(subject, schema);
        schemaIdMap.put(schema, retrievedId);
        idCache.get(null).put(retrievedId, schema);
        return retrievedId;
    }

    @Override
    public Schema getByID(final int id) throws IOException, RestClientException {
        return getById(id);
    }

    @Override
    public synchronized Schema getById(int id) throws IOException, RestClientException {
        return getBySubjectAndId(null, id);
    }

    @Override
    public Schema getBySubjectAndID(final String subject, final int id)
            throws IOException, RestClientException {
        return getBySubjectAndId(subject, id);
    }

    @Override
    public synchronized Schema getBySubjectAndId(String subject, int id)
            throws IOException, RestClientException {

        final Map<Integer, Schema> idSchemaMap = idCache
                .computeIfAbsent(subject, k -> new HashMap<>());

        final Schema cachedSchema = idSchemaMap.get(id);
        if (cachedSchema != null) {
            return cachedSchema;
        }

        final Schema retrievedSchema = getSchemaByIdFromRegistry(id);
        idSchemaMap.put(id, retrievedSchema);
        return retrievedSchema;
    }

    @Override
    public SchemaMetadata getSchemaMetadata(String subject, int version)
            throws IOException, RestClientException {
        io.confluent.kafka.schemaregistry.client.rest.entities.Schema response
                = restService.getVersion(subject, version);
        int id = response.getId();
        String schema = response.getSchema();
        return new SchemaMetadata(id, version, schema);
    }

    @Override
    public synchronized SchemaMetadata getLatestSchemaMetadata(String subject)
            throws IOException, RestClientException {
        io.confluent.kafka.schemaregistry.client.rest.entities.Schema response
                = restService.getLatestVersion(subject);
        int id = response.getId();
        int version = response.getVersion();
        String schema = response.getSchema();
        return new SchemaMetadata(id, version, schema);
    }

    @Override
    public synchronized int getVersion(String subject, Schema schema)
            throws IOException, RestClientException {
        final Map<Schema, Integer> schemaVersionMap =
                versionCache.computeIfAbsent(subject, k -> new HashMap<>());

        final Integer cachedVersion = schemaVersionMap.get(schema);
        if (cachedVersion != null) {
            return cachedVersion;
        }

        if (schemaVersionMap.size() >= identityMapCapacity) {
            throw new IllegalStateException("Too many schema objects created for " + subject + "!");
        }

        final int retrievedVersion = getVersionFromRegistry(subject, schema);
        schemaVersionMap.put(schema, retrievedVersion);
        return retrievedVersion;
    }

    @Override
    public List<Integer> getAllVersions(String subject)
            throws IOException, RestClientException {
        return restService.getAllVersions(subject);
    }

    @Override
    public synchronized int getId(String subject, Schema schema)
            throws IOException, RestClientException {
        final Map<Schema, Integer> schemaIdMap =
                schemaCache.computeIfAbsent(subject, k -> new HashMap<>());

        final Integer cachedId = schemaIdMap.get(schema);
        if (cachedId != null) {
            return cachedId;
        }

        if (schemaIdMap.size() >= identityMapCapacity) {
            throw new IllegalStateException("Too many schema objects created for " + subject + "!");
        }

        final int retrievedId = getIdFromRegistry(subject, schema);
        schemaIdMap.put(schema, retrievedId);
        idCache.get(null).put(retrievedId, schema);
        return retrievedId;
    }

    @Override
    public List<Integer> deleteSubject(String subject) throws IOException, RestClientException {
        return deleteSubject(DEFAULT_REQUEST_PROPERTIES, subject);
    }

    @Override
    public synchronized List<Integer> deleteSubject(
            Map<String, String> requestProperties, String subject)
            throws IOException, RestClientException {
        Objects.requireNonNull(subject, "subject");
        versionCache.remove(subject);
        idCache.remove(subject);
        schemaCache.remove(subject);
        return restService.deleteSubject(requestProperties, subject);
    }

    @Override
    public Integer deleteSchemaVersion(String subject, String version)
            throws IOException, RestClientException {
        return deleteSchemaVersion(DEFAULT_REQUEST_PROPERTIES, subject, version);
    }

    @Override
    public synchronized Integer deleteSchemaVersion(
            Map<String, String> requestProperties,
            String subject,
            String version)
            throws IOException, RestClientException {
        versionCache
                .getOrDefault(subject, Collections.emptyMap())
                .values()
                .remove(Integer.valueOf(version));
        return restService.deleteSchemaVersion(requestProperties, subject, version);
    }

    @Override
    public boolean testCompatibility(String subject, Schema schema)
            throws IOException, RestClientException {
        return restService.testCompatibility(schema.toString(), subject, "latest");
    }

    @Override
    public String updateCompatibility(String subject, String compatibility)
            throws IOException, RestClientException {
        ConfigUpdateRequest response = restService.updateCompatibility(compatibility, subject);
        return response.getCompatibilityLevel();
    }

    @Override
    public String getCompatibility(String subject) throws IOException, RestClientException {
        Config response = restService.getConfig(subject);
        return response.getCompatibilityLevel();
    }

    @Override
    public String setMode(String mode)
            throws IOException, RestClientException {
        ModeUpdateRequest response = restService.setMode(mode);
        return response.getMode();
    }

    @Override
    public String setMode(String mode, String subject)
            throws IOException, RestClientException {
        ModeUpdateRequest response = restService.setMode(mode, subject);
        return response.getMode();
    }

    @Override
    public String getMode() throws IOException, RestClientException {
        ModeGetResponse response = restService.getMode();
        return response.getMode();
    }

    @Override
    public String getMode(String subject) throws IOException, RestClientException {
        ModeGetResponse response = restService.getMode(subject);
        return response.getMode();
    }

    @Override
    public Collection<String> getAllSubjects() throws IOException, RestClientException {
        return restService.getAllSubjects();
    }

    @Override
    public void reset() {
        schemaCache.clear();
        idCache.clear();
        versionCache.clear();
        idCache.put(null, new HashMap<>());
    }
}
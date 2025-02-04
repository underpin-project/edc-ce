/*
 * Copyright (c) 2025 Graphwise.ai
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Graphwise.ai - init
 *
 */

package de.sovity.edc.utils.catalog;

import lombok.RequiredArgsConstructor;
import org.eclipse.edc.spi.monitor.Monitor;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.Response.Status.ACCEPTED;

@RequiredArgsConstructor
public class CatalogMetadataSender {

    private HttpClient httpClient;
    private int maxTrials;
    private String metadataIngesterEndpoint;

    private final Monitor monitor;

    public CatalogMetadataSender(int maxTrials, int connectionTimeout, String metadataIngesterEndpoint, Monitor monitor) {
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(connectionTimeout)).build();
        this.maxTrials = maxTrials;
        this.metadataIngesterEndpoint = metadataIngesterEndpoint;
        this.monitor = monitor;
    }

    public void sendMetadataForIngestion(String metadata) {
        int statusCode = 0;
        int trialNumber = 0;
        do {
            trialNumber++;
            try {
                HttpRequest request = HttpRequest.newBuilder(new URI(metadataIngesterEndpoint))
                    .header(CONTENT_TYPE, APPLICATION_JSON)
                    .timeout(Duration.of(60, ChronoUnit.SECONDS))
                    .POST(HttpRequest.BodyPublishers.ofString(metadata))
                    .build();
                CompletableFuture<Integer> responseCode = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenApply(HttpResponse::statusCode);
                statusCode = responseCode.get();
            } catch (InterruptedException | ExecutionException | URISyntaxException e) {
                monitor.severe("Could not successfully send the metadata to the Metadata Ingester!", e);
            }
        } while ((statusCode != ACCEPTED.getStatusCode()) && trialNumber <= maxTrials);
    }
}

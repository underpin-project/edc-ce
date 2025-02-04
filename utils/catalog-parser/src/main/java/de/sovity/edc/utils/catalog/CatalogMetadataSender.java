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

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(60)).build();
    private static final int MAX_TRIALS = 3;
    private static final String METADATA_INGESTER_ENDPOINT = "http://metadata-ingester:8080/ingest";

    private final Monitor monitor;

    public void sendMetadataForIngestion(String metadata) {
        int statusCode = 0;
        int trialNumber = 0;
        do {
            trialNumber++;
            try {
                HttpRequest request = HttpRequest.newBuilder(new URI(METADATA_INGESTER_ENDPOINT))
                    .header(CONTENT_TYPE, APPLICATION_JSON)
                    .timeout(Duration.of(60, ChronoUnit.SECONDS))
                    .POST(HttpRequest.BodyPublishers.ofString(metadata))
                    .build();
                CompletableFuture<Integer> responseCode = HTTP_CLIENT.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenApply(HttpResponse::statusCode);
                statusCode = responseCode.get();
            } catch (InterruptedException | ExecutionException | URISyntaxException e) {
                monitor.severe("Could not successfully send the metadata to the Metadata Ingester!", e);
            }
        } while ((statusCode != ACCEPTED.getStatusCode()) && trialNumber <= MAX_TRIALS);
    }
}

package com.github.ambry.frontend;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.rest.RestRequestMetrics;


/**
 * Ambry frontend specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the Ambry frontend to the provided {@link MetricRegistry}.
 */
class FrontendMetrics {

  // RestRequestMetrics instances
  // DELETE
  public final RestRequestMetrics deleteBlobMetrics;
  // HEAD
  public final RestRequestMetrics headBlobMetrics;
  // GET
  public final RestRequestMetrics getBlobInfoMetrics;
  public final RestRequestMetrics getBlobMetrics;
  public final RestRequestMetrics getUserMetadataMetrics;
  // POST
  public final RestRequestMetrics postBlobMetrics;

  // Latencies
  // AmbryBlobStorageService
  // DELETE
  public final Histogram deletePreProcessingTimeInMs;
  // HEAD
  public final Histogram headPreProcessingTimeInMs;
  // GET
  public final Histogram getPreProcessingTimeInMs;
  // POST
  public final Histogram blobPropsBuildTimeInMs;
  public final Histogram postPreProcessingTimeInMs;
  // DeleteCallback
  public final Histogram deleteCallbackProcessingTimeInMs;
  public final Histogram deleteTimeInMs;
  // HeadCallback
  public final Histogram headCallbackProcessingTimeInMs;
  public final Histogram headTimeInMs;
  public final Histogram headSecurityResponseTimeInMs;
  public final Histogram headSecurityResponseCallbackProcessingTimeInMs;
  // HeadForGetCallback
  public final Histogram headForGetCallbackProcessingTimeInMs;
  public final Histogram headForGetTimeInMs;
  public final Histogram getSecurityResponseCallbackProcessingTimeInMs;
  public final Histogram getSecurityResponseTimeInMs;
  // GetCallback
  public final Histogram getCallbackProcessingTimeInMs;
  public final Histogram getTimeInMs;
  // PostCallback
  public final Histogram outboundIdConversionCallbackProcessingTimeInMs;
  public final Histogram outboundIdConversionTimeInMs;
  public final Histogram postCallbackProcessingTimeInMs;
  public final Histogram postTimeInMs;
  // InboundIdConverterCallback
  public final Histogram inboundIdConversionCallbackProcessingTimeInMs;
  public final Histogram inboundIdConversionTimeInMs;
  // SecurityProcessRequestCallback
  public final Histogram deleteSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram getSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram headSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram postSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram deleteSecurityRequestTimeInMs;
  public final Histogram getSecurityRequestTimeInMs;
  public final Histogram headSecurityRequestTimeInMs;
  public final Histogram postSecurityRequestTimeInMs;

  // Errors
  // AmbryBlobStorageService
  public final Counter responseSubmissionError;
  public final Counter resourceReleaseError;
  // DeleteCallback
  public final Counter deleteCallbackProcessingError;
  // HeadCallback
  public final Counter headCallbackProcessingError;
  // HeadForGetCallback
  public final Counter headForGetCallbackProcessingError;
  public final Counter getSecurityResponseCallbackProcessingError;
  // GetCallback
  public final Counter getCallbackProcessingError;
  // PostCallback
  public final Counter postCallbackProcessingError;
  public final Counter outboundIdConversionCallbackProcessingError;

  // Other
  // AmbryBlobStorageService
  public final Histogram blobStorageServiceStartupTimeInMs;
  public final Histogram blobStorageServiceShutdownTimeInMs;

  /**
   * Creates an instance of FrontendMetrics using the given {@code metricRegistry}.
   * @param metricRegistry the {@link MetricRegistry} to use for the metrics.
   */
  public FrontendMetrics(MetricRegistry metricRegistry) {
    // RestRequestMetrics instances
    // DELETE
    deleteBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "DeleteBlob", metricRegistry);
    // HEAD
    headBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "HeadBlob", metricRegistry);
    // GET
    getBlobInfoMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "GetBlobInfo", metricRegistry);
    getBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "GetBlob", metricRegistry);
    getUserMetadataMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "GetUserMetadata", metricRegistry);
    // POST
    postBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "PostBlob", metricRegistry);

    // Latencies
    // AmbryBlobStorageService
    // DELETE
    deletePreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "DeletePreProcessingTimeInMs"));
    // HEAD
    headPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "HeadPreProcessingTimeInMs"));
    // GET
    getPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "GetPreProcessingTimeInMs"));
    // POST
    blobPropsBuildTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "BlobPropsBuildTimeInMs"));
    postPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "PostPreProcessingTimeInMs"));
    // DeleteCallback
    deleteCallbackProcessingTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(AmbryBlobStorageService.class, "DeleteCallbackProcessingTimeInMs"));
    deleteTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "DeleteCallbackResultTimeInMs"));
    // HeadCallback
    headCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "HeadCallbackProcessingTimeInMs"));
    headTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "HeadCallbackResultTimeInMs"));
    headSecurityResponseCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AmbryBlobStorageService.class, "HeadSecurityResponseCallbackProcessingTimeInMs"));
    headSecurityResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "HeadSecurityResponseTimeInMs"));
    // HeadForGetCallback
    headForGetCallbackProcessingTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(AmbryBlobStorageService.class, "HeadForGetCallbackProcessingTimeInMs"));
    headForGetTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(AmbryBlobStorageService.class, "HeadForGetCallbackResultTimeInMs"));
    getSecurityResponseCallbackProcessingTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(AmbryBlobStorageService.class, "GetSecurityResponseCallbackProcessingTimeInMs"));
    getSecurityResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "GetSecurityResponseTimeInMs"));
    // GetCallback
    getCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "GetCallbackProcessingTimeInMs"));
    getTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "GetCallbackResultTimeInMs"));
    // PostCallback
    outboundIdConversionCallbackProcessingTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(AmbryBlobStorageService.class, "OutboundIdCallbackProcessingTimeInMs"));
    outboundIdConversionTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "OutboundIdConversionTimeInMs"));
    postCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "PostCallbackProcessingTimeInMs"));
    postTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "PostCallbackResultTimeInMs"));
    // InboundIdConverterCallback
    inboundIdConversionCallbackProcessingTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(AmbryBlobStorageService.class, "InboundIdCallbackProcessingTimeInMs"));
    inboundIdConversionTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "InboundIdConversionTimeInMs"));
    // SecurityProcessRequestCallback
    deleteSecurityRequestCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AmbryBlobStorageService.class, "DeleteSecurityRequestCallbackProcessingTimeInMs"));
    deleteSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "DeleteSecurityRequestTimeInMs"));
    headSecurityRequestCallbackProcessingTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(AmbryBlobStorageService.class, "HeadSecurityRequestCallbackProcessingTimeInMs"));
    headSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "HeadSecurityRequestTimeInMs"));
    getSecurityRequestCallbackProcessingTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(AmbryBlobStorageService.class, "GetSecurityRequestCallbackProcessingTimeInMs"));
    getSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "GetSecurityRequestTimeInMs"));
    postSecurityRequestCallbackProcessingTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(AmbryBlobStorageService.class, "PostSecurityRequestCallbackProcessingTimeInMs"));
    postSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "PostSecurityRequestTimeInMs"));

    // Errors
    // AmbryBlobStorageService
    responseSubmissionError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "ResponseSubmissionError"));
    resourceReleaseError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "ResourceReleaseError"));
    // DeleteCallback
    deleteCallbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "DeleteCallbackProcessingError"));
    // HeadCallback
    headCallbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "HeadCallbackProcessingError"));
    // HeadForGetCallback
    headForGetCallbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "HeadForGetCallbackProcessingError"));
    getSecurityResponseCallbackProcessingError = metricRegistry
        .counter(MetricRegistry.name(AmbryBlobStorageService.class, "GetSecurityResponseCallbackProcessingError"));
    // GetCallback
    getCallbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "GetCallbackProcessingError"));
    // PostCallback
    postCallbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "PostCallbackProcessingError"));
    outboundIdConversionCallbackProcessingError = metricRegistry
        .counter(MetricRegistry.name(AmbryBlobStorageService.class, "OutboundIdConversionCallbackProcessingError"));

    // Other
    blobStorageServiceStartupTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "StartupTimeInMs"));
    blobStorageServiceShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "ShutdownTimeInMs"));
  }
}

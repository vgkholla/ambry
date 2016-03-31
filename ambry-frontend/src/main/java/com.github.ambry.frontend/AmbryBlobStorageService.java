package com.github.ambry.frontend;

import com.codahale.metrics.Histogram;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.IdConverter;
import com.github.ambry.rest.IdConverterFactory;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestResponseHandler;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.rest.SecurityService;
import com.github.ambry.rest.SecurityServiceFactory;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is an Ambry frontend specific implementation of {@link BlobStorageService}.
 * <p/>
 * All the operations that need to be performed by the Ambry frontend are supported here.
 */
class AmbryBlobStorageService implements BlobStorageService {
  protected static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
  protected final FrontendMetrics frontendMetrics;

  private final ClusterMap clusterMap;
  private final RestResponseHandler responseHandler;
  private final Router router;
  private final IdConverterFactory idConverterFactory;
  private final SecurityServiceFactory securityServiceFactory;
  private final Logger logger = LoggerFactory.getLogger(AmbryBlobStorageService.class);

  private IdConverter idConverter = null;
  private SecurityService securityService = null;
  private boolean isUp = false;

  /**
   * Create a new instance of AmbryBlobStorageService by supplying it with config, metrics, cluster map, a
   * response handler controller and a router.
   * @param frontendMetrics the metrics instance to use in the form of {@link FrontendMetrics}.
   * @param clusterMap the {@link ClusterMap} to be used for operations.
   * @param responseHandler the {@link RestResponseHandler} that can be used to submit responses that need to be sent
   *                        out.
   * @param router the {@link Router} instance to use to perform blob operations.
   * @param idConverterFactory the {@link IdConverterFactory} to use to get an {@link IdConverter}.
   * @param securityServiceFactory the {@link SecurityServiceFactory} to use to get an {@link SecurityService}.
   */
  public AmbryBlobStorageService(FrontendMetrics frontendMetrics, ClusterMap clusterMap,
      RestResponseHandler responseHandler, Router router, IdConverterFactory idConverterFactory,
      SecurityServiceFactory securityServiceFactory) {
    this.frontendMetrics = frontendMetrics;
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.router = router;
    this.idConverterFactory = idConverterFactory;
    this.securityServiceFactory = securityServiceFactory;
    logger.trace("Instantiated AmbryBlobStorageService");
  }

  @Override
  public void start()
      throws InstantiationException {
    long startupBeginTime = System.currentTimeMillis();
    idConverter = idConverterFactory.getIdConverter();
    securityService = securityServiceFactory.getSecurityService();
    isUp = true;
    logger.info("AmbryBlobStorageService has started");
    frontendMetrics.blobStorageServiceStartupTimeInMs.update(System.currentTimeMillis() - startupBeginTime);
  }

  @Override
  public void shutdown() {
    long shutdownBeginTime = System.currentTimeMillis();
    isUp = false;
    try {
      if (securityService != null) {
        securityService.close();
        securityService = null;
      }
      if (idConverter != null) {
        idConverter.close();
        idConverter = null;
      }
      logger.info("AmbryBlobStorageService shutdown complete");
    } catch (IOException e) {
      logger.error("Downstream service close failed", e);
    } finally {
      frontendMetrics.blobStorageServiceShutdownTimeInMs.update(System.currentTimeMillis() - shutdownBeginTime);
    }
  }

  @Override
  public void handleGet(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    try {
      logger.trace("Handling GET request - {}", restRequest.getUri());
      checkAvailable();
      RestUtils.SubResource subresource = RestUtils.getBlobSubResource(restRequest);
      RestRequestMetrics requestMetrics = frontendMetrics.getBlobMetrics;
      if (subresource != null) {
        logger.trace("Sub-resource requested: {}", subresource);
        switch (subresource) {
          case BlobInfo:
            requestMetrics = frontendMetrics.getBlobInfoMetrics;
            break;
          case UserMetadata:
            requestMetrics = frontendMetrics.getUserMetadataMetrics;
            break;
        }
      }
      restRequest.getMetricsTracker().injectMetrics(requestMetrics);
      HeadForGetCallback routerCallback = new HeadForGetCallback(restRequest, restResponseChannel, subresource);
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      SecurityProcessRequestCallback securityCallback =
          new SecurityProcessRequestCallback(restRequest, restResponseChannel, routerCallback);
      securityService.processRequest(restRequest, securityCallback);
    } catch (Exception e) {
      submitResponse(restRequest, restResponseChannel, null, e);
    } finally {
      frontendMetrics.getPreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(preProcessingTime);
    }
  }

  @Override
  public void handlePost(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    restRequest.getMetricsTracker().injectMetrics(frontendMetrics.postBlobMetrics);
    try {
      logger.trace("Handling POST request - {}", restRequest.getUri());
      checkAvailable();
      long propsBuildStartTime = System.currentTimeMillis();
      BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest);
      byte[] usermetadata = RestUtils.buildUsermetadata(restRequest);
      frontendMetrics.blobPropsBuildTimeInMs.update(System.currentTimeMillis() - propsBuildStartTime);
      logger.trace("Blob properties of blob being POSTed - {}", blobProperties);
      PostCallback routerCallback = new PostCallback(restRequest, restResponseChannel, blobProperties);
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      SecurityProcessRequestCallback securityCallback =
          new SecurityProcessRequestCallback(restRequest, restResponseChannel, blobProperties, usermetadata,
              routerCallback);
      securityService.processRequest(restRequest, securityCallback);
    } catch (Exception e) {
      submitResponse(restRequest, restResponseChannel, null, e);
    } finally {
      frontendMetrics.postPreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(preProcessingTime);
    }
  }

  @Override
  public void handleDelete(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    restRequest.getMetricsTracker().injectMetrics(frontendMetrics.deleteBlobMetrics);
    try {
      logger.trace("Handling DELETE request - {}", restRequest.getUri());
      checkAvailable();
      DeleteCallback routerCallback = new DeleteCallback(restRequest, restResponseChannel);
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      SecurityProcessRequestCallback securityCallback =
          new SecurityProcessRequestCallback(restRequest, restResponseChannel, routerCallback);
      securityService.processRequest(restRequest, securityCallback);
    } catch (Exception e) {
      submitResponse(restRequest, restResponseChannel, null, e);
    } finally {
      frontendMetrics.deletePreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(preProcessingTime);
    }
  }

  @Override
  public void handleHead(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    restRequest.getMetricsTracker().injectMetrics(frontendMetrics.headBlobMetrics);
    try {
      logger.trace("Handling HEAD request - {}", restRequest.getUri());
      checkAvailable();
      HeadCallback routerCallback = new HeadCallback(restRequest, restResponseChannel);
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      SecurityProcessRequestCallback securityCallback =
          new SecurityProcessRequestCallback(restRequest, restResponseChannel, routerCallback);
      securityService.processRequest(restRequest, securityCallback);
    } catch (Exception e) {
      submitResponse(restRequest, restResponseChannel, null, e);
    } finally {
      frontendMetrics.headPreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(preProcessingTime);
    }
  }

  /**
   * Submits the  {@code response} (and any {@code exception})for the {@code restRequest} to the
   * {@code responseHandler}.
   * @param restRequest the {@link RestRequest} for which a a {@code response} is ready.
   * @param restResponseChannel the {@link RestResponseChannel} over which the response can be sent.
   * @param response the response in the form of a {@link ReadableStreamChannel}.
   * @param exception any {@link Exception} that occurred during the handling of {@code restRequest}.
   */
  protected void submitResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel response, Exception exception) {
    try {
      if (exception != null && exception instanceof RouterException) {
        exception = new RestServiceException(exception,
            RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
      }
      responseHandler.handleResponse(restRequest, restResponseChannel, response, exception);
    } catch (RestServiceException e) {
      frontendMetrics.responseSubmissionError.inc();
      if (exception != null) {
        logger.error("Error submitting response to response handler", e);
      } else {
        exception = e;
      }
      logger.error("Handling of request {} failed", restRequest.getUri(), exception);
      restResponseChannel.onResponseComplete(exception);

      if (response != null) {
        try {
          response.close();
        } catch (IOException ioe) {
          frontendMetrics.resourceReleaseError.inc();
          logger.error("Error closing ReadableStreamChannel", e);
        }
      }
    }
  }

  /**
   * Checks for bad arguments or states.
   * @param restRequest the {@link RestRequest} to use. Cannot be null.
   * @param restResponseChannel the {@link RestResponseChannel} to use. Cannot be null.
   */
  private void handlePrechecks(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    if (restRequest == null || restResponseChannel == null) {
      StringBuilder errorMessage = new StringBuilder("Null arg(s) received -");
      if (restRequest == null) {
        errorMessage.append(" [RestRequest] ");
      }
      if (restResponseChannel == null) {
        errorMessage.append(" [RestResponseChannel] ");
      }
      throw new IllegalArgumentException(errorMessage.toString());
    }
  }

  /**
   * Checks if {@link AmbryBlobStorageService} is available to serve requests.
   * @throws RestServiceException if {@link AmbryBlobStorageService} is not available to serve requests.
   */
  private void checkAvailable()
      throws RestServiceException {
    if (!isUp) {
      throw new RestServiceException("AmbryBlobStorageService unavailable", RestServiceErrorCode.ServiceUnavailable);
    }
  }

  /**
   * Callback for {@link IdConverter} that is used when inbound IDs are converted.
   */
  private class InboundIdConverterCallback implements Callback<String> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final HeadForGetCallback headForGetCallback;
    private final HeadCallback headCallback;
    private final DeleteCallback deleteCallback;
    private final CallbackTracker callbackTracker;

    private InboundIdConverterCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        HeadForGetCallback callback) {
      this(restRequest, restResponseChannel, callback, null, null);
    }

    private InboundIdConverterCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        HeadCallback callback) {
      this(restRequest, restResponseChannel, null, callback, null);
    }

    private InboundIdConverterCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        DeleteCallback callback) {
      this(restRequest, restResponseChannel, null, null, callback);
    }

    private InboundIdConverterCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        HeadForGetCallback headForGetCallback, HeadCallback headCallback, DeleteCallback deleteCallback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.headForGetCallback = headForGetCallback;
      this.headCallback = headCallback;
      this.deleteCallback = deleteCallback;
      callbackTracker =
          new CallbackTracker(restRequest, "Inbound Id Conversion", frontendMetrics.inboundIdConversionTimeInMs,
              frontendMetrics.inboundIdConversionCallbackProcessingTimeInMs);
      callbackTracker.markOperationStart();
    }

    /**
     * Forwards request to the {@link Router} once ID conversion is complete.
     * @param result The converted ID. This would be non null when the request executed successfully
     * @param exception The exception that was reported on execution of the request
     */
    @Override
    public void onCompletion(String result, Exception exception) {
      callbackTracker.markOperationEnd();
      try {
        if (result == null && exception == null) {
          throw new IllegalStateException("Both result and exception cannot be null");
        } else if (exception != null) {
          submitResponse(restRequest, restResponseChannel, null, exception);
        } else {
          RestMethod restMethod = restRequest.getRestMethod();
          logger.trace("Forwarding {} of {} to the router", restMethod, result);
          switch (restMethod) {
            case GET:
              headForGetCallback.markStartTime();
              router.getBlobInfo(result, headForGetCallback);
              break;
            case HEAD:
              headCallback.markStartTime();
              router.getBlobInfo(result, headCallback);
              break;
            case DELETE:
              deleteCallback.markStartTime();
              router.deleteBlob(result, deleteCallback);
              break;
            default:
              throw new IllegalStateException("Unrecognized RestMethod: " + restMethod);
          }
        }
      } finally {
        callbackTracker.markCallbackProcessingEnd();
      }
    }
  }

  /**
   * Callback for {@link SecurityService#processRequest(RestRequest, Callback)}.
   */
  private class SecurityProcessRequestCallback implements Callback<Void> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final CallbackTracker callbackTracker;

    private HeadForGetCallback headForGetCallback;
    private HeadCallback headCallback;
    private PostCallback postCallback;
    private DeleteCallback deleteCallback;

    private BlobProperties blobProperties;
    private byte[] userMetadata;

    private SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        HeadForGetCallback callback) {
      this(restRequest, restResponseChannel, "Security Process GET Request", frontendMetrics.getSecurityRequestTimeInMs,
          frontendMetrics.getSecurityRequestCallbackProcessingTimeInMs);
      this.headForGetCallback = callback;
    }

    private SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        HeadCallback callback) {
      this(restRequest, restResponseChannel, "Security Process HEAD Request",
          frontendMetrics.headSecurityRequestTimeInMs, frontendMetrics.headSecurityRequestCallbackProcessingTimeInMs);
      this.headCallback = callback;
    }

    private SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        BlobProperties blobProperties, byte[] userMetadata, PostCallback callback) {
      this(restRequest, restResponseChannel, "Security Process POST Request",
          frontendMetrics.postSecurityRequestTimeInMs, frontendMetrics.postSecurityRequestCallbackProcessingTimeInMs);
      this.blobProperties = blobProperties;
      this.userMetadata = userMetadata;
      this.postCallback = callback;
    }

    private SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        DeleteCallback callback) {
      this(restRequest, restResponseChannel, "Security Process DELETE Request",
          frontendMetrics.deleteSecurityRequestTimeInMs,
          frontendMetrics.deleteSecurityRequestCallbackProcessingTimeInMs);
      this.deleteCallback = callback;
    }

    private SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        String operationType, Histogram operationTimeTracker, Histogram callbackProcessingTimeTracker) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      callbackTracker =
          new CallbackTracker(restRequest, operationType, operationTimeTracker, callbackProcessingTimeTracker);
      callbackTracker.markOperationStart();
    }

    /**
     * Handles request once it has been vetted by the {@link SecurityService}.
     * In case of exception, response is immediately submitted to the {@link RestResponseHandler}.
     * In case of GET, HEAD and DELETE, ID conversion is triggered.
     * In case of POST, request is forwarded to the {@link Router}.
     * @param result The result of the request. This would be non null when the request executed successfully
     * @param exception The exception that was reported on execution of the request
     */
    @Override
    public void onCompletion(Void result, Exception exception) {
      try {
        callbackTracker.markOperationEnd();
        if (exception != null) {
          submitResponse(restRequest, restResponseChannel, null, exception);
        } else {
          RestMethod restMethod = restRequest.getRestMethod();
          logger.trace("Forwarding {} to the IdConverter/Router", restMethod);
          switch (restMethod) {
            case GET:
              String receivedId = RestUtils.getOperationOrBlobIdFromUri(restRequest);
              InboundIdConverterCallback idConverterCallback =
                  new InboundIdConverterCallback(restRequest, restResponseChannel, headForGetCallback);
              idConverter.convert(restRequest, receivedId, idConverterCallback);
              break;
            case HEAD:
              receivedId = RestUtils.getOperationOrBlobIdFromUri(restRequest);
              idConverterCallback = new InboundIdConverterCallback(restRequest, restResponseChannel, headCallback);
              idConverter.convert(restRequest, receivedId, idConverterCallback);
              break;
            case POST:
              postCallback.markStartTime();
              router.putBlob(blobProperties, userMetadata, restRequest, postCallback);
              break;
            case DELETE:
              receivedId = RestUtils.getOperationOrBlobIdFromUri(restRequest);
              idConverterCallback = new InboundIdConverterCallback(restRequest, restResponseChannel, deleteCallback);
              idConverter.convert(restRequest, receivedId, idConverterCallback);
              break;
            default:
              throw new IllegalStateException("Unrecognized RestMethod: " + restMethod);
          }
        }
      } finally {
        callbackTracker.markCallbackProcessingEnd();
      }
    }
  }

  /**
   * Tracks metrics and logs progress of operations that accept callbacks.
   */
  private class CallbackTracker {
    private long operationStartTime = 0;
    private long processingStartTime = 0;

    private final RestRequest restRequest;
    private final String operationType;
    private final Histogram operationTimeTracker;
    private final Histogram callbackProcessingTimeTracker;
    private final String blobId;

    /**
     * Create a CallbackTracker that tracks a particular operation.
     * @param restRequest the {@link RestRequest} for the operation.
     * @param operationType the type of operation.
     * @param operationTimeTracker the {@link Histogram} of the time taken by the operation.
     * @param callbackProcessingTimeTracker the {@link Histogram} of the time taken by the callback of the operation.
     */
    protected CallbackTracker(RestRequest restRequest, String operationType, Histogram operationTimeTracker,
        Histogram callbackProcessingTimeTracker) {
      this.restRequest = restRequest;
      this.operationType = operationType;
      this.operationTimeTracker = operationTimeTracker;
      this.callbackProcessingTimeTracker = callbackProcessingTimeTracker;
      blobId = RestUtils.getOperationOrBlobIdFromUri(restRequest);
    }

    /**
     * Marks that the operation being tracked has started.
     */
    private void markOperationStart() {
      logger.trace("{} started for {}", operationType, blobId);
      operationStartTime = System.currentTimeMillis();
    }

    /**
     * Marks that the operation being tracked has ended and callback processing has started.
     */
    private void markOperationEnd() {
      logger.trace("{} finished for {}", operationType, blobId);
      processingStartTime = System.currentTimeMillis();
      long operationTime = processingStartTime - operationStartTime;
      operationTimeTracker.update(operationTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(operationTime);
    }

    /**
     * Marks that the  callback processing has ended.
     */
    private void markCallbackProcessingEnd() {
      logger.trace("Callback for {} of {} finished", operationType, blobId);
      long processingTime = System.currentTimeMillis() - processingStartTime;
      callbackProcessingTimeTracker.update(processingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(processingTime);
    }
  }

  /**
   * Callback for HEAD that precedes GET operations. Updates headers and invokes GET with a new callback.
   */
  private class HeadForGetCallback implements Callback<BlobInfo> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final RestUtils.SubResource subResource;
    private final CallbackTracker callbackTracker;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Create a HEAD before GET callback.
     * @param restRequest the {@link RestRequest} for whose response this is a callback.
     * @param restResponseChannel the {@link RestResponseChannel} to set headers on.
     * @param subResource the sub-resource requested.
     */
    public HeadForGetCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        RestUtils.SubResource subResource) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.subResource = subResource;
      callbackTracker = new CallbackTracker(restRequest, "HEAD before GET", frontendMetrics.headForGetTimeInMs,
          frontendMetrics.headForGetCallbackProcessingTimeInMs);
    }

    /**
     * If the request is not for a sub resource, makes a GET call to the router. If the request is for a sub resource,
     * responds immediately. If there was no {@code routerResult} or if there was an exception, bails out.
     * @param routerResult The result of the request i.e a {@link BlobInfo} object with the properties of the blob that
     *                     is going to be scheduled for GET. This is non null if the request executed successfully.
     * @param exception The exception that was reported on execution of the request (if any).
     */
    @Override
    public void onCompletion(final BlobInfo routerResult, Exception exception) {
      try {
        callbackTracker.markOperationEnd();
        final String blobId = RestUtils.getOperationOrBlobIdFromUri(restRequest);
        if (exception == null && routerResult != null) {
          final CallbackTracker securityCallbackTracker =
              new CallbackTracker(restRequest, "GET Response Security", frontendMetrics.getSecurityResponseTimeInMs,
                  frontendMetrics.getSecurityResponseCallbackProcessingTimeInMs);
          securityCallbackTracker.markOperationStart();
          securityService.processResponse(restRequest, restResponseChannel, routerResult, new Callback<Void>() {
            @Override
            public void onCompletion(Void antivirusResult, Exception exception) {
              securityCallbackTracker.markOperationEnd();
              ReadableStreamChannel response = null;
              try {
                if (exception == null) {
                  if (subResource == null) {
                    logger.trace("Forwarding GET after HEAD for {} to the router", blobId);
                    router.getBlob(blobId, new GetCallback(restRequest, restResponseChannel));
                  } else {
                    // TODO: if old style, make RestUtils.getUserMetadata() just return null.
                    Map<String, String> userMetadata = RestUtils.buildUserMetadata(routerResult.getUserMetadata());
                    if (shouldSendMetadataAsContent(userMetadata)) {
                      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, "application/octet-stream");
                      restResponseChannel
                          .setHeader(RestUtils.Headers.CONTENT_LENGTH, routerResult.getUserMetadata().length);
                      response = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(routerResult.getUserMetadata()));
                    } else {
                      setUserMetadataHeaders(userMetadata, restResponseChannel);
                      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
                      response = new ByteBufferReadableStreamChannel(AmbryBlobStorageService.EMPTY_BUFFER);
                    }
                  }
                }
              } catch (RestServiceException e) {
                frontendMetrics.getSecurityResponseCallbackProcessingError.inc();
                exception = e;
              } finally {
                securityCallbackTracker.markCallbackProcessingEnd();
                if (response != null || exception != null) {
                  submitResponse(restRequest, restResponseChannel, response, exception);
                }
              }
            }
          });
        } else if (exception == null) {
          throw new IllegalStateException("Both response and exception are null");
        }
      } catch (Exception e) {
        frontendMetrics.headForGetCallbackProcessingError.inc();
        if (exception != null) {
          logger.error("Error while processing callback", e);
        } else {
          exception = e;
        }
      } finally {
        callbackTracker.markCallbackProcessingEnd();
        if (exception != null) {
          submitResponse(restRequest, restResponseChannel, null, exception);
        }
      }
    }

    /**
     * Marks the start time of the operation.
     */
    protected void markStartTime() {
      callbackTracker.markOperationStart();
    }

    /**
     * Determines if user metadata should be sent as content by looking for any keys that are prefixed with
     * {@link RestUtils.Headers#USER_META_DATA_OLD_STYLE_PREFIX}.
     * @param userMetadata the user metadata that was constructed from the byte stream.
     * @return {@code true} if any key is prefixed with {@link RestUtils.Headers#USER_META_DATA_OLD_STYLE_PREFIX}.
     *         {@code false} otherwise.
     */
    private boolean shouldSendMetadataAsContent(Map<String, String> userMetadata) {
      boolean shouldSendAsContent = false;
      for (Map.Entry<String, String> entry : userMetadata.entrySet()) {
        if (entry.getKey().startsWith(RestUtils.Headers.USER_META_DATA_OLD_STYLE_PREFIX)) {
          shouldSendAsContent = true;
          break;
        }
      }
      return shouldSendAsContent;
    }

    /**
     * Sets the user metadata in the headers of the response.
     * @param userMetadata the user metadata that need to be set in the headers.
     * @param restResponseChannel the {@link RestResponseChannel} that is used for sending the response.
     * @throws RestServiceException if there are any problems setting the header.
     */
    private void setUserMetadataHeaders(Map<String, String> userMetadata, RestResponseChannel restResponseChannel)
        throws RestServiceException {
      for (Map.Entry<String, String> entry : userMetadata.entrySet()) {
        restResponseChannel.setHeader(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Callback for GET operations. Submits the response received to an instance of {@link RestResponseHandler}.
   */
  private class GetCallback implements Callback<ReadableStreamChannel> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final CallbackTracker callbackTracker;

    /**
     * Create a GET callback.
     * @param restRequest the {@link RestRequest} for whose response this is a callback.
     * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be
     *                            sent.
     */
    public GetCallback(RestRequest restRequest, RestResponseChannel restResponseChannel) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      callbackTracker = new CallbackTracker(restRequest, "GET", frontendMetrics.getTimeInMs,
          frontendMetrics.getCallbackProcessingTimeInMs);
      callbackTracker.markOperationStart();
    }

    /**
     * Submits the GET response to {@link RestResponseHandler} so that it can be sent (or the exception handled).
     * @param result The result of the request. This is the actual blob data as a {@link ReadableStreamChannel}.
     *               This is non null if the request executed successfully.
     * @param exception The exception that was reported on execution of the request (if any).
     */
    @Override
    public void onCompletion(ReadableStreamChannel result, Exception exception) {
      try {
        callbackTracker.markOperationEnd();
        String blobId = RestUtils.getOperationOrBlobIdFromUri(restRequest);
        if (exception == null && result == null) {
          throw new IllegalStateException("Both response and exception are null for GetCallback of " + blobId);
        }
      } catch (Exception e) {
        frontendMetrics.getCallbackProcessingError.inc();
        if (exception != null) {
          logger.error("Error while processing callback", e);
        } else {
          exception = e;
        }
      } finally {
        callbackTracker.markCallbackProcessingEnd();
        submitResponse(restRequest, restResponseChannel, result, exception);
      }
    }
  }

  /**
   * Callback for POST operations. Sends the response received to the client. Submits response either to handle
   * exceptions or to clean up after a response.
   */
  private class PostCallback implements Callback<String> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final BlobProperties blobProperties;
    private final CallbackTracker callbackTracker;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Create a POST callback.
     * @param restRequest the {@link RestRequest} for whose response this is a callback.
     * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be
     *                            sent.
     * @param createdBlobProperties the {@link BlobProperties} of the blob that was asked to be POSTed.
     */
    public PostCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        BlobProperties createdBlobProperties) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.blobProperties = createdBlobProperties;
      callbackTracker = new CallbackTracker(restRequest, "POST", frontendMetrics.postTimeInMs,
          frontendMetrics.postCallbackProcessingTimeInMs);
    }

    /**
     * If there was no exception, updates the header with the location of the object. Submits the response either for
     * exception handling or for cleanup.
     * @param result The result of the request. This is the blob ID of the blob. This is non null if the request
     *               executed successfully.
     * @param exception The exception that was reported on execution of the request (if any).
     */
    @Override
    public void onCompletion(String result, Exception exception) {
      try {
        callbackTracker.markOperationEnd();
        if (exception == null && result != null) {
          logger.trace("Successful POST of {}", result);
          final CallbackTracker idConversionCallbackTracker =
              new CallbackTracker(restRequest, "Outbound Id Conversion", frontendMetrics.outboundIdConversionTimeInMs,
                  frontendMetrics.outboundIdConversionCallbackProcessingTimeInMs);
          idConversionCallbackTracker.markOperationStart();
          idConverter.convert(restRequest, result, new Callback<String>() {
            @Override
            public void onCompletion(String result, Exception exception) {
              idConversionCallbackTracker.markOperationEnd();
              if (exception == null) {
                try {
                  setResponseHeaders(result);
                } catch (RestServiceException e) {
                  frontendMetrics.outboundIdConversionCallbackProcessingError.inc();
                  exception = e;
                }
              }
              idConversionCallbackTracker.markCallbackProcessingEnd();
              submitResponse(restRequest, restResponseChannel, null, exception);
            }
          });
        } else if (exception == null) {
          throw new IllegalStateException("Both response and exception are null for PostCallback");
        }
      } catch (Exception e) {
        frontendMetrics.postCallbackProcessingError.inc();
        if (exception != null) {
          logger.error("Error while processing callback", e);
        } else {
          exception = e;
        }
      } finally {
        callbackTracker.markCallbackProcessingEnd();
        if (exception != null) {
          submitResponse(restRequest, restResponseChannel, null, exception);
        }
      }
    }

    /**
     * Marks the start time of the operation.
     */
    protected void markStartTime() {
      callbackTracker.markOperationStart();
    }

    /**
     * Sets the required headers in the response.
     * @param location the location of the created resource.
     * @throws RestServiceException if there was any problem setting the headers.
     */
    private void setResponseHeaders(String location)
        throws RestServiceException {
      restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
      restResponseChannel.setStatus(ResponseStatus.Created);
      restResponseChannel.setHeader(RestUtils.Headers.LOCATION, location);
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
      restResponseChannel.setHeader(RestUtils.Headers.CREATION_TIME, new Date(blobProperties.getCreationTimeInMs()));
    }
  }

  /**
   * Callback for DELETE operations. Sends an ACCEPTED response to the client if operation is successful. Submits
   * response either to handle exceptions or to clean up after a response.
   */
  private class DeleteCallback implements Callback<Void> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final CallbackTracker callbackTracker;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Create a DELETE callback.
     * @param restRequest the {@link RestRequest} for whose response this is a callback.
     * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be
     *                            sent.
     */
    public DeleteCallback(RestRequest restRequest, RestResponseChannel restResponseChannel) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      callbackTracker = new CallbackTracker(restRequest, "DELETE", frontendMetrics.deleteTimeInMs,
          frontendMetrics.deleteCallbackProcessingTimeInMs);
    }

    /**
     * If there was no exception, updates the header with the acceptance of the request. Submits the response either for
     * exception handling or for cleanup.
     * @param result The result of the request. This is always null.
     * @param exception The exception that was reported on execution of the request (if any).
     */
    @Override
    public void onCompletion(Void result, Exception exception) {
      try {
        callbackTracker.markOperationEnd();
        restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        if (exception == null) {
          restResponseChannel.setStatus(ResponseStatus.Accepted);
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
        }
      } catch (RestServiceException e) {
        frontendMetrics.deleteCallbackProcessingError.inc();
        if (exception != null) {
          logger.error("Error while processing callback", e);
        } else {
          exception = e;
        }
      } finally {
        callbackTracker.markCallbackProcessingEnd();
        submitResponse(restRequest, restResponseChannel, null, exception);
      }
    }

    /**
     * Marks the start time of the operation.
     */
    protected void markStartTime() {
      callbackTracker.markOperationStart();
    }
  }

  /**
   * Callback for HEAD operations. Sends the headers to the client if operation is successful. Submits response either
   * to handle exceptions or to clean up after a response.
   */
  private class HeadCallback implements Callback<BlobInfo> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final CallbackTracker callbackTracker;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Create a HEAD callback.
     * @param restRequest the {@link RestRequest} for whose response this is a callback.
     * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be
     *                            sent.
     */
    public HeadCallback(RestRequest restRequest, RestResponseChannel restResponseChannel) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      callbackTracker = new CallbackTracker(restRequest, "HEAD", frontendMetrics.headTimeInMs,
          frontendMetrics.headCallbackProcessingTimeInMs);
    }

    /**
     * If there was no exception, updates the header with the properties. Exceptions, if any, will be handled upon
     * submission.
     * @param result The result of the request i.e a {@link BlobInfo} object with the properties of the blob. This is
     *               non null if the request executed successfully.
     * @param exception The exception that was reported on execution of the request (if any).
     */
    @Override
    public void onCompletion(BlobInfo result, Exception exception) {
      try {
        callbackTracker.markOperationEnd();
        if (exception == null && result != null) {
          final CallbackTracker securityCallbackTracker =
              new CallbackTracker(restRequest, "HEAD Response Security", frontendMetrics.headSecurityResponseTimeInMs,
                  frontendMetrics.headSecurityResponseCallbackProcessingTimeInMs);
          securityCallbackTracker.markOperationStart();
          securityService.processResponse(restRequest, restResponseChannel, result, new Callback<Void>() {
            @Override
            public void onCompletion(Void result, Exception exception) {
              callbackTracker.markOperationEnd();
              callbackTracker.markCallbackProcessingEnd();
              submitResponse(restRequest, restResponseChannel, null, exception);
            }
          });
        } else if (exception == null) {
          throw new IllegalStateException("Both response and exception are null for HeadCallback");
        }
      } catch (Exception e) {
        frontendMetrics.headCallbackProcessingError.inc();
        if (exception != null) {
          logger.error("Error while processing callback", e);
        } else {
          exception = e;
        }
      } finally {
        callbackTracker.markCallbackProcessingEnd();
        if (exception != null) {
          submitResponse(restRequest, restResponseChannel, null, exception);
        }
      }
    }

    /**
     * Marks the start time of the operation.
     */
    protected void markStartTime() {
      callbackTracker.markOperationStart();
    }
  }
}

package com.github.ambry.router;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.Port;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.TTLRequest;
import com.github.ambry.protocol.TTLResponse;
import com.github.ambry.utils.Time;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TTLUpdateOperation {
  //Operation arguments
  private final RouterConfig routerConfig;
  private final BlobId blobId;
  private final String serviceId;
  private final FutureResult<Void> futureResult;
  private final Callback<Void> callback;
  private final Time time;
  private final NonBlockingRouterMetrics routerMetrics;
  private final long submissionTimeMs;
  private final long expiresAtMs;
  private final long operationTimeMs;

  // Parameters associated with the state.

  // The operation tracker that tracks the state of this operation.
  private final OperationTracker operationTracker;
  // A map used to find inflight requests using a correlation id.
  private final HashMap<Integer, TTLRequestInfo> ttlRequestInfos;
  // The result of this operation to be set into FutureResult.
  private final Void operationResult = null;
  // the cause for failure of this operation. This will be set if and when the operation encounters an irrecoverable
  // failure.
  private final AtomicReference<Exception> operationException = new AtomicReference<Exception>();
  // RouterErrorCode that is resolved from all the received ServerErrorCode for this operation.
  private RouterErrorCode resolvedRouterErrorCode;
  // Denotes whether the operation is complete.
  private boolean operationCompleted = false;

  private static final Logger logger = LoggerFactory.getLogger(TTLUpdateOperation.class);

  TTLUpdateOperation(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, ResponseHandler responsehandler,
      BlobId blobId, String serviceId, long expiresAtMs, Callback<Void> callback, Time time,
      FutureResult<Void> futureResult) {
    this.submissionTimeMs = time.milliseconds();
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.blobId = blobId;
    this.serviceId = serviceId;
    this.futureResult = futureResult;
    this.callback = callback;
    this.time = time;
    this.expiresAtMs = expiresAtMs;
    this.operationTimeMs = time.milliseconds();
    this.ttlRequestInfos = new HashMap<Integer, TTLRequestInfo>();
    this.operationTracker = new SimpleOperationTracker(routerConfig.routerDatacenterName, blobId.getPartition(), true,
        routerConfig.routerPutSuccessTarget, routerConfig.routerPutRequestParallelism, false);
  }

  void poll(RequestRegistrationCallback<TTLUpdateOperation> requestRegistrationCallback) {
    cleanupExpiredInflightRequests();
    checkAndMaybeComplete();
    if (!isOperationComplete()) {
      fetchRequests(requestRegistrationCallback);
    }
  }

  private void fetchRequests(RequestRegistrationCallback<TTLUpdateOperation> requestRegistrationCallback) {
    Iterator<ReplicaId> replicaIterator = operationTracker.getReplicaIterator();
    while (replicaIterator.hasNext()) {
      ReplicaId replica = replicaIterator.next();
      String hostname = replica.getDataNodeId().getHostname();
      Port port = replica.getDataNodeId().getPortToConnectTo();
      TTLRequest ttlRequest = createTtlRequest();
      ttlRequestInfos.put(ttlRequest.getCorrelationId(), new TTLRequestInfo(time.milliseconds(), replica));
      RouterRequestInfo requestInfo = new RouterRequestInfo(hostname, port, ttlRequest, replica);
      requestRegistrationCallback.registerRequestToSend(this, requestInfo);
      replicaIterator.remove();
      if (RouterUtils.isRemoteReplica(routerConfig, replica)) {
        logger.info("Making request with correlationId {} to a remote replica {} in {} ", ttlRequest.getCorrelationId(),
            replica.getDataNodeId(), replica.getDataNodeId().getDatacenterName());
        routerMetrics.crossColoRequestCount.inc();
      } else {
        logger.info("Making request with correlationId {} to a local replica {} ", ttlRequest.getCorrelationId(),
            replica.getDataNodeId());
      }
      routerMetrics.getDataNodeBasedMetrics(replica.getDataNodeId()).deleteRequestRate.mark();
    }
  }

  private TTLRequest createTtlRequest() {
    return new TTLRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname,
        blobId, expiresAtMs, operationTimeMs);
  }

  void handleResponse(ResponseInfo responseInfo, TTLResponse ttlResponse) {
    TTLRequest ttlRequest = (TTLRequest) responseInfo.getRequestInfo().getRequest();
    TTLRequestInfo ttlRequestInfo = ttlRequestInfos.remove(ttlRequest.getCorrelationId());
    // ttlRequestInfo can be null if this request was timed out before this response is received. No
    // metric is updated here, as corresponding metrics have been updated when the request was timed out.
    if (ttlRequestInfo == null) {
      return;
    }
    ReplicaId replica = ttlRequestInfo.replica;
    // Check the error code from NetworkClient.
    if (responseInfo.getError() != null) {
      logger.info("TTLRequest with response correlationId {} timed out for replica {} ", ttlRequest.getCorrelationId(),
          replica.getDataNodeId());
      updateOperationState(replica, RouterErrorCode.OperationTimedOut);
    } else {
      if (ttlResponse == null) {
        logger.info(
            "TTLRequest with response correlationId {} received UnexpectedInternalError on response deserialization for replica {} ",
            ttlRequest.getCorrelationId(), replica.getDataNodeId());
        updateOperationState(replica, RouterErrorCode.UnexpectedInternalError);
      } else {
        // The true case below should not really happen. This means a response has been received
        // not for its original request. We will immediately fail this operation.
        if (ttlResponse.getCorrelationId() != ttlRequest.getCorrelationId()) {
          logger.error("The correlation id in the DeleteResponse " + ttlResponse.getCorrelationId()
              + " is not the same as the correlation id in the associated DeleteRequest: "
              + ttlRequest.getCorrelationId());
          routerMetrics.unknownReplicaResponseError.inc();
          setOperationException(
              new RouterException("Received wrong response that is not for the corresponding request.",
                  RouterErrorCode.UnexpectedInternalError));
          updateOperationState(replica, RouterErrorCode.UnexpectedInternalError);
        } else {
          // The status of operation tracker will be updated within the processServerError method.
          processServerError(replica, ttlResponse.getError(), ttlResponse.getCorrelationId());
        }
      }
    }
    checkAndMaybeComplete();
  }

  /**
   * A wrapper class that is used to check if a request has been expired.
   */
  private class TTLRequestInfo {
    final long startTimeMs;
    final ReplicaId replica;

    TTLRequestInfo(long submissionTime, ReplicaId replica) {
      this.startTimeMs = submissionTime;
      this.replica = replica;
    }
  }

  /**
   * Goes through the inflight request list of this {@code DeleteOperation} and remove those that
   * have been timed out.
   */
  private void cleanupExpiredInflightRequests() {
    Iterator<Map.Entry<Integer, TTLRequestInfo>> itr = ttlRequestInfos.entrySet().iterator();
    while (itr.hasNext()) {
      Map.Entry<Integer, TTLRequestInfo> ttlRequestInfoEntry = itr.next();
      TTLRequestInfo ttlRequestInfo = ttlRequestInfoEntry.getValue();
      if (time.milliseconds() - ttlRequestInfo.startTimeMs > routerConfig.routerRequestTimeoutMs) {
        itr.remove();
        logger.info("TTL Request with correlationid {} in flight has expired for replica {} ",
            ttlRequestInfoEntry.getKey(), ttlRequestInfo.replica.getDataNodeId());
        // Do not notify this as a failure to the response handler, as this timeout could simply be due to
        // connection unavailability. If there is indeed a network error, the NetworkClient will provide an error
        // response and the response handler will be notified accordingly.
        updateOperationState(ttlRequestInfo.replica, RouterErrorCode.OperationTimedOut);
      }
    }
  }

  /**
   * Processes {@link ServerErrorCode} received from {@code replica}. This method maps a {@link ServerErrorCode}
   * to a {@link RouterErrorCode}, and then makes corresponding state update.
   * @param replica The replica for which the ServerErrorCode was generated.
   * @param serverErrorCode The ServerErrorCode received from the replica.
   * @param correlationId the correlationId of the request
   */
  private void processServerError(ReplicaId replica, ServerErrorCode serverErrorCode, int correlationId) {
    switch (serverErrorCode) {
      case No_Error:
        operationTracker.onResponse(replica, true);
        break;
      case Blob_Deleted:
        updateOperationState(replica, RouterErrorCode.BlobDeleted);
        break;
      case Blob_Expired:
        updateOperationState(replica, RouterErrorCode.BlobExpired);
        break;
      case Blob_Not_Found:
        updateOperationState(replica, RouterErrorCode.BlobDoesNotExist);
        break;
      case Partition_Unknown:
        updateOperationState(replica, RouterErrorCode.UnexpectedInternalError);
        break;
      case Disk_Unavailable:
        updateOperationState(replica, RouterErrorCode.AmbryUnavailable);
        break;
      default:
        updateOperationState(replica, RouterErrorCode.UnexpectedInternalError);
        break;
    }
    if (serverErrorCode != ServerErrorCode.No_Error) {
      logger.info("Replica {} returned an error {} for a delete request with response correlationId : {} ",
          replica.getDataNodeId(), serverErrorCode, correlationId);
    }
  }

  private void updateOperationState(ReplicaId replica, RouterErrorCode error) {
    if (resolvedRouterErrorCode == null) {
      resolvedRouterErrorCode = error;
    } else {
      if (getPrecedenceLevel(error) < getPrecedenceLevel(resolvedRouterErrorCode)) {
        resolvedRouterErrorCode = error;
      }
    }
    operationTracker.onResponse(replica, false);
    if (error != RouterErrorCode.BlobDeleted && error != RouterErrorCode.BlobExpired) {
      routerMetrics.routerRequestErrorCount.inc();
    }
    routerMetrics.getDataNodeBasedMetrics(replica.getDataNodeId()).deleteRequestErrorCount.inc();
  }

  private void checkAndMaybeComplete() {
    if (operationTracker.isDone()) {
      if (!operationTracker.hasSucceeded()) {
        setOperationException(
            new RouterException("The DeleteOperation could not be completed.", resolvedRouterErrorCode));
      }
      operationCompleted = true;
    }
  }

  /**
   * Gets the precedence level for a {@link RouterErrorCode}. A precedence level is a relative priority assigned
   * to a {@link RouterErrorCode}. If a {@link RouterErrorCode} has not been assigned a precedence level, a
   * {@code Integer.MIN_VALUE} will be returned.
   * @param routerErrorCode The {@link RouterErrorCode} for which to get its precedence level.
   * @return The precedence level of the {@link RouterErrorCode}.
   */
  private Integer getPrecedenceLevel(RouterErrorCode routerErrorCode) {
    switch (routerErrorCode) {
      case BlobDeleted:
        return 0;
      case BlobExpired:
        return 1;
      case AmbryUnavailable:
        return 2;
      case UnexpectedInternalError:
        return 3;
      case OperationTimedOut:
        return 4;
      case BlobDoesNotExist:
        return 5;
      default:
        return Integer.MIN_VALUE;
    }
  }

  /**
   * Returns whether the operation has completed.
   * @return whether the operation has completed.
   */
  boolean isOperationComplete() {
    return operationCompleted;
  }

  /**
   * Gets {@link BlobId} of this {@code DeleteOperation}.
   * @return The {@link BlobId}.
   */
  BlobId getBlobId() {
    return blobId;
  }

  /**
   * @return the service ID for the service requesting this delete operation.
   */
  String getServiceId() {
    return serviceId;
  }

  /**
   * Get the {@link FutureResult} for this {@code DeleteOperation}.
   * @return The {@link FutureResult}.
   */
  FutureResult<Void> getFutureResult() {
    return futureResult;
  }

  /**
   * Gets the {@link Callback} for this {@code DeleteOperation}.
   * @return The {@link Callback}.
   */
  Callback<Void> getCallback() {
    return callback;
  }

  /**
   * Gets the exception associated with this operation if it failed; null otherwise.
   * @return exception associated with this operation if it failed; null otherwise.
   */
  Exception getOperationException() {
    return operationException.get();
  }

  /**
   * Gets the result for this {@code DeleteOperation}. In a {@link DeleteOperation}, nothing is returned
   * to the caller as a result of this operation. Including this {@link Void} result is for consistency
   * with other operations.
   * @return Void.
   */
  Void getOperationResult() {
    return operationResult;
  }

  /**
   * Sets the exception associated with this operation. When this is called, the operation has failed.
   * @param exception the irrecoverable exception associated with this operation.
   */
  void setOperationException(Exception exception) {
    operationException.set(exception);
  }

  long getSubmissionTimeMs() {
    return submissionTimeMs;
  }

  long getExpiresAtMs() {
    return expiresAtMs;
  }
}

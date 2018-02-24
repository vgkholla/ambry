package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.TTLRequest;
import com.github.ambry.protocol.TTLResponse;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Time;
import java.io.DataInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TTLUpdateManager {
  private final Set<TTLUpdateOperation> ttlUpdateOperations;
  private final HashMap<Integer, TTLUpdateOperation> correlationIdToTtlUpdateOperation;
  private final NotificationSystem notificationSystem;
  private final Time time;
  private final ResponseHandler responseHandler;
  private final NonBlockingRouterMetrics routerMetrics;
  private final RouterConfig routerConfig;

  private static final Logger logger = LoggerFactory.getLogger(TTLUpdateManager.class);

  private class TTlRequestRegistrationCallbackImpl implements RequestRegistrationCallback<TTLUpdateOperation> {
    private List<RequestInfo> requestListToFill;

    @Override
    public void registerRequestToSend(TTLUpdateOperation ttlUpdateOperation, RequestInfo requestInfo) {
      requestListToFill.add(requestInfo);
      correlationIdToTtlUpdateOperation.put(((RequestOrResponse) requestInfo.getRequest()).getCorrelationId(),
          ttlUpdateOperation);
    }
  }

  private final TTlRequestRegistrationCallbackImpl requestRegistrationCallback =
      new TTlRequestRegistrationCallbackImpl();

  TTLUpdateManager(ClusterMap clusterMap, ResponseHandler responseHandler, NotificationSystem notificationSystem,
      RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, RouterCallback routerCallback, Time time) {
    this.responseHandler = responseHandler;
    this.notificationSystem = notificationSystem;
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.time = time;
    ttlUpdateOperations = Collections.newSetFromMap(new ConcurrentHashMap<TTLUpdateOperation, Boolean>());
    correlationIdToTtlUpdateOperation = new HashMap<Integer, TTLUpdateOperation>();
  }

  void submitTtlUpdateOperation(BlobId blobId, String serviceId, long expiresAtMs, FutureResult<Void> futureResult,
      Callback<Void> callback) {
    TTLUpdateOperation ttlUpdateOperation =
        new TTLUpdateOperation(routerConfig, routerMetrics, responseHandler, blobId, serviceId, expiresAtMs, callback,
            time, futureResult);
    ttlUpdateOperations.add(ttlUpdateOperation);
  }

  void poll(List<RequestInfo> requestListToFill) {
    requestRegistrationCallback.requestListToFill = requestListToFill;
    for (TTLUpdateOperation op : ttlUpdateOperations) {
      boolean exceptionEncountered = false;
      try {
        op.poll(requestRegistrationCallback);
      } catch (Exception e) {
        exceptionEncountered = true;
        op.setOperationException(new RouterException("TTL poll encountered unexpected error", e,
            RouterErrorCode.UnexpectedInternalError));
      }
      if (exceptionEncountered || op.isOperationComplete()) {
        if (ttlUpdateOperations.remove(op)) {
          // In order to ensure that an operation is completed only once, call onComplete() only at the place where the
          // operation actually gets removed from the set of operations. See comment within close().
          onComplete(op);
        }
      }
    }
  }

  void handleResponse(ResponseInfo responseInfo) {
    TTLResponse ttlResponse = extractTTLResponseAndNotifyResponseHandler(responseInfo);
    RouterRequestInfo routerRequestInfo = (RouterRequestInfo) responseInfo.getRequestInfo();
    int correlationId = ((TTLRequest) routerRequestInfo.getRequest()).getCorrelationId();
    TTLUpdateOperation ttlUpdateOperation = correlationIdToTtlUpdateOperation.remove(correlationId);
    // If it is still an active operation, hand over the response. Otherwise, ignore.
    if (ttlUpdateOperations.contains(ttlUpdateOperation)) {
      boolean exceptionEncountered = false;
      try {
        ttlUpdateOperation.handleResponse(responseInfo, ttlResponse);
      } catch (Exception e) {
        exceptionEncountered = true;
        ttlUpdateOperation.setOperationException(
            new RouterException("TTLUpdate handleResponse encountered unexpected error", e,
                RouterErrorCode.UnexpectedInternalError));
      }
      if (exceptionEncountered || ttlUpdateOperation.isOperationComplete()) {
        if (ttlUpdateOperations.remove(ttlUpdateOperation)) {
          onComplete(ttlUpdateOperation);
        }
      }
    } else {
      routerMetrics.ignoredResponseCount.inc();
    }
  }

  private TTLResponse extractTTLResponseAndNotifyResponseHandler(ResponseInfo responseInfo) {
    TTLResponse ttlResponse = null;
    ReplicaId replicaId = ((RouterRequestInfo) responseInfo.getRequestInfo()).getReplicaId();
    NetworkClientErrorCode networkClientErrorCode = responseInfo.getError();
    if (networkClientErrorCode == null) {
      try {
        ttlResponse =
            TTLResponse.readFrom(new DataInputStream(new ByteBufferInputStream(responseInfo.getResponse())));
        responseHandler.onEvent(replicaId, ttlResponse.getError());
      } catch (Exception e) {
        // Ignore. There is no value in notifying the response handler.
        logger.error("Response deserialization received unexpected error", e);
        routerMetrics.responseDeserializationErrorCount.inc();
      }
    } else {
      responseHandler.onEvent(replicaId, networkClientErrorCode);
    }
    return ttlResponse;
  }

  private void onComplete(TTLUpdateOperation op) {
    NonBlockingRouter.completeOperation(op.getFutureResult(), op.getCallback(), op.getOperationResult(),
        op.getOperationException());
  }

  void close() {
    for (TTLUpdateOperation op : ttlUpdateOperations) {
      // There is a rare scenario where the operation gets removed from this set and gets completed concurrently by
      // the RequestResponseHandler thread when it is in poll() or handleResponse(). In order to avoid the completion
      // from happening twice, complete it here only if the remove was successful.
      if (ttlUpdateOperations.remove(op)) {
        Exception e = new RouterException("Aborted operation because Router is closed.", RouterErrorCode.RouterClosed);
        routerMetrics.operationDequeuingRate.mark();
        routerMetrics.operationAbortCount.inc();
        NonBlockingRouter.completeOperation(op.getFutureResult(), op.getCallback(), null, e);
      }
    }
  }
}

package com.github.ambry.admin;

import com.github.ambry.rest.RestErrorCode;
import com.github.ambry.rest.RestException;
import com.github.ambry.rest.RestMessageHandler;
import com.github.ambry.rest.RestRequestDelegator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  Admin specific implementation of RestRequestDelgator.
 *
 *  When instantiated, we create a configurable number of message handlers.
 *  The rest server requests to use a handler and we choose one out of the pool of handlers
 *  and return it.
 *  Multiple requests can share the same handler i.e. just because we returned a handler does not mean
 *  that it is fully occupied. Our job is to just return a handler (any one) when asked for. The handler will
 *  take care of serving all the requests that it has been assigned to. Having said this, we would like to
 *  balance load among all the handlers.
 */
public class AdminRequestDelegator implements RestRequestDelegator {

  private final AdminMetrics adminMetrics;
  private final ExecutorService executor;
  private final int handlerCount;
  private final List<AdminMessageHandler> adminMessageHandlers;

  private int currIndex = 0;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public AdminRequestDelegator(int handlerCount, AdminMetrics adminMetrics) {
    this.handlerCount = handlerCount;
    this.adminMetrics = adminMetrics;
    adminMessageHandlers = new ArrayList<AdminMessageHandler>(handlerCount);
    executor = Executors.newFixedThreadPool(handlerCount);
  }

  public void start()
      throws Exception {
    for (int i = 0; i < handlerCount; i++) {
      AdminMessageHandler messageHandler = new AdminMessageHandler(adminMetrics);
      executor.execute(messageHandler);
      adminMessageHandlers.add(messageHandler);
    }
  }

  public RestMessageHandler getMessageHandler()
      throws Exception {
    if (adminMessageHandlers.size() == 0) {
      adminMetrics.noMessageHandlersErrorCount.inc();
      throw new RestException("No message handlers available", RestErrorCode.NoMessageHandlers);
    }

    try {
      //Alternative: can have an implementation where we check queue sizes and then return the one with the least
      /*  Not locking here because we don't really care if currIndex fails to increment (because of stamping).
          As long as it does not go out of bounds, we cool.
          So my train of thought is that locking unnecessarily slows things down here.
       */
      AdminMessageHandler messageHandler = adminMessageHandlers.get(currIndex);
      currIndex = (currIndex + 1) % adminMessageHandlers.size();
      return messageHandler;
    } catch (Exception e) {
      logger.error("Error while trying to pick a handler to return - " + e);
      adminMetrics.handlerSelectionErrorCount.inc();
      throw new RestException("Error while trying to pick a handler to return", RestErrorCode.HandlerSelectionError);
    }
  }

  public void shutdown()
      throws Exception {
    for (int i = 0; i < adminMessageHandlers.size(); i++) {
      adminMessageHandlers.get(i).shutdownGracefully();
    }
    executor.shutdown();
  }
}
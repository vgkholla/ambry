package com.github.ambry.rest;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * Netty client to send requests and receive responses in tests.
 */
public class NettyClient {
  private final EventLoopGroup group = new NioEventLoopGroup();

  private final String hostname;
  private final int port;
  private final LinkedBlockingQueue<HttpObject> responseParts;
  private final CommunicationHandler communicationHandler;

  /**
   * Create a NettyClient by providing it a queue of request parts.
   * @param hostname the host to connect to.
   * @param port the port to connect to.
   * @param request the request as a {@link FullHttpRequest}.
   */
  public NettyClient(String hostname, int port, FullHttpRequest request) {
    this.hostname = hostname;
    this.port = port;
    this.responseParts = new LinkedBlockingQueue<HttpObject>();
    communicationHandler = new CommunicationHandler(request, responseParts);
  }

  /**
   * Sends the request (and all its parts) to the server.
   * <p/>
   * The connection is established synchronously but the request sending is asynchronous.
   * <p/>
   * Be sure to decrease the reference counts of each of the response parts via a call to
   * {@link ReferenceCountUtil#release(Object)} once the part has been processed. Neglecting to do so might result in
   * OOM.
   * <p/>
   * Always check for exceptions via {@link #getException()} once {@link io.netty.handler.codec.http.LastHttpContent} is
   * received.
   * @return queue through which response will be returned in parts. Starts with a
   *         {@link io.netty.handler.codec.http.HttpResponse}, may contain any number of
   *         {@link io.netty.handler.codec.http.HttpContent} and will end with a
   *         {@link io.netty.handler.codec.http.LastHttpContent}.
   * @throws InterruptedException
   */
  public LinkedBlockingQueue<HttpObject> sendRequest()
      throws InterruptedException {
    Bootstrap b = new Bootstrap();
    b.group(group).channel(NioSocketChannel.class).option(ChannelOption.SO_KEEPALIVE, false)
        .option(ChannelOption.TCP_NODELAY, false).handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch)
          throws Exception {
        ch.pipeline().addLast(new HttpClientCodec()).addLast(communicationHandler);
      }
    });
    ChannelFuture f = b.connect(hostname, port).sync();
    f.sync();
    return responseParts;
  }

  /**
   * Shuts down the netty client.
   * @throws InterruptedException
   */
  public void shutdown()
      throws InterruptedException {
    if (!group.isTerminated()) {
      group.shutdownGracefully();
      if (!group.awaitTermination(30, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Client did not shutdown within timeout");
      }
    }
  }

  /**
   * Waits until the full response arrives.
   * <p/>
   * If there was any exception during the request/response processing, it is thrown.
   * @param timeout the amount of time to wait for the full response.
   * @param timeUnit time unit of {@code timeout}.
   * @return {@code true} if the full response was received within the {@code timeout}. {@code false} otherwise.
   * @throws Exception if the wait for the full response is interrupted or if there was an exception during
   *                   request/response processing.
   */
  public boolean awaitFullResponse(long timeout, TimeUnit timeUnit)
      throws Exception {
    return communicationHandler.awaitFullResponse(timeout, timeUnit);
  }

  /**
   * Gets exception that occurred during request/response transmission if any.
   * @return exception that occurred during request/response transmission if any.
   */
  public Exception getException() {
    return communicationHandler.getException();
  }
}

/**
 * Custom handler that sends out the request and receives response.
 */
class CommunicationHandler extends SimpleChannelInboundHandler<Object> {
  private final FullHttpRequest request;
  private final LinkedBlockingQueue<HttpObject> responseParts;
  private final CountDownLatch responseComplete = new CountDownLatch(1);

  private volatile Exception exception = null;

  /**
   * Creates a handler that sends the request and its parts and recieves the response and stores them in
   * {@code responseParts}.
   * @param request the request as a {@link FullHttpRequest}.
   * @param responseParts queue through which response will be returned in parts. Starts with a
   *                      {@link io.netty.handler.codec.http.HttpResponse}, may contain any number of
   *                      {@link io.netty.handler.codec.http.HttpContent} and will end with a
   *                      {@link io.netty.handler.codec.http.LastHttpContent}.
   */
  public CommunicationHandler(FullHttpRequest request, LinkedBlockingQueue<HttpObject> responseParts) {
    this.request = request;
    this.responseParts = responseParts;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx)
      throws Exception {
    ctx.writeAndFlush(request);
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, Object in)
      throws Exception {
    if (in instanceof HttpObject) {
      // Make sure that we increase refCnt because we are going to process it async. The other end has to release after
      // processing.
      ReferenceCountUtil.retain(in);
      responseParts.offer((HttpObject) in);
      if (in instanceof LastHttpContent) {
        responseComplete.countDown();
      }
    } else {
      throw new IllegalStateException("Read object is not a HTTPObject");
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    ctx.close();
    responseComplete.countDown();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (cause instanceof Exception) {
      exception = (Exception) cause;
      responseParts.offer(new DefaultLastHttpContent());
      responseComplete.countDown();
    } else {
      ctx.fireExceptionCaught(cause);
    }
  }

  /**
   * Waits until the full response arrives.
   * <p/>
   * If there was any exception during the request/response processing, it is thrown.
   * @param timeout the amount of time to wait for the full response.
   * @param timeUnit time unit of {@code timeout}.
   * @return {@code true} if the full response was received within the {@code timeout}. {@code false} otherwise.
   * @throws Exception if the wait for the full response is interrupted or if there was an exception during
   *                   request/response processing.
   */
  public boolean awaitFullResponse(long timeout, TimeUnit timeUnit)
      throws Exception {
    boolean success = responseComplete.await(timeout, timeUnit);
    if (exception != null) {
      throw exception;
    }
    return success;
  }

  /**
   * Gets exception that occurred during request/response transmission if any.
   * @return exception that occurred during request/response transmission if any.
   */
  protected Exception getException() {
    return exception;
  }
}

package com.github.ambry.protocol;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


public class TTLRequest extends RequestOrResponse {

  private final BlobId blobId;
  private final long expiresAtMs;
  private final long operationTimeInMs;
  private final short version;
  static final short TTL_REQUEST_VERSION_1 = 1;
  private final static short CURRENT_VERSION = TTL_REQUEST_VERSION_1;

  private int sizeSent;

  public TTLRequest(int correlationId, String clientId, BlobId blobId, long expiresAtMs, long deletionTimeInMs) {
    this(correlationId, clientId, blobId, expiresAtMs, deletionTimeInMs, CURRENT_VERSION);
  }

  protected TTLRequest(int correlationId, String clientId, BlobId blobId, long expiresAtMs, long deletionTimeInMs,
      short version) {
    super(RequestOrResponseType.TTLRequest, version, correlationId, clientId);
    this.version = version;
    this.blobId = blobId;
    this.expiresAtMs = expiresAtMs;
    this.operationTimeInMs = deletionTimeInMs;
    sizeSent = 0;
  }

  public static TTLRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    Short version = stream.readShort();
    switch (version) {
      case TTL_REQUEST_VERSION_1:
        return TTLRequest_V1.readFrom(stream, map);
      default:
        throw new IllegalStateException("Unknown TTL Request version " + version);
    }
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0;
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.put(blobId.toBytes());
      bufferToSend.putLong(expiresAtMs);
      bufferToSend.putLong(operationTimeInMs);
      bufferToSend.flip();
    }
    if (bufferToSend.remaining() > 0) {
      written = channel.write(bufferToSend);
      sizeSent += written;
    }
    return written;
  }

  @Override
  public boolean isSendComplete() {
    return sizeSent == sizeInBytes();
  }

  public BlobId getBlobId() {
    return blobId;
  }

  public short getAccountId() {
    return blobId.getAccountId();
  }

  public short getContainerId() {
    return blobId.getContainerId();
  }

  public long getExpiresAtMs() {
    return expiresAtMs;
  }

  public long getOperationTimeInMs() {
    return operationTimeInMs;
  }

  @Override
  public long sizeInBytes() {
    // header + blobId
    // expires time
    // op time
    return super.sizeInBytes() + blobId.sizeInBytes() + 2 * Long.BYTES;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("TTLRequest[");
    sb.append("BlobID=").append(blobId);
    sb.append(", ").append("PartitionId=").append(blobId.getPartition());
    sb.append(", ").append("ClientId=").append(clientId);
    sb.append(", ").append("CorrelationId=").append(correlationId);
    sb.append(", ").append("AccountId=").append(blobId.getAccountId());
    sb.append(", ").append("ContainerId=").append(blobId.getContainerId());
    sb.append(", ").append("ExpiresAtMs=").append(expiresAtMs);
    sb.append(", ").append("OperationTimeMs=").append(operationTimeInMs);
    sb.append("]");
    return sb.toString();
  }

  /**
   * Class to read protocol version 1 DeleteRequest from the stream.
   */
  private static class TTLRequest_V1 {
    static TTLRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      long expiresAtMs = stream.readLong();
      long operationTimeMs = stream.readLong();
      return new TTLRequest(correlationId, clientId, id, expiresAtMs, operationTimeMs, TTL_REQUEST_VERSION_1);
    }
  }
}

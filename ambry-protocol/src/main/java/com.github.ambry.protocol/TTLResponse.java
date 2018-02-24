package com.github.ambry.protocol;

import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;


public class TTLResponse extends Response {
  private static final short TTL_Response_Version_V1 = 1;

  public TTLResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.TTLResponse, TTL_Response_Version_V1, correlationId, clientId, error);
  }

  public static TTLResponse readFrom(DataInputStream stream) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.TTLResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    // ignore version for now
    return new TTLResponse(correlationId, clientId, error);
  }

  @Override
  public String toString() {
    return "TTLResponse[" + "ServerErrorCode=" + getError() + "]";
  }
}

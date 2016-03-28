package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.SecurityService;
import com.github.ambry.rest.SecurityServiceFactory;


/**
 * Default implementation of {@link SecurityServiceFactory} for Ambry
 * <p/>
 * Returns a new instance of {@link AmbrySecurityService} on {@link #getSecurityService()} call.
 */
public class AmbrySecurityServiceFactory implements SecurityServiceFactory {
  private final VerifiableProperties verifiableProperties;

  public AmbrySecurityServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
    if (verifiableProperties == null) {
      throw new IllegalArgumentException("VerfiableProperties is null");
    }
    this.verifiableProperties = verifiableProperties;
  }

  public SecurityService getSecurityService() {
    FrontendConfig frontendConfig = new FrontendConfig(verifiableProperties);
    return new AmbrySecurityService(frontendConfig);
  }
}

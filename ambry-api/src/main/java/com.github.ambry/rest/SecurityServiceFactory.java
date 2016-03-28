package com.github.ambry.rest;

/**
 *  SecurityServiceFactory is a factory to generate all the supporting cast required to instantiate a
 * {@link SecurityService}.
 * <p/>
 * Usually called with the canonical class name and as such might have to support appropriate (multiple) constructors.
 */
public interface SecurityServiceFactory {

  /**
   * Returns an instance of the {@link SecurityService} that the factory generates.
   * @return an instance of {@link SecurityService} generated by this factory.
   * @throws InstantiationException if the {@link SecurityService} instance cannot be created.
   */
  public SecurityService getSecurityService()
      throws InstantiationException;
}

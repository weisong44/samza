package org.apache.samza.example;

import org.apache.samza.config.Config;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.CoordinationUtilsFactory;


public class PassthroughCoordinationUtilsFactory implements CoordinationUtilsFactory {
  @Override
  public CoordinationUtils getCoordinationUtils(String groupId, String participantId, Config updatedConfig) {
    return null;
  }
}
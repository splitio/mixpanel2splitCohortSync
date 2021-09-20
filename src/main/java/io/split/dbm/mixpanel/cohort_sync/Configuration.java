package io.split.dbm.mixpanel.cohort_sync;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.gson.Gson;

public class Configuration {

	public int connectTimeoutInSeconds;
	public int readTimeoutInSeconds;
	public String environmentId;
	public String workspaceId;
	public String trafficTypeId;
	public int projectId;
	public String mixpanelAuthorization;
	public String splitAuthorization;
	public int segmentLimit;
	public String[] includedCohorts;
	
    public static Configuration fromFile(String configFilePath) throws IOException {
        String configContents = Files.readString(Paths.get(configFilePath));
        return new Gson().fromJson(configContents, Configuration.class);
    }

}

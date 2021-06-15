# MixPanel to Split Cohort Synchronization

To run, build the executable JAR file and run with a JSON configuration as argument.

mvn clean compile assembly:single

Executable JAR takes name of configuration file as argument.

Sample configuration file.
```
{
	"connectTimeoutInSeconds" : 120,
	"readTimeoutInSeconds" : 120,
	"environmentId" : "194da2f0-retrieve-from-split",
	"workspaceId" : "194c1c50-retrieve-from-split",
	"trafficTypeId" : "194c6a70-retrieve-from-split",
	"projectId" : 123456,
	"mixpanelAuthorization" : "Basic M2Y5xxXXXXXXXXXXXXXXXX",
	"splitAuthorization" : "Bearer s89oXXXXXXXXXXXXXXXXX",
	"segmentLimit" : 100
}
```
Configuration Fields:

* "connectTimeoutInSeconds" - how long should the http client wait to connect?
* "readTimeoutInSeconds" - how long should the http client wait to read?
* "environmentId" - unique identifier of a single Split environment.  This is where the MixPanel cohorts will become new segments in Split.
* "workspaceId" - unique workspace identifier for the environment selected
* "trafficTypeId" - what should be the traffic type of the newly created segments?
* "projectId" - retrieve from MixPanel configuration UI
* "mixpanelAuthorization" - for now, the unfriendly Basic Auth string; get from Postman
* "splitAuthorization" - get an admin API token from Split Admin UI and paste it after the Bearer"
* "segmentLimit" - maximum number of segments to synchronize



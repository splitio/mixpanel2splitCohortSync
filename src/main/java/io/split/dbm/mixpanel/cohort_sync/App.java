package io.split.dbm.mixpanel.cohort_sync;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONObject;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class App 
{
	public static Map<String, SplitSegment> splitSegments = new TreeMap<String, SplitSegment>();
	
	public static OkHttpClient client; 
	public static Configuration config;
	
    private static String configFile(String[] args) {
        if(args.length != 1) {
            System.err.println("ERROR - first argument should be configuration file  (e.g. java -jar amplitude2split.jar amplitude2split.config)");
            System.exit(1);
        }
        return args[0];
    }
    
	public static void main( String[] args )
			throws Exception
	{
		long start = System.currentTimeMillis();
		
		System.out.println("" + new Date() + " - MixPanel to Split cohort synchronization begins");
		config = Configuration.fromFile(configFile(args));
        
		client = new OkHttpClient().newBuilder()
				.connectTimeout(config.connectTimeoutInSeconds, TimeUnit.SECONDS)
				.readTimeout(config.connectTimeoutInSeconds, TimeUnit.SECONDS)
				.build();

		gatherListOfSplitSegments(client);

		Map<String, Integer> cohorts = gatherListOfMixPanelCohorts(client);
		System.out.println("syncing " + cohorts.size() + " cohorts...");
		for(Entry<String, Integer> entry : cohorts.entrySet()) {
			syncCohort(client, entry.getKey(), entry.getValue());
		}
		
		System.out.println("..." + cohorts.size() + " cohorts synchronized with Split in " + (System.currentTimeMillis() - start) + "ms");
	}

	private static Map<String, Integer> gatherListOfMixPanelCohorts(OkHttpClient client) throws Exception {
		Request requestCohorts = new Request.Builder()
				.url("https://mixpanel.com/api/2.0/cohorts/list?project_id=" + config.projectId)
				.method("GET", null)
				.addHeader("Authorization", config.mixpanelAuthorization)
				.build();
		Response responseCohorts = client.newCall(requestCohorts).execute();
		checkResponse(responseCohorts, "failed to gather list of cohorts");
		JSONArray cohortArray = new JSONArray(responseCohorts.body().string());
		//System.out.println(cohortArray.toString(2));
		
		Map<String, Integer> cohorts = new TreeMap<String, Integer>();
		
		for(int k = 0; k < cohortArray.length(); k++) {
			JSONObject o = cohortArray.getJSONObject(k);
			cohorts.put(o.getString("name"), o.getInt("id"));
			System.out.println("queuing cohort " + o.getString("name") + "...");
		}
		return cohorts;
	}

	private static void gatherListOfSplitSegments(OkHttpClient client) throws Exception {
		// Gather list of Split segments
		Request requestSegments = new Request.Builder()
				  .url("https://api.split.io/internal/api/v2/segments/ws/" + config.workspaceId +"/environments/" + config.environmentId + "?limit=" + config.segmentLimit + "&offset=0")
				  .method("GET", null)
				  .addHeader("Content-Type", "application/json")
				  .addHeader("Authorization", config.splitAuthorization)
				  .build();
		Response responseSegments = client.newCall(requestSegments).execute();
		checkResponse(responseSegments, "failed to gather list of split segments");
		JSONObject responseObj = new JSONObject(responseSegments.body().string());
		JSONArray segmentsObj = responseObj.getJSONArray("objects");
		for(int m = 0; m < segmentsObj.length(); m++) {
			JSONObject segmentObject = segmentsObj.getJSONObject(m);
			SplitSegment splitSegment = new SplitSegment();
			splitSegment.name = segmentObject.getString("name");
			splitSegment.environmentId = segmentObject.getJSONObject("environment").getString("id");
			splitSegment.trafficTypeId = segmentObject.getJSONObject("trafficType").getString("id");
			splitSegments.put(splitSegment.name, splitSegment);
		}
	}

	private static void syncCohort(OkHttpClient client, String cohortName, int cohortId) throws Exception {
		System.out.println("Syncing cohort " + cohortName + " (" + cohortId + ")");
		
		JSONArray distinct_idList = gatherDistinctIdsForCohort(client, cohortName, cohortId);

		putDistinctIdsInMatchingSplitSegment(client, cohortName, distinct_idList);
	}

	static String convertToSplitPattern(String name) {
		String result = name;
		if(!Character.isAlphabetic(name.charAt(0))) {
			result = "a" + result.substring(1);
		}
		for(int i = 0; i < result.length(); i++) {
			if(!Character.isAlphabetic(result.charAt(i))
					&& !Character.isDigit(result.charAt(i))
					&& result.charAt(i) != '-'
					&& result.charAt(i) != '_') {
				result = result.substring(0, i) + '_' + result.substring(i+1);
			}
		}
		
		return result;
	}
	
	private static void putDistinctIdsInMatchingSplitSegment(OkHttpClient client, String cohortName, JSONArray distinct_idList) throws Exception {
		
		boolean found = false;
		for(Entry<String, SplitSegment> entry : splitSegments.entrySet()) {
			//System.out.println(convertToSplitPattern(cohortName) + " ?= " + entry.getKey());
			if(entry.getKey().equalsIgnoreCase(convertToSplitPattern(cohortName))) {
				found = true;
				break;
			}
		}
		if(!found) {
			createSplitSegmentForCohort(convertToSplitPattern(cohortName));
		}
		
		MediaType mediaTypeJson = MediaType.parse("application/json");
		String ids = distinct_idList.toString();
		if(ids.equals("[]")) {
			// FIXME is this the right behavior?
			ids = "[\"splitizen\"]"; // placeholder so that empty cohorts still create a segment
		}
		System.out.println("IDS: " + ids);
		RequestBody bodySplit = RequestBody.create(mediaTypeJson, ids);
		Request requestSplit = new Request.Builder()
				.url("https://api.split.io/internal/api/v2/segments/" + config.environmentId +"/" + convertToSplitPattern(cohortName) + "/upload")
				.method("PUT", bodySplit)
				.addHeader("Content-Type", "application/json")
				.addHeader("Authorization", config.splitAuthorization)
				.build();
		Response responseSplit = client.newCall(requestSplit).execute();
		checkResponse(responseSplit, "error writing list of distinct_ids to split segment: " + convertToSplitPattern(cohortName));
	}

	private static void createSplitSegmentForCohort(String cohortName) throws Exception {
		MediaType mediaType = MediaType.parse("application/json");
		String bodyAsString = "{\"name\":\"" + cohortName + "\",\n\t\"description\":\"\"}";
		//System.out.println(bodyAsString);
		RequestBody body = RequestBody.create(mediaType, bodyAsString);
		Request request = new Request.Builder()
		  .url("https://api.split.io/internal/api/v2/segments/ws/" + config.workspaceId + "/trafficTypes/" + config.trafficTypeId)
		  .method("POST", body)
		  .addHeader("Content-Type", "application/json")
		  .addHeader("Authorization", config.splitAuthorization)
		  .build();
		Response response = client.newCall(request).execute();
		checkResponse(response, "failed to create Split segment for cohort: " + cohortName);
		
		// now create a definition in our environment
		MediaType mediaTypeDef = MediaType.parse("application/json");
		RequestBody bodyDef = RequestBody.create(mediaTypeDef, "");
		Request requestDef = new Request.Builder()
		  .url("https://api.split.io/internal/api/v2/segments/" + config.environmentId + "/" + cohortName)
		  .method("POST", bodyDef)
		  .addHeader("Content-Type", "application/json")
		  .addHeader("Authorization", config.splitAuthorization)
		  .build();
		Response responseDef = client.newCall(requestDef).execute();
		checkResponse(responseDef, "failed to activate segment with name: " + cohortName);
	}

	private static JSONArray gatherDistinctIdsForCohort(OkHttpClient client, String cohortName, int cohortId) throws IOException {
		long start = System.currentTimeMillis();
		System.out.println("START MixPanel engage query");
		MediaType mediaType = MediaType.parse("application/x-www-form-urlencoded");
		@SuppressWarnings("deprecation")
		RequestBody body = RequestBody.create(mediaType, 
				"filter_by_cohort={\"raw_cohort\":{\"description\":\"\",\"name\":\"\",\"id\":null,\"unsavedId\":\"\",\"groups\":[{\"type\":\"cohort_group\",\"event\":{\"resourceType\":\"cohort\",\"value\":\"$all_users\",\"label\":\"All Users\"},\"filters\":[{\"filterValue\":" 
						+"[{\"cohort\":{\"id\":" + cohortId + ",\"name\":\"" + cohortName + "\"" 
						+ ",\"negated\":false}}],\"resourceType\":\"cohort\",\"propertyType\":\"list\",\"filterOperator\":\"contains\"}],\"filtersOperator\":\"and\",\"behavioralFiltersOperator\":\"and\",\"groupingOperator\":null,\"property\":null,\"dataGroupId\":\"\"}]}}&project_id=" + config.projectId);
		Request request = new Request.Builder()
				.url("https://mixpanel.com/api/2.0/engage?project_id=" + config.projectId)
				.method("POST", body)
				.addHeader("Authorization", config.mixpanelAuthorization)
				.addHeader("Content-Type", "application/x-www-form-urlencoded")
				.build();
		Response response = null;
		try {
			response = client.newCall(request).execute();
			checkResponse(response, "failed to gather distinct ids for cohort " + cohortName);
			System.out.println("FINISH MixPanel engage query in " + (System.currentTimeMillis() - start) + "ms");
		} catch (Exception e) {
			e.printStackTrace(System.err);
			System.err.println("Failed to reach MixPanel... exiting!");
			System.exit(1);
		}

		JSONObject jsonResponse = new JSONObject(response.body().string());
		JSONArray distinct_idList = new JSONArray();
		JSONArray resultsArray = jsonResponse.getJSONArray("results");
		for(int i = 0; i < resultsArray.length(); i++) {
			JSONObject userObject = resultsArray.getJSONObject(i);
			//System.out.println(userObject.getString("$distinct_id"));
			distinct_idList.put(userObject.getString("$distinct_id"));
		}
		return distinct_idList;
	}

	private static void checkResponse(Response response, String message) throws Exception {
		if(response != null && !response.networkResponse().isSuccessful()) {
			System.err.println("" + response.networkResponse().code() + ": " + response.body().string());
			System.err.println(message);
			new Exception().printStackTrace(System.err);
			System.exit(1);
		}
	}
}

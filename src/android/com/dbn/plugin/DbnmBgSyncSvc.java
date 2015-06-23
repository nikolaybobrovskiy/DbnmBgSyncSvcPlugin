package com.dbn.plugin;

import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.preference.PreferenceManager;
import android.util.JsonReader;
import android.util.JsonToken;
import android.util.Log;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.red_folder.phonegap.plugin.backgroundservice.BackgroundService;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// TODO: Parse possible exception from Direct API.
// TODO: Check connection.

public class DbnmBgSyncSvc extends BackgroundService {
    private final static String TAG = com.dbn.plugin.DbnmBgSyncSvc.class.getSimpleName();
    private boolean isDoingWork = false;
    private final Object isDoingWorkLock = new Object();
    private String directApiUrl;
    private String directApiAuth;
    private String appVersion;
    private String lang;
    private long pauseDuration;
    //    private boolean runOnce;
    private final static DecimalFormat DecimalFormatter = new DecimalFormat();

    static {
        DecimalFormatSymbols formatterSymbols = new DecimalFormatSymbols();
        formatterSymbols.setDecimalSeparator('.');
        DecimalFormatter.setDecimalSeparatorAlwaysShown(false);
        DecimalFormatter.setGroupingUsed(false);
        DecimalFormatter.setDecimalFormatSymbols(formatterSymbols);
    }

    @Override
    protected JSONObject doWork() {
        synchronized (this.isDoingWorkLock) {
            if (this.isDoingWork) {
                Log.d(DbnmBgSyncSvc.TAG, "Currently is RUNNING. So return simply return latest result.");
                return this.getLatestResult();
            }

            this.isDoingWork = true;
        }

        JSONObject currentResult = new JSONObject();
        JSONObject finalResult = new JSONObject();

        final Object finalResultLock = new Object();

        try {
            currentResult.put("isWorking", true);
            currentResult.put("progress", 0);
            this.setLatestResult(currentResult);

            String dbPath = this.getDatabasePath("dbnm-common").getAbsolutePath() + ".db";
            Log.d(DbnmBgSyncSvc.TAG, "Open database: " + dbPath);
            SQLiteDatabase db = SQLiteDatabase.openDatabase(dbPath, null, SQLiteDatabase.ENABLE_WRITE_AHEAD_LOGGING);
            try {
                ArrayList<IncomingUpdatesRequest> incomingUpdatesRequests = this.generateIncomingUpdatesRequests(db);
                final Object incomingUpdatesRequestsLock = new Object();

                while (!incomingUpdatesRequests.isEmpty()) {
                    JSONArray outgoingUpdatesRequests = this.generateOutgoingUpdatesRequests(db);
                    Log.d(DbnmBgSyncSvc.TAG, "Start processing of " + incomingUpdatesRequests.size() + " request(s)");
                    JSONObject directApiCallResponse = this.invokeDirectApiCall(this.createDirectApiSyncRequest(incomingUpdatesRequests, outgoingUpdatesRequests));
                    //noinspection UnusedAssignment
                    outgoingUpdatesRequests = null;

                    Log.d(DbnmBgSyncSvc.TAG, "Extracting incoming updates responses...");
                    ArrayList<IncomingUpdatesResponse> incomingUpdatesResponses = this.getIncomingUpdatesResponsesFromDirectApiSyncResponse(incomingUpdatesRequests, directApiCallResponse);
                    incomingUpdatesRequests.clear();

                    Log.d(DbnmBgSyncSvc.TAG, "Extracting outgoing updates responses...");
                    JSONArray outgoingUpdatesResponses = this.getOutgoingUpdatesResponsesFromDirectApiSyncResponse(directApiCallResponse);

                    Log.d(DbnmBgSyncSvc.TAG, "Extracting errors...");
                    JSONArray errors = this.getErrorsFromDirectApiSyncResponse(directApiCallResponse);

                    //noinspection UnusedAssignment
                    directApiCallResponse = null;

                    if (errors.length() > 0) {
                        StringBuilder exceptionMessageBuilder = new StringBuilder();
                        for (int i = 0; i < errors.length(); i++) {
                            if (exceptionMessageBuilder.length() > 0)
                                exceptionMessageBuilder.append(", ");
                            exceptionMessageBuilder.append(errors.getString(i));
                        }
                        finalResult.put("exception", true);
                        finalResult.put("exceptionMessage", exceptionMessageBuilder.toString());
                    }

                    ExecutorService executorService = Executors.newCachedThreadPool();

                    if (outgoingUpdatesResponses.length() > 0)
                        executorService.execute(new OutgoingUpdatesResponsesProcessor(db, outgoingUpdatesResponses, finalResult, finalResultLock));
                    else
                        Log.d(DbnmBgSyncSvc.TAG, "No outgoing sync.");
                    for (IncomingUpdatesResponse rsp : incomingUpdatesResponses) {
//                        if (!rsp.TableName.equals("Task")
//                                && !rsp.TableName.equals("WorkItem")
//                                && !rsp.TableName.equals("RelatedGood")
//                                && !rsp.TableName.equals("ActivityFirstMobileScreen")
//                                && !rsp.TableName.equals("ActivityProduct")) continue;
                        executorService.execute(new IncomingUpdatesResponseProcessor(db, rsp, finalResult, finalResultLock, incomingUpdatesRequests, incomingUpdatesRequestsLock));
                    }
                    executorService.shutdown();
                    executorService.awaitTermination(30, TimeUnit.MINUTES);
                }

            } finally {
                db.close();
            }
        } catch (Exception e) {
            Log.e(DbnmBgSyncSvc.TAG, e.getMessage(), e);
            try {
                finalResult.put("exception", true);
                finalResult.put("exceptionMessage", e.getMessage());
            } catch (JSONException e1) {
                Log.e(DbnmBgSyncSvc.TAG, e.getMessage(), e1);
            }
        } finally {
            synchronized (this.isDoingWorkLock) {
                this.isDoingWork = false;
            }

            try {
                finalResult.put("done", true);
            } catch (JSONException e) {
                Log.e(DbnmBgSyncSvc.TAG, e.getMessage(), e);
            }
        }

        Log.d(DbnmBgSyncSvc.TAG, "doWork() COMPLETED");
        return finalResult;
    }

    @Override
    protected JSONObject getConfig() {
        JSONObject result = new JSONObject();

        try {
            result.put("directApiUrl", this.directApiUrl);
            result.put("directApiAuth", this.directApiAuth);
            result.put("appVersion", this.appVersion);
            result.put("lang", this.lang);
            result.put("pauseDuration", this.pauseDuration);
        } catch (JSONException e) {
            Log.e(DbnmBgSyncSvc.TAG, e.getMessage(), e);
        }

        return result;
    }

    @Override
    protected void setConfig(JSONObject config) {
        try {
            SharedPreferences.Editor prefsEditor = PreferenceManager.getDefaultSharedPreferences(this).edit();
            if (config.has("directApiUrl")) this.directApiUrl = config.getString("directApiUrl");
            if (config.has("directApiAuth")) this.directApiAuth = config.getString("directApiAuth");
            if (config.has("appVersion")) this.appVersion = config.getString("appVersion");
            if (config.has("lang")) this.lang = config.getString("lang");
            if (config.has("pauseDuration")) this.pauseDuration = config.getLong("pauseDuration");
            if (config.has("resetResult") && config.getBoolean("resetResult")) {
                this.setLatestResult(new JSONObject());
            }
            if (config.has("clientAliveTimeStamp")) {
                Log.d(DbnmBgSyncSvc.TAG, "Pause duration set to " + this.pauseDuration);
                this.setPauseDuration(this.pauseDuration);
            }

            prefsEditor.putString(this.getClass().getName() + ".directApiUrl", this.directApiUrl);
            prefsEditor.putString(this.getClass().getName() + ".directApiAuth", this.directApiAuth);
            prefsEditor.putString(this.getClass().getName() + ".appVersion", this.appVersion);
            prefsEditor.putString(this.getClass().getName() + ".lang", this.lang);
            prefsEditor.commit();
        } catch (JSONException e) {
            Log.e(DbnmBgSyncSvc.TAG, e.getMessage(), e);
        }
    }

    @Override
    protected JSONObject initialiseLatestResult() {
        Log.d(DbnmBgSyncSvc.TAG, "initialiseLatestResult()");

        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(this);

        this.directApiUrl = prefs.getString(this.getClass().getName() + ".directApiUrl", "");
        this.directApiAuth = prefs.getString(this.getClass().getName() + ".directApiAuth", "");
        this.appVersion = prefs.getString(this.getClass().getName() + ".appVersion", "");
        this.lang = prefs.getString(this.getClass().getName() + ".lang", "");

        return null;
    }

    @Override
    protected void onTimerEnabled() {
        Log.d(DbnmBgSyncSvc.TAG, "Timer enabled");
    }

    @Override
    protected void onTimerDisabled() {
        Log.d(DbnmBgSyncSvc.TAG, "Timer disabled");
    }

    private JSONObject invokeDirectApiCall(JSONObject directApiRequest) throws JSONException, IOException {
        Log.d(DbnmBgSyncSvc.TAG, "Invoking Direct API...");
        HttpURLConnection conn = null;
        try {
            URL url = new URL(this.directApiUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(5000);
            //conn.setReadTimeout(timeout);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Auth", this.directApiAuth);
            conn.setRequestProperty("AppVersion", this.appVersion);
            conn.setRequestProperty("Lang", this.lang);
            OutputStreamWriter osw = new OutputStreamWriter(conn.getOutputStream(), "UTF-8");
            osw.write(directApiRequest.toString());
            osw.flush();
            osw.close();

            int statusCode = conn.getResponseCode();
            Log.d(DbnmBgSyncSvc.TAG, "Direct API responed with code " + statusCode);
            InputStream is = new BufferedInputStream(conn.getInputStream());
            Log.d(DbnmBgSyncSvc.TAG, "Parsing Direct API response...");
            JSONObject result = new JSONObject();

//            JSONObject result = new JSONObject(new Scanner(is).useDelimiter("\\A").next());

//            Log.d(DbnmBgSyncSvc.TAG, "   ...building JSON string...");
//            BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
//            StringBuilder builder = new StringBuilder();
//            for (String line; (line = reader.readLine()) != null; ) {
//                builder.append(line).append("\n");
//            }
//            Log.d(DbnmBgSyncSvc.TAG, "   ...creating tokener...");
//            JSONTokener tokener = new JSONTokener(builder.toString());
//            Log.d(DbnmBgSyncSvc.TAG, "   ...creating JSON object...");
//            JSONObject result = new JSONObject(tokener);

            JsonReader reader = new JsonReader(new InputStreamReader(is, "UTF-8"));
            try {
                readJsonObject(reader, result);
            } finally {
                reader.close();
            }

            Log.d(DbnmBgSyncSvc.TAG, "Direct API response parsing finished.");
            return result;
        } finally {
            Log.d(DbnmBgSyncSvc.TAG, "Direct API call finished");
            if (conn != null)
                conn.disconnect();
        }
    }

    private static void readJsonObject(JsonReader reader, JSONObject obj) throws IOException, JSONException {
        reader.beginObject();

        while (reader.hasNext()) {
            String name = reader.nextName();
            JsonToken curr = reader.peek();
            if (curr == JsonToken.NULL) {
                obj.put(name, null);
                reader.skipValue();
            } else if (curr == JsonToken.BOOLEAN) {
                obj.put(name, reader.nextBoolean());
            } else if (curr == JsonToken.NUMBER) {
                obj.put(name, reader.nextDouble());
            } else if (curr == JsonToken.STRING) {
                obj.put(name, reader.nextString());
            } else if (curr == JsonToken.BEGIN_OBJECT) {
                JSONObject childObj = new JSONObject();
                obj.put(name, childObj);
                readJsonObject(reader, childObj);
            } else if (curr == JsonToken.BEGIN_ARRAY) {
                JSONArray childArr = new JSONArray();
                obj.put(name, childArr);
                readJsonArray(reader, childArr);
            }
        }

        reader.endObject();
    }

    private static void readJsonArray(JsonReader reader, JSONArray arr) throws IOException, JSONException {
        reader.beginArray();

        while (reader.hasNext()) {
            JsonToken curr = reader.peek();
            if (curr == JsonToken.NULL) {
                arr.put(null);
                reader.skipValue();
            } else if (curr == JsonToken.BOOLEAN) {
                arr.put(reader.nextBoolean());
            } else if (curr == JsonToken.NUMBER) {
                arr.put(reader.nextDouble());
            } else if (curr == JsonToken.STRING) {
                arr.put(reader.nextString());
            } else if (curr == JsonToken.BEGIN_OBJECT) {
                JSONObject childObj = new JSONObject();
                arr.put(childObj);
                readJsonObject(reader, childObj);
            } else if (curr == JsonToken.BEGIN_ARRAY) {
                JSONArray childArr = new JSONArray();
                arr.put(childArr);
                readJsonArray(reader, childArr);
            }
        }

        reader.endArray();
    }

    private ArrayList<IncomingUpdatesRequest> generateIncomingUpdatesRequests(SQLiteDatabase db) {
        Log.d(DbnmBgSyncSvc.TAG, "Generating incoming updates requests...");
        ArrayList<IncomingUpdatesRequest> updatesRequests = new ArrayList<IncomingUpdatesRequest>();
        Cursor syncStates = db.rawQuery("select * from SyncState", null);
        try {
            syncStates.moveToFirst();
            while (!syncStates.isAfterLast()) {
                IncomingUpdatesRequest updatesRequest = new IncomingUpdatesRequest();
                updatesRequest.TableName = syncStates.getString(syncStates.getColumnIndex("tableName"));
                updatesRequest.LastSyncTimeStamp = syncStates.getDouble(syncStates.getColumnIndex("lastSyncTimeStamp"));
                updatesRequest.Id = updatesRequests.size();
                updatesRequest.RecordsSelectionStart = 0;
                Log.d(DbnmBgSyncSvc.TAG, "Update request for " + updatesRequest.TableName + ": " + Math.round(updatesRequest.LastSyncTimeStamp));
                updatesRequests.add(updatesRequest);
                syncStates.moveToNext();
            }
        } finally {
            syncStates.close();
        }

        Log.d(DbnmBgSyncSvc.TAG, "Generated " + updatesRequests.size() + " incoming updates request(s)");
        return updatesRequests;
    }

    private JSONArray generateOutgoingUpdatesRequests(SQLiteDatabase db) throws JSONException {
        Log.d(DbnmBgSyncSvc.TAG, "Generating outgoing updates requests...");
        JSONArray requests = new JSONArray();
        Cursor syncEntries = db.rawQuery("select * from SyncEntry", null);
        try {
            syncEntries.moveToFirst();

            while (!syncEntries.isAfterLast()) {
                JSONObject request = new JSONObject();
                request.put("newValue", syncEntries.getString(syncEntries.getColumnIndex("newValue")));
                request.put("id", syncEntries.getString(syncEntries.getColumnIndex("id")));
                request.put("tbl", syncEntries.getString(syncEntries.getColumnIndex("tbl")));
                request.put("oid", syncEntries.getString(syncEntries.getColumnIndex("oid")));
                request.put("field", syncEntries.getString(syncEntries.getColumnIndex("field")));
                request.put("type", syncEntries.getString(syncEntries.getColumnIndex("type")));
                request.put("oldValue", syncEntries.getString(syncEntries.getColumnIndex("oldValue")));
                request.put("timeStamp", syncEntries.getString(syncEntries.getColumnIndex("timeStamp")));
                request.put("session", syncEntries.getString(syncEntries.getColumnIndex("session")));
                request.put("state", syncEntries.getInt(syncEntries.getColumnIndex("state")));
                request.put("additionalInfo", syncEntries.getString(syncEntries.getColumnIndex("additionalInfo")));
                requests.put(request);
                syncEntries.moveToNext();
            }
        } finally {
            syncEntries.close();
        }

        Log.d(DbnmBgSyncSvc.TAG, "Generated " + requests.length() + " outgoing updates request(s)");
        return requests;
    }

    private JSONObject createDirectApiSyncRequest(
            ArrayList<IncomingUpdatesRequest> incomingUpdatesRequests,
            JSONArray outgoingUpdatesRequests) throws JSONException {
        JSONObject directApiRequest = new JSONObject();
        JSONObject methodParams = new JSONObject();
        JSONArray data = new JSONArray();
        JSONArray requests = new JSONArray();

        for (IncomingUpdatesRequest updateRequest : incomingUpdatesRequests) {
            JSONObject request = new JSONObject();
            request.put("tableName", updateRequest.TableName);
            request.put("lastSyncTimeStamp", updateRequest.LastSyncTimeStamp);
            request.put("index", updateRequest.Id);
            request.put("skip", updateRequest.RecordsSelectionStart);
            request.put("additionalParams", new JSONObject());
            requests.put(request);
        }

        methodParams.put("requests", requests);
        methodParams.put("sync", outgoingUpdatesRequests);
        methodParams.put("device", "MobileDevice");
        data.put(methodParams);
        directApiRequest.put("action", "DirectApi");
        directApiRequest.put("method", "syncTables");
        directApiRequest.put("type", "rpc");
        directApiRequest.put("tid", 1);
        directApiRequest.put("data", data);

        return directApiRequest;
    }

    private ArrayList<IncomingUpdatesResponse> getIncomingUpdatesResponsesFromDirectApiSyncResponse(ArrayList<IncomingUpdatesRequest> incomingUpdatesRequests, JSONObject response) throws JSONException {
        ArrayList<IncomingUpdatesResponse> result = new ArrayList<IncomingUpdatesResponse>();
        if (!response.has("result")) return result;
        if (!response.getJSONObject("result").has("syncResults")) return result;
        JSONArray syncResults = response.getJSONObject("result").getJSONArray("syncResults");

        for (int i = 0; i < syncResults.length(); i++) {
            JSONObject syncResult = syncResults.getJSONObject(i);
            int requestId = syncResult.getInt("index");
            IncomingUpdatesRequest incomingUpdatesRequest = incomingUpdatesRequests.get(requestId);
            IncomingUpdatesResponse incomingUpdatesResponse = new IncomingUpdatesResponse();
            incomingUpdatesResponse.RequestId = requestId;
            incomingUpdatesResponse.TableName = incomingUpdatesRequest.TableName;
            incomingUpdatesResponse.OriginalLastSyncTimeStamp = incomingUpdatesRequest.LastSyncTimeStamp;
            incomingUpdatesResponse.LastSyncTimeStamp = syncResult.getDouble("lastSyncTimeStamp");
            incomingUpdatesResponse.NextRecordsSelectionStart = syncResult.getInt("nextRequestStart");
            incomingUpdatesResponse.Records = syncResult.getJSONArray("records");
            syncResult.put("records", new JSONArray());
            result.add(incomingUpdatesResponse);
        }

        response.getJSONObject("result").put("syncResults", new JSONArray());
        //noinspection StatementWithEmptyBody
        while (syncResults.remove(0) != null) ;

        return result;
    }

    private JSONArray getOutgoingUpdatesResponsesFromDirectApiSyncResponse(JSONObject response) throws JSONException {
        JSONArray result = new JSONArray();
        if (!response.has("result")) return result;
        if (!response.getJSONObject("result").has("sync")) return result;
        result = response.getJSONObject("result").getJSONArray("sync");
        return result;
    }

    private JSONArray getErrorsFromDirectApiSyncResponse(JSONObject response) throws JSONException {
        JSONArray result = new JSONArray();
        if (!response.has("result")) return result;
        if (!response.getJSONObject("result").has("errors")) return result;
        result = response.getJSONObject("result").getJSONArray("errors");
        return result;
    }

    class IncomingUpdatesRequest {
        public int Id;
        public String TableName;
        public Double LastSyncTimeStamp;
        public int RecordsSelectionStart;
    }

    class IncomingUpdatesResponse {
        public int RequestId;
        public String TableName;
        public Double OriginalLastSyncTimeStamp;
        public Double LastSyncTimeStamp;
        public JSONArray Records;
        public int NextRecordsSelectionStart;
    }

    class OutgoingUpdatesResponsesProcessor implements Runnable {
        private SQLiteDatabase db;
        private JSONArray responses;
        private JSONObject commonResult;
        private final Object commonResultLock;

        public OutgoingUpdatesResponsesProcessor(
                SQLiteDatabase db,
                JSONArray responses,
                JSONObject commonResult,
                Object commonResultLock) {
            this.db = db;
            this.responses = responses;
            this.commonResult = commonResult;
            this.commonResultLock = commonResultLock;
        }

        @Override
        public void run() {
            Boolean exception = false;
            String exceptionMessage = null;
            try {
                Log.d(DbnmBgSyncSvc.TAG, "Sync entries results to process: " + this.responses.length());
                ArrayList<String> sqlStatements = new ArrayList<String>();
                for (int i = 0; i < this.responses.length(); i++) {
                    JSONObject response = this.responses.getJSONObject(i);
                    String syncEntryId = response.getString("id");
                    int syncEntryState = response.getInt("state");
                    if (syncEntryState == SyncEntryState.Applied.getValue()) {
                        sqlStatements.add("delete from SyncEntry where id = '" + syncEntryId + "';");
                    } else {
                        sqlStatements.add("update SyncEntry set state = " + syncEntryState + " where id = '" + syncEntryId + "';");
                    }
                }

                this.db.beginTransactionNonExclusive();
                try {
                    Log.d(DbnmBgSyncSvc.TAG, "Transaction started for table SyncEntry");
                    for (String sql : sqlStatements) {
                        this.db.execSQL(sql);
                    }

                    this.db.setTransactionSuccessful();
                    Log.d(DbnmBgSyncSvc.TAG, "Transaction is successful for table SyncEntry");
                } finally {
                    this.db.endTransaction();
                    Log.d(DbnmBgSyncSvc.TAG, "Transaction ended for table SyncEntry");
                }
            } catch (Exception e) {
                Log.e(DbnmBgSyncSvc.TAG, e.getMessage(), e);
                exception = true;
                exceptionMessage = e.getMessage();
            }

            JSONObject processingResult = new JSONObject();
            try {
                processingResult.put("exception", exception);
                processingResult.put("exceptionMessage", exceptionMessage);
                synchronized (this.commonResultLock) {
                    this.commonResult.put("SyncEntries", processingResult);
                }
            } catch (JSONException e) {
                Log.e(DbnmBgSyncSvc.TAG, e.getMessage(), e);
            }
        }
    }

    @SuppressWarnings("unused")
    enum SyncEntryState {
        Failed(1), DependentOnFailed(2), NotApplied(3), Applied(4);

        private int id;

        SyncEntryState(int id) {
            this.id = id;
        }

        public int getValue() {
            return this.id;
        }
    }

    class IncomingUpdatesResponseProcessor implements Runnable {
        private SQLiteDatabase db;
        private IncomingUpdatesResponse response;
        private JSONObject commonResult;
        private final Object commonResultLock;
        private ArrayList<IncomingUpdatesRequest> incomingUpdatesRequests;
        private final Object incomingUpdatesRequestsLock;

        public IncomingUpdatesResponseProcessor(
                SQLiteDatabase db,
                IncomingUpdatesResponse response,
                JSONObject commonResult,
                Object commonResultLock,
                ArrayList<IncomingUpdatesRequest> incomingUpdatesRequests,
                Object incomingUpdatesRequestsLock) {
            this.db = db;
            this.response = response;
            this.commonResult = commonResult;
            this.commonResultLock = commonResultLock;
            this.incomingUpdatesRequests = incomingUpdatesRequests;
            this.incomingUpdatesRequestsLock = incomingUpdatesRequestsLock;
        }

        @Override
        public void run() {
            int processedRecordsCount = 0;
            int recordsToProcessCount = this.response.Records.length();
            Boolean exception = false;
            String exceptionMessage = null;
            try {
                Log.d(DbnmBgSyncSvc.TAG, "Table " + this.response.TableName + " records to process: " + this.response.Records.length());

                this.db.beginTransactionNonExclusive();
                try {
                    Log.d(DbnmBgSyncSvc.TAG, "Transaction started for table " + this.response.TableName);

                    for (int i = 0; i < this.response.Records.length(); i++) {
                        JSONObject newRecordData = this.response.Records.getJSONObject(i);

                        if (newRecordData.has("_isDeleted") && newRecordData.getBoolean("_isDeleted")) {
                            String sql = "delete from " + this.response.TableName + " where id = '" + newRecordData.getString("id") + "';";
                            this.db.execSQL(sql);
                            processedRecordsCount++;
                            continue;
                        }

                        Iterator fieldNames = newRecordData.keys();
                        StringBuilder fieldNamesStringBuilder = new StringBuilder();
                        StringBuilder fieldValuesStringBuilder = new StringBuilder();
                        String idValue = "";
                        while (fieldNames.hasNext()) {
                            String fieldName = (String) fieldNames.next();

                            if (fieldNamesStringBuilder.length() != 0)
                                fieldNamesStringBuilder.append(",");
                            fieldNamesStringBuilder.append(fieldName);

                            if (fieldValuesStringBuilder.length() != 0)
                                fieldValuesStringBuilder.append(",");

                            Object fieldValue;
                            if (newRecordData.isNull(fieldName))
                                fieldValue = null;
                            else
                                fieldValue = newRecordData.get(fieldName);

                            if (fieldName.equals("id")) {
                                if (fieldValue instanceof String)
                                    idValue = "'" + fieldValue + "'";
                                else if (fieldValue != null)
                                    idValue = fieldValue.toString();
                            }

                            if (fieldValue == null) {
                                fieldValuesStringBuilder.append("null");
                            } else if (fieldValue instanceof String) {
                                fieldValuesStringBuilder.append("'").append(((String) fieldValue).replaceAll("'", "''")).append("'");
                            } else if (fieldValue instanceof Boolean) {
                                fieldValuesStringBuilder.append((Boolean) fieldValue ? 1 : 0);
                            } else if (fieldValue instanceof Double) {
                                fieldValuesStringBuilder.append(DbnmBgSyncSvc.DecimalFormatter.format(fieldValue));
                            } else if (fieldValue instanceof Float) {
                                fieldValuesStringBuilder.append(DbnmBgSyncSvc.DecimalFormatter.format(fieldValue));
                            } else if (fieldValue instanceof Integer) {
                                fieldValuesStringBuilder.append(fieldValue);
                            }
//                            if (fieldValue != null)
//                                Log.d(DbnmBgSyncSvc.TAG, "Field " + fieldName + " is of type " + fieldValue.getClass().getCanonicalName());
//                            else
//                                Log.d(DbnmBgSyncSvc.TAG, "Field " + fieldName + " is null");
                        }

                        if (this.response.TableName.equals("UserProfile")) {
                            fieldNamesStringBuilder.append(",userName,pwd,realPwd");
                            fieldValuesStringBuilder
                                    .append(",(select userName from UserProfile where id=").append(idValue).append(")")
                                    .append(",(select pwd from UserProfile where id=").append(idValue).append(")")
                                    .append(",(select realPwd from UserProfile where id=").append(idValue).append(")");
                        }

                        String sqlInertOrReplace = "insert or replace into " + this.response.TableName + " (" + fieldNamesStringBuilder.toString() + ") values (" + fieldValuesStringBuilder.toString() + ");";
//                    Log.d(DbnmBgSyncSvc.TAG, sqlInertOrReplace);
                        this.db.execSQL(sqlInertOrReplace);
                        processedRecordsCount++;
                    }

                    //noinspection StatementWithEmptyBody
                    while (this.response.Records.remove(0) != null) ;
                    this.response.Records = null;

                    if (this.response.NextRecordsSelectionStart > 0) {
                        synchronized (this.incomingUpdatesRequestsLock) {
                            IncomingUpdatesRequest newRequest = new IncomingUpdatesRequest();
                            newRequest.TableName = this.response.TableName;
                            newRequest.Id = this.incomingUpdatesRequests.size();
                            newRequest.LastSyncTimeStamp = this.response.OriginalLastSyncTimeStamp;
                            newRequest.RecordsSelectionStart = this.response.NextRecordsSelectionStart;
                            this.incomingUpdatesRequests.add(newRequest);
                            Log.d(DbnmBgSyncSvc.TAG, "Additional request #" + newRequest.Id + " for " + newRequest.TableName + " with start at " + newRequest.RecordsSelectionStart + " and timestamp = " + Math.round(this.response.LastSyncTimeStamp));
                        }
                    } else {
                        String syncStateSql = "update SyncState set lastSyncTimeStamp = " + Math.round(this.response.LastSyncTimeStamp) + " where tableName = '" + this.response.TableName + "'";
//                    Log.d(DbnmBgSyncSvc.TAG, syncStateSql);
                        this.db.execSQL(syncStateSql);
                    }

//                    this.db.yieldIfContendedSafely(0);
                    this.db.setTransactionSuccessful();
                    Log.d(DbnmBgSyncSvc.TAG, "Transaction is successful for table " + this.response.TableName);
                } finally {
                    this.db.endTransaction();
                    Log.d(DbnmBgSyncSvc.TAG, "Transaction ended for table " + this.response.TableName);
                }
            } catch (Exception e) {
                Log.e(DbnmBgSyncSvc.TAG, e.getMessage(), e);
                exception = true;
                exceptionMessage = e.getMessage();
            }

            JSONObject processingResult = new JSONObject();
            synchronized (this.commonResultLock) {
                try {
                    int previouslyProcessedRecordsCount = 0, previouslyRecordsToProcessCount = 0;
                    if (this.commonResult.has(this.response.TableName)) {
                        previouslyProcessedRecordsCount = this.commonResult.getJSONObject(this.response.TableName).getInt("processedRecordsCount");
                        previouslyRecordsToProcessCount = this.commonResult.getJSONObject(this.response.TableName).getInt("recordsToProcessCount");
                    }
                    processingResult.put("requestId", this.response.RequestId);
                    processingResult.put("newLastSyncTimeStamp", this.response.LastSyncTimeStamp);
                    processingResult.put("recordsToProcessCount", previouslyRecordsToProcessCount + recordsToProcessCount);
                    processingResult.put("processedRecordsCount", processedRecordsCount + previouslyProcessedRecordsCount);
                    processingResult.put("exception", exception);
                    processingResult.put("exceptionMessage", exceptionMessage);
                    this.commonResult.put(this.response.TableName, processingResult);
                } catch (JSONException e) {
                    Log.e(DbnmBgSyncSvc.TAG, e.getMessage(), e);
                }
            }
        }
    }
}
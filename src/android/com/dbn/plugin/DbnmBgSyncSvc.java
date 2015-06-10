package com.dbn.plugin;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.util.Log;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.red_folder.phonegap.plugin.backgroundservice.BackgroundService;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// TODO: Parse possible exception from Direct API.
// TODO: Params setting from client.

public class DbnmBgSyncSvc extends BackgroundService {
    private final static String TAG = com.dbn.plugin.DbnmBgSyncSvc.class.getSimpleName();
    private String dbPath = "/data/data/com.dbn.DbnmKfsTest/databases/dbnm-common.db";
    private String directApiUrl = "http://192.168.100.100:54593/rpc";
    private String directApiAuth = "YmE=Bf-#1NjMxNQ==";
    private String appVersion = "1.5.12";
    private String lang = "en";

    @Override
    protected JSONObject doWork() {
        JSONObject result = new JSONObject();
        final Object resultLock = new Object();

        try {
            SQLiteDatabase db = SQLiteDatabase.openDatabase(this.dbPath, null, SQLiteDatabase.ENABLE_WRITE_AHEAD_LOGGING);
            try {
                ArrayList<IncomingUpdatesRequest> incomingUpdatesRequests = this.generateIncomingUpdatesRequests(db);
                JSONArray outgoingUpdatesRequests = this.generateOutgoingUpdatesRequests(db);
                final Object incomingUpdatesRequestsLock = new Object();

                while (!incomingUpdatesRequests.isEmpty()) {
                    Log.d(DbnmBgSyncSvc.TAG, "Start processing of " + incomingUpdatesRequests.size() + " request(s)");
                    JSONObject directApiCallResponse = this.invokeDirectApiCall(this.createDirectApiSyncRequest(incomingUpdatesRequests, outgoingUpdatesRequests));
                    ArrayList<IncomingUpdatesResponse> incomingUpdatesResponses = this.getIncomingUpdatesResponsesFromDirectApiSyncResponse(incomingUpdatesRequests, directApiCallResponse);
                    JSONArray outgoingUpdatesResponses = this.getOutgoingUpdatesResponsesFromDirectApiSyncResponse(directApiCallResponse);
                    incomingUpdatesRequests.clear();
                    ExecutorService executorService = Executors.newCachedThreadPool();

                    if (outgoingUpdatesResponses.length() > 0)
                        executorService.execute(new OutgoingUpdatesResponsesProcessor(db, outgoingUpdatesResponses, result, resultLock));
                    else
                        Log.d(DbnmBgSyncSvc.TAG, "No outgoing sync.");
                    for (IncomingUpdatesResponse rsp : incomingUpdatesResponses) {
//                        if (!rsp.TableName.equals("Task")
//                                && !rsp.TableName.equals("WorkItem")
//                                && !rsp.TableName.equals("RelatedGood")
//                                && !rsp.TableName.equals("ActivityFirstMobileScreen")
//                                && !rsp.TableName.equals("ActivityProduct")) continue;
                        executorService.execute(new IncomingUpdatesResponseProcessor(db, rsp, result, resultLock, incomingUpdatesRequests, incomingUpdatesRequestsLock));
                    }
                    executorService.shutdown();
                    executorService.awaitTermination(30, TimeUnit.MINUTES);
                }

            } finally {
                db.close();
            }
        } catch (Exception e) {
            try {
                result.put("exception", true);
                result.put("exceptionMessage", e.getMessage());
            } catch (JSONException e1) {
                e1.printStackTrace();
            }
        }

        return result;
    }

    @Override
    protected JSONObject getConfig() {
        JSONObject result = new JSONObject();

        try {
            result.put("dbPath", this.dbPath);
            result.put("directApiUrl", this.directApiUrl);
            result.put("directApiAuth", this.directApiAuth);
            result.put("appVersion", this.appVersion);
            result.put("lang", this.lang);
        } catch (JSONException e) {
            e.printStackTrace();
        }

        return result;
    }

    @Override
    protected void setConfig(JSONObject config) {
        try {
            if (config.has("dbPath")) this.dbPath = config.getString("dbPath");
            if (config.has("directApiUrl")) this.directApiUrl = config.getString("directApiUrl");
            if (config.has("directApiAuth")) this.directApiAuth = config.getString("directApiAuth");
            if (config.has("appVersion")) this.appVersion = config.getString("appVersion");
            if (config.has("lang")) this.lang = config.getString("lang");
        } catch (JSONException e) {
            e.printStackTrace();
        }

    }

    @Override
    protected JSONObject initialiseLatestResult() {
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
            //conn.setConnectTimeout(timeout);
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

            //int statusCode = conn.getResponseCode();
            InputStream is = new BufferedInputStream(conn.getInputStream());
            return new JSONObject(new Scanner(is).useDelimiter("\\A").next());
        } finally {
            Log.d(DbnmBgSyncSvc.TAG, "Direct API call finished");
            if (conn != null)
                conn.disconnect();
        }
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
            result.add(incomingUpdatesResponse);
        }
        return result;
    }

    private JSONArray getOutgoingUpdatesResponsesFromDirectApiSyncResponse(JSONObject response) throws JSONException {
        JSONArray result = new JSONArray();
        if (!response.has("result")) return result;
        if (!response.getJSONObject("result").has("sync")) return result;
        result = response.getJSONObject("result").getJSONArray("sync");
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
            Boolean exception = false;
            String exceptionMessage = null;
            try {
                Log.d(DbnmBgSyncSvc.TAG, "Table " + this.response.TableName + " records to process: " + this.response.Records.length());
                ArrayList<String> sqlStatements = new ArrayList<String>();
                for (int i = 0; i < this.response.Records.length(); i++) {
                    JSONObject newRecordData = this.response.Records.getJSONObject(i);
                    String sql;

                    if (newRecordData.has("_isDeleted") && newRecordData.getBoolean("_isDeleted")) {
                        sql = "delete from " + this.response.TableName + " where id = '" + newRecordData.getString("id") + "';";
                        sqlStatements.add(sql);
                        continue;
                    }

                    Iterator fieldNames = newRecordData.keys();
                    StringBuilder fieldNamesStringBuilder = new StringBuilder();
                    StringBuilder fieldValuesStringBuilder = new StringBuilder();
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

                        if (fieldValue == null)
                            fieldValuesStringBuilder.append("null");
                        else if (fieldValue instanceof String)
                            fieldValuesStringBuilder.append("'").append(fieldValue).append("'");
                        else if (fieldValue instanceof Boolean)
                            fieldValuesStringBuilder.append((Boolean) fieldValue ? 1 : 0);
                        else if (fieldValue instanceof Double)
                            fieldValuesStringBuilder.append(fieldValue);
                        else if (fieldValue instanceof Float)
                            fieldValuesStringBuilder.append(fieldValue);
                        else if (fieldValue instanceof Integer)
                            fieldValuesStringBuilder.append(fieldValue);

//                            if (fieldValue != null)
//                                Log.d(DbnmBgSyncSvc.TAG, "Field " + fieldName + " is of type " + fieldValue.getClass().getCanonicalName());
//                            else
//                                Log.d(DbnmBgSyncSvc.TAG, "Field " + fieldName + " is null");
                    }
                    sql = "insert or replace into " + this.response.TableName + " (" + fieldNamesStringBuilder.toString() + ") values (" + fieldValuesStringBuilder.toString() + ");";
//                    Log.d(DbnmBgSyncSvc.TAG, sql);
                    sqlStatements.add(sql);
                }

                this.db.beginTransactionNonExclusive();
                try {
                    Log.d(DbnmBgSyncSvc.TAG, "Transaction started for table " + this.response.TableName);

                    for (String sql : sqlStatements) {
                        this.db.execSQL(sql);
                        processedRecordsCount++;
                    }

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
                    processingResult.put("recordsToProcessCount", previouslyRecordsToProcessCount + this.response.Records.length());
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
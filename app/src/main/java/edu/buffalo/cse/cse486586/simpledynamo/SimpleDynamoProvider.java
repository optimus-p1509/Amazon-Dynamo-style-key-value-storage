package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static android.content.Context.TELEPHONY_SERVICE;

public class SimpleDynamoProvider extends ContentProvider {

	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private static final int SERVER_PORT = 10000;
	private static final String ATYAS = "addToYourselfAndSuccessors";
	private static final String JUSTADD = "saveForPredecessor";
	private static final String ACK = "ackMsg";
	private static final String DELIMITER = "<>";
	private static final String DELIMITER2 = "###";
	private static final String QUERYLOCAL = "@";
	private static final String QUERYGLOBAL = "*";
	private static final String NF = "NOTFOUND";
	private static final String GIVEVALUE = "giveValueForKey";
	private static final String SENDLOCAL = "SendLocalString";
	private static final String SENDREPLICATIONREQUEST = "srr";
	private static final String SENDINSERTREQUEST = "sir";
	private static final String RECOVERY = "recovery";
	private static final String PENDINGINSERTS = "pendingInserts";
	String REMOTE_PORT0 = "5554";
	String REMOTE_PORT1 = "5556";
	String REMOTE_PORT2 = "5558";
	String REMOTE_PORT3 = "5560";
	String REMOTE_PORT4 = "5562";
	String[] CURRENT_PORTS = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
	HashMap<String, String> portMapReverse = new HashMap<String, String>();
	String myPort = ""; // 11xxx
	String myNodeString = ""; // = myPort / 2
	String myHash = ""; // genHash(myNode)
	String mySuccessor1 = ""; //from string array
	String mySuccessor2 = ""; //from string array

	String failedPort = "";
	String[] CurrentNodesList = new String[5];
	ArrayList<String> CurrentNodesHash = new ArrayList<String>();
	ArrayList<String> FailedInserts = new ArrayList<String>();

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			Log.i("onCreate","ServerSocket Created");
		} catch (IOException e) {
			e.printStackTrace();
			Log.e("onCreateException",e.toString());
			Log.e("onCreateException", "CHECK Can't create a ServerSocket");
		}
		TelephonyManager telManager = (TelephonyManager) getContext().getSystemService(TELEPHONY_SERVICE);
		String portString = telManager.getLine1Number().substring(telManager.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portString)) * 2);
		myNodeString = portString;
		try {
			myHash = genHash(myNodeString);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		for (String port : CURRENT_PORTS){
			try {
				CurrentNodesHash.add(genHash(port));
				Collections.sort(CurrentNodesHash);
				portMapReverse.put(genHash(port), Integer.toString(Integer.parseInt(port)*2));
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		int i = 0;
		for (String hash : CurrentNodesHash){
			CurrentNodesList[i++] = portMapReverse.get(hash);
		}
		System.out.println(Arrays.toString(CurrentNodesList));
		System.out.println(CurrentNodesHash);
		System.out.println(portMapReverse);
		Node myNode = new Node(myPort, CurrentNodesList);
		System.out.println(myNode.toString());
		mySuccessor1 = myNode.successor1Port;
		mySuccessor2 = myNode.successor2Port;

		Log.e("onCreate","I have Recovered");
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, RECOVERY, myPort);
		//Handle when alive again with client task
		Log.e("onCreate","END OF ON CREATE");
		return true;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.e("Delete","Deleting "+selection);
		if(selection.equals(QUERYGLOBAL) || selection.equals(QUERYLOCAL))
		{
			File[] list  = getContext().getFilesDir().listFiles();
			for (File file : list) {
				file.delete();
			}
		}else{
			File dir =  getContext().getFilesDir();
			File file= new File(dir,selection);
			file.delete();
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String keyToInsert = (String) values.get(KEY_FIELD);
		String valueToInsert = (String) values.get(VALUE_FIELD);

		Log.e("Insert Called", "Insert Called for key = " + keyToInsert + " at " + myPort);
		callInsert(keyToInsert,valueToInsert);
		return uri;
	}

	//callInsert function to insert at proper place
	private void callInsert(String key, String value) {
		String keyHash = null;
		try {
			keyHash = genHash(key);
			Log.i("callInsert", "Key = " + key);
			Log.i("callInsert", "keyHash = " + keyHash);
			String toPort = whereDoesItBelong(key, CurrentNodesHash, portMapReverse);
			Log.e("callInsert", "Key = " + key + " belongs to " + toPort);
			if (toPort.equals(myPort)) {
				Log.i("callInsert", "Belongs to Me");
				insertAtCurrentNode(key, value); //InsertToMyself
				Log.i("callInsert", "Sending to Successors : " + key);
				String msg1 = SENDREPLICATIONREQUEST + DELIMITER + key + DELIMITER + value;
				Log.i("CallInsert", "ClientTask to Self for " + key);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, mySuccessor1);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, mySuccessor2);

			} else {
				Log.e("callInsert", "Belongs to "+toPort);
				String msg2 = SENDREPLICATIONREQUEST + DELIMITER + key + DELIMITER + value;
				Log.i("CallInsert","msg2 = "+msg2);

				Log.i("CallInsert", "ClientTask to "+ toPort +" INSERT REPLICATE for " + key);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2, toPort);

				String suc = getSuccessorsForNode(toPort,CurrentNodesList);
				String succ1 = suc.split(DELIMITER)[0];
				String succ2 = suc.split(DELIMITER)[1];
				Log.i("CallInsert", "ClientTask to "+ succ1 +" REPLICATE for " + key);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2, succ1);
				Log.i("CallInsert", "ClientTask to "+ succ2 +" REPLICATE for " + key);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2, succ2);
			}

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Log.e("callInsert","Insert Finished for "+ key);
	}

	//returns successors for a node
	public static String getSuccessorsForNode(String port,String[] currentNodes){
		int len = currentNodes.length;
		for(int i=0; i<len; i++){
			if(currentNodes[i].equals(port)){
				String succ1 = currentNodes[(i+1)%len];
				String succ2 = currentNodes[(i+2)%len];
				Log.e("getSuccessorsForNode","Sending Back " + succ1 + "<>" + succ2);
				return succ1 + "<>" + succ2;
			}
		}
		Log.e("getSuccessorsForNode","Sending Back Blank");
		return "";
	}

	//Performs Insert at the Node
	private  void insertAtCurrentNode(String key, String value){
		try {
			Log.e("insertAtCurrentNode","Actually Inserting to myPort - "+myPort+"-"+key+" "+value);
			FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
			outputStream.write(value.getBytes());
			outputStream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			Log.e("insertAtCurrentNode","FileNotFoundException");
		} catch (IOException e) {
			e.printStackTrace();
			Log.e("insertAtCurrentNode","CHECK Node Failed before Insert");
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Log.e("Query","For Selection = " + selection);
		try {
			Thread.sleep(50);
			Log.i("Query","Hash = " + genHash(selection));
		} catch (NoSuchAlgorithmException e) {
//			e.printStackTrace();
			Log.e("Query","No such algorithm");
		} catch (InterruptedException e) {
//			e.printStackTrace();
			Log.e("Query","Thread.sleep");
		}
		String retString = "";
		if(selection.equals(QUERYLOCAL)){
			Log.i("Query", "Query Local");
			retString = queryMyselfForAllString();
			MatrixCursor returnCursor = getCursorFromString(retString);
			Log.i("query", "count = " + returnCursor.getCount());
			return returnCursor;
		}
		else if(selection.equals(QUERYGLOBAL)){
			Log.i("Query", "QUERY GLOBAL");
			String allLocal = queryAllforLocalString();
			String[] parts = allLocal.split("---");
			Set<String> localSet = new HashSet<String>(Arrays.asList(parts));
			for(String p : localSet){
				retString+=p+"---";
			}
			MatrixCursor returnCursor = getCursorFromString(retString);
			Log.i("query", "count = " + returnCursor.getCount());
			return returnCursor;
		}
		else {
			Log.i("Query","for Key " + selection);
			String inPort = whereDoesItBelong(selection, CurrentNodesHash, portMapReverse);
			Log.i("Query", selection + " Belongs to "+inPort);
			if (inPort.equals(myPort)) {
				Log.e("Query Selection","My Port");
				retString = selection + ":" +queryMyselfForOneString(selection);
				MatrixCursor returnCursor = getCursorFromString(retString);
				Log.i("query", "count = " + returnCursor.getCount());
				return returnCursor;
			}
			else {
				Log.e("Query Selection","From Port "+inPort);
				String val = getValueFromPort(selection, inPort);
				retString = selection + ":" + val;
				MatrixCursor returnCursor = getCursorFromString(retString);
				Log.i("query", "count = " + returnCursor.getCount());
				return returnCursor;
			}
		}
	}

	//Returns all local string
	private String queryMyselfForAllString(){
		String retString = "";
		FileInputStream inputStream;
		String mFileContent;
		byte[] content = new byte[50];
		String[] Flist = getContext().fileList();
//		System.out.println("FileList = "+Arrays.toString(Flist));
		for(String key : Flist){
			Log.i("Query", "Looking for key :" + key);
			try {
				inputStream = getContext().openFileInput(key);
				int length = inputStream.read(content);
				mFileContent = new String(content).substring(0, length);
				retString+= key+":"+mFileContent+"---";
				inputStream.close();
			} catch (Exception e) {
				Log.e("queryFileException", e.toString());
			}
		}
		return retString;
	}

	//Asks All For All local strings
	private String queryAllforLocalString(){
		String returnString = "";
		int count = 0;
		for (String port : CurrentNodesList) {
			if (port.equals(myPort)) {
				Log.e("QueryALL","My Port");
				returnString += queryMyselfForAllString();
			} else {
				try {
					Socket socketToPort = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(port));  //connection to correctPort for key,value
					Log.i("queryAllforLocal", "Established Socket to " + port);
					DataOutputStream msgSend = new DataOutputStream(socketToPort.getOutputStream());
					DataInputStream ackRecieve = new DataInputStream(socketToPort.getInputStream());
					String msg1 = SENDLOCAL;
					Log.i("queryAllforLocal", "Sending to " + port + " : " + msg1);
					msgSend.writeUTF(msg1);

					String ackRec = ackRecieve.readUTF();
					String[] ackParts = ackRec.split(DELIMITER);
					if (ackParts[0].equals(ACK)) {
						returnString += ackParts[1];
						count++;
						Log.i("queryAllforLocal", "Here count = " + count);
						msgSend.flush();
						msgSend.close();
						ackRecieve.close();
						socketToPort.close();
					} else
						Log.e("queryAllforLocal", "No ACK From " + port);
				} catch (IOException e) {
					e.printStackTrace();
					Log.e("queryAllforLocal", "CHECK Port = " + port + " Failed");
				}
			}
		}
		return returnString;
	}

	//Return value for a key from a particular port
	private String getValueFromPort(String key, String inPort){
//		String value = NF;
		try {
			Socket socketToCorrectPort = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(inPort));  //connection to correctPort for key,value
			DataOutputStream msgSend = new DataOutputStream(socketToCorrectPort.getOutputStream());
			DataInputStream ackRecieve = new DataInputStream(socketToCorrectPort.getInputStream());
			String msg1 = GIVEVALUE + DELIMITER + key;
			Log.i("getValue", "Sending to " + inPort + " : " + msg1);
			msgSend.writeUTF(msg1);

			String ackRec = ackRecieve.readUTF();
			String value = ackRec;
			msgSend.flush();
			msgSend.close();
			ackRecieve.close();
			socketToCorrectPort.close();

			return value;
		} catch (IOException e) {
//			e.printStackTrace();
			Log.e("getValue from Port","CHECK Port = " + inPort + " Failed"); //QuerySuccessors
			String succs = getSuccessorsForNode(inPort,CurrentNodesList);
			String succ1 = succs.split(DELIMITER)[0];
			String val = getValueFromPort(key,succ1);

			return val;
		}
	}

	//Returns a cursor from a string
	private MatrixCursor getCursorFromString(String str){
//		Log.i("getCursorFromString","string = "+str);
		String[] mColumnNames = {KEY_FIELD, VALUE_FIELD};
		MatrixCursor returnCursor = new MatrixCursor(mColumnNames);
		if(str.equals(""))
			return returnCursor;
		else {
			String[] keyValuePairs = str.split("---");
			for (String pair : keyValuePairs) {
				String[] p = pair.split(":");
				String key = p[0];
				String value = p[1];
				MatrixCursor.RowBuilder mRowBuilder = returnCursor.newRow();
				mRowBuilder.add(mColumnNames[0], key);
				mRowBuilder.add(mColumnNames[1], value);
			}
		}
		return returnCursor;
	}

	//Returns value for given key
	private String queryMyselfForOneString(String key){
		FileInputStream inputStream;
		String retString;
		String mFileContent;
		byte[] content = new byte[50];
		try {
			inputStream = getContext().openFileInput(key);
			int length = inputStream.read(content);
			mFileContent = new String(content).substring(0, length);
			retString = mFileContent;
			inputStream.close();
			return retString;
		} catch (Exception e) {
			Log.e("queryFileException", e.toString());
		}
		return "NotFound";
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			Log.i("ServerTask","InServerTask");
			ServerSocket serverSocket = sockets[0];
			try {
				while (true) {
					String msg = "";
					Socket clientSocket = serverSocket.accept();
					Log.i("ServerTask","SocketAccepted");
					DataInputStream inMsg = new DataInputStream(clientSocket.getInputStream());
					DataOutputStream outAck = new DataOutputStream(clientSocket.getOutputStream());
					msg = inMsg.readUTF();
					Log.i("ServerTask", "msgReceived = " + msg);
					String[] msgParts = msg.split(DELIMITER);
					String msgType = msgParts[0];
					Log.i("ServerTask", "msgType = " + msgType);
					//Receiving Recovery request from failed Node
					if (msgType.equals(RECOVERY)) {
						Log.e("ServerTask RECOVERY","Handling Missed Inserts");
						Log.e("ServerTask RECOVERY", "failedPort = " +failedPort);
						Log.e("ServerTask RECOVERY", "Port Recovered "+msgParts[1]);
						Log.e("ServerTask RECOVERY","FailedInserts.size() = "+FailedInserts.size());
						String msgBack = "";
						if (FailedInserts.size() != 0 && failedPort.equals(msgParts[1])) {
							for (String pair : FailedInserts) {
								msgBack += pair+DELIMITER2;
								Log.i("ServerTask Recovery","Publish Progress on");
								System.out.println(msgBack);
								publishProgress(msgBack);
							}
							//Clearing Inserts stored for failed nodes
//							Log.e("ServerTask RECOVERY","Sending back Inserts "+msgBack);
							FailedInserts.clear();
//							outAck.writeUTF(ACK +DELIMITER+ msgBack);
						}
//						else {
							Log.e("ServerTask Recovery", "No Pending Inserts");
							Log.e("ServerTask RECOVERY", "Sending back ACK after RECOVERY");
							outAck.writeUTF(ACK);
//						}
					}
					//Asking Node to add key-value to its own
					else if (msgType.equals(JUSTADD)) {
						Log.i("ServerTask JUSTADD", "Replicating at " + myPort);
						String key = msgParts[1];
						String value = msgParts[2];
						Log.i("ServerTask JUSTADD", "key = " + key + " value = " + value);
						insertAtCurrentNode(key, value);

						Log.e("ServerTask JUSTADD","Sending back ACK after Replicating");
						outAck.writeUTF(ACK);
					}
					//Asking a Node to give a value for a Key
					else if(msgType.equals(GIVEVALUE)){
						Log.i("ServerTask GIVEVALUE","Looking for Value for key " + msgParts[1]);
						String value = queryMyselfForOneString(msgParts[1]);
						Log.e("ServerTask GIVEVALUE","Sending Value = "+value);
//						String ackBack = ACK + DELIMITER + value;
						String ackBack = value;

						Log.e("ServerTask GIVEVALUE","Sending back ACK+value after GIVEVALUE");
						outAck.writeUTF(ackBack);
					}
					//Asking Node to Send all Local Keys
					else if(msgType.equals(SENDLOCAL)){
						Log.i("ServerTask","SendingLocalString");
						String ql = queryMyselfForAllString();
						String ackBack = ACK + DELIMITER + ql;

						Log.e("ServerTask SENDLOCAL","Sending back ACK+locak after SENDLOCAL");
						outAck.writeUTF(ackBack);
					}
					//Pending Inserts at Nodes
					else if(msgType.equals(PENDINGINSERTS)){
						Log.i("ServerTask","PENDINGINSERTS");
						String keyVals = msgParts[1];
						String[] pairs = keyVals.split(DELIMITER2);
						for (String keyval : pairs){
							String[] parts = keyval.split(":");
							Log.e("Server PENDINGINSERTS","Inserting to Failed Node "+parts[0]);
							insertAtCurrentNode(parts[0],parts[1]);
						}
					}
					else {
						Log.e("ServerTask", "Unhandled Case");
					}
					inMsg.close();
					outAck.flush();
					outAck.close();
					Log.e("ServerTask","ClientSocketClosed");
					clientSocket.close();
				}
			}catch (IOException e) {
				Log.e("ServerTask", "IO Exception");
				e.printStackTrace();
//				Log.e("ServerTask", e.toString());
//				Log.e("ServerTask","CHECK Failed Socket");
			}
			Log.e("ServerTask","END OF SERVER TASK");
			return null;
		}

		//Used to complete the Key-Value pairs after Recovery using client task
		@Override
		protected void onProgressUpdate(String... values) {
			super.onProgressUpdate(values);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, PENDINGINSERTS, values[0]);
		}
		private Uri buildUri(String scheme, String authority) {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(authority);
			uriBuilder.scheme(scheme);
			return uriBuilder.build();
		}
	}

	//Client Task to contact other server tasks at other Nodes
	private class ClientTask extends AsyncTask<String, Void, Void> {
		@Override
		protected Void doInBackground(String... params) {
			Log.e("ClientTask","Msg = "+params[0]);
			String[] msgParts = params[0].split(DELIMITER);
			String type = msgParts[0];
			//Sending replication request to successors and to the node
			if(type.equals(SENDREPLICATIONREQUEST)){
				Log.i("ClientTask REPLICATION","SENDREPLICATIONREQUEST");
				String msgSend = JUSTADD + DELIMITER + msgParts[1] + DELIMITER + msgParts[2];
				String toPort = params[1];
				try {
					Socket socketToPort = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(toPort));
					Log.e("ClientTask REPLICATION", "Connected Socket to Port "+toPort);
					DataOutputStream sendMsg = new DataOutputStream(socketToPort.getOutputStream());
					DataInputStream ackRecieve = new DataInputStream(socketToPort.getInputStream());
					Log.e("ClientTask Replication","Sending to "+toPort+" msg = "+msgSend);
					sendMsg.writeUTF(msgSend);

					Log.e("ClientTask Replication","Waiting for ACK");
//					try {
						String ackRec = ackRecieve.readUTF();
						Log.e("ClientTask Replication", "From Servertask " + ackRec);
						if (ackRec.equals(ACK)) {
							sendMsg.flush();
							sendMsg.close();
							ackRecieve.close();
							Log.e("ClientTask Replication", "Received ACK from " + toPort + "Closing Socket");
							socketToPort.close();
						} else
							Log.e("ClientTask Replication", "No ACK from " + toPort + "Check Socket");
//					}
				} catch (IOException e) {
//					e.printStackTrace();
					Log.e("ClientTask Replication","Cannot connect to "+toPort);
					Log.e("ClientTask Replication","CHECK" + toPort +" Failed before msg = " + msgSend);
					failedPort = toPort;
					Log.e("FailedPort Update","FailedPort Update " + failedPort);
					String pair = msgParts[1] + ":" + msgParts[2];
					FailedInserts.add(pair);
					Log.e("ClientTask Replication","Added to Failed Inserts "+msgParts[1]);
				} catch (Exception e){
					Log.e("ClientTask Replication","Unknown Exception e");
					e.printStackTrace();
				}
			}
			// Recovery from a node under 2 conditions
			//1. At the start
			//2. After a failure
			else if(type.equals(RECOVERY)){
				Log.e("ClientTask RECOVERY","Sending Recovery to All but me");
				for (String toPort : CURRENT_PORTS){
					Log.e("ClientTask RECOVERY","Current NODE = "+toPort);
					if(toPort.equals(myNodeString)) {
						Log.e("ClientTask RECOVERY","MY Port, Do Nothing");
					}
					else {
						try {
							Socket socketToPort = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(toPort)*2);
							Log.e("ClientTask RECOVERY","Socket to "+toPort+ " established");
							DataOutputStream sendMsg = new DataOutputStream(socketToPort.getOutputStream());
							DataInputStream ackRecieve = new DataInputStream(socketToPort.getInputStream());
							String msgSend = RECOVERY + DELIMITER + myPort;
							Log.e("ClientTask RECOVERY", "Sending to " + toPort + " msg = " + msgSend);
							sendMsg.writeUTF(msgSend);

							Log.e("ClientTask RECOVERY", "Waiting for Ack from " + toPort + " for msg = " + msgSend);
							String ackRec = ackRecieve.readUTF();
							Log.e("ClientTask RECOVERY", "Reading From " + toPort + " msg = " + ackRec);
//							if(ackRec.equals(null)) {
//								new IOException("null");
//							}
							if(ackRec.equals(ACK)) {
								sendMsg.flush();
								sendMsg.close();
								ackRecieve.close();
								Log.e("ClientTask RECOVERY", "Received ACK from " + toPort + " Closing Socket");
								socketToPort.close();
							}
						} catch (IOException e) {
//							e.printStackTrace();
							Log.i("ClientTask Recovery","FAILED AT START " + toPort);
							Log.v("ClientTask Recovery","SocketTimeout at " + toPort);
						}
					}
				}
			}
			//Insert the pending key-value pairs after recovery
			else if(type.equals(PENDINGINSERTS)){
				String keyVals = params[1];
				try {
					Socket socketToFailedPort = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(failedPort));
					DataOutputStream sendMsg = new DataOutputStream(socketToFailedPort.getOutputStream());
					DataInputStream ackRecieve = new DataInputStream(socketToFailedPort.getInputStream());
					String msgSend = PENDINGINSERTS + DELIMITER + keyVals;
					Log.e("Client PENDINGINSERTS", "Sending to " + failedPort + " msg = " + msgSend);
					sendMsg.writeUTF(msgSend);
					Log.e("Client PENDINGINSERTS", "Waiting for Ack from " + failedPort + " for msg = " + msgSend);

					String ackRec = ackRecieve.readUTF();
					Log.e("Client PENDINGINSERTS", "Reading From " + failedPort + " msg = " + ackRec);
					if(ackRec.equals(ACK)) {
						sendMsg.flush();
						sendMsg.close();
						ackRecieve.close();
						Log.e("ClientTask RECOVERY", "Received ACK from " + failedPort + " Closing Socket");
						socketToFailedPort.close();
					}

				}catch (IOException e){
					Log.e("Client PENDINGINSERTS","PENDINGINSERTS IO Exception");
					Log.e("Client PENDINGINSERTS","Failed to connect to "+failedPort);
				}
			}
			else
				Log.e("ClientTask","Unhandled Case");
			Log.i("ClientTask","ClientTaskEnd");
			return null;
		}
	}

	//Function to return where a key belongs in the ring
	public static String whereDoesItBelong (String key,ArrayList CurrentNodeHash, HashMap portMapReverse)  {
		String keyHash = null;
		try {
			keyHash = genHash(key);
			for(Object hash : CurrentNodeHash){
				if(keyHash.compareTo(hash.toString()) < 1)
					return portMapReverse.get(hash.toString()).toString();
			}
			return portMapReverse.get(CurrentNodeHash.get(0).toString()).toString();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}
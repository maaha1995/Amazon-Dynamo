//Citation: File Storage, File Retrieval, File delete : https://docs.oracle.com/javase/tutorial/essential/io/fileio.html
// parts of code from previous project

package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();

    String current_port = "";
    String predecessor_port1 = "";
    String predecessor_port2= "";
    String replication_port1 ="";
    String replication_port2 ="";
    ArrayList<Node> nodes = new ArrayList<Node>();
    HashSet<String> hashSet = new HashSet<String>();
    String[] remotePort = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    String[] portNums = {"5554","5556","5558","5560","5562"};
    ContentResolver contentProvider ;
    Boolean flag = false;

    HashMap<String, String> hashvalues = new HashMap<String, String>();

    class Node{
        String portNumber; // store the port number e.g. 5554, 5556
        String hashValue;   // hashed value of port number
        String predecessor1; // pred port number e.g. 5554 etc.
        String predecessor2;
        String successor1;   // succ port number e.g. 5554 etc.
        String successor2;


        public Node(String portNumber,String hashValue, String predecessor1,String predecessor2, String successor1, String successor2){
            this.portNumber = portNumber;
            this.hashValue = hashValue;
            this.predecessor1 = predecessor1;
            this.predecessor2 = predecessor2;
            this.successor1 = successor1;
            this.successor2 = successor2;
        }
    }

    public void addtoArrListandhashSet(String port){
        String hash_value = "";
        try {
            hash_value = genHash(port);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Node nodef = new Node (port, hash_value, null, null,null,null);
        nodes.add(nodef);
        hashSet.add(port);
        Collections.sort(nodes, new Comparator<Node>() {
            @Override
            public int compare(Node lhs, Node rhs) {
                return lhs.hashValue.compareTo(rhs.hashValue);
            }
        });

    }

    public void assignpd(){
        for(int i=0;i<nodes.size();i++){
            if (i == 0) {
                nodes.get(i).predecessor1 = nodes.get(nodes.size() - 1).portNumber;
                nodes.get(i).predecessor2 = nodes.get(nodes.size() - 2).portNumber;
                nodes.get(i).successor1 = nodes.get(i+1).portNumber;
                nodes.get(i).successor2 = nodes.get(i+2).portNumber;
            } else if (i == 1) {
                nodes.get(i).predecessor1 = nodes.get(i -1).portNumber;
                nodes.get(i).predecessor2 = nodes.get(nodes.size() - 1).portNumber;
                nodes.get(i).successor1 = nodes.get(i+1).portNumber;
                nodes.get(i).successor2 = nodes.get(i+2).portNumber;
            } else if(i == 2 ){
                nodes.get(i).predecessor1 = nodes.get(i - 1).portNumber;
                nodes.get(i).predecessor2 = nodes.get(i - 2).portNumber;
                nodes.get(i).successor1 = nodes.get(i+1).portNumber;
                nodes.get(i).successor2 = nodes.get(i+2).portNumber;
            }
            else if(i == 3 ){
                nodes.get(i).predecessor1 = nodes.get(i - 1).portNumber;
                nodes.get(i).predecessor2 = nodes.get(i - 2).portNumber;
                nodes.get(i).successor1 = nodes.get(i+1).portNumber;
                nodes.get(i).successor2 = nodes.get(0).portNumber;
            }
            else {
                nodes.get(i).predecessor1 = nodes.get(i - 1).portNumber;
                nodes.get(i).predecessor2 = nodes.get(i - 2).portNumber;
                nodes.get(i).successor1 = nodes.get(0).portNumber;
                nodes.get(i).successor2 = nodes.get(1).portNumber;
            }
            if (nodes.get(i).portNumber.equals(current_port)) {
                predecessor_port1 = nodes.get(i).predecessor1;
                predecessor_port2 = nodes.get(i).predecessor2;
                replication_port1 = nodes.get(i).successor1;
                replication_port2 = nodes.get(i).successor2;
            }
        }
    }

    public String multiplyPortNumBy2(String portNum){
        return Integer.toString(Integer.parseInt(portNum) * 2);
    }

    public String belongsTo(String key){
        String keyHash ="";

        try{
            keyHash = genHash(key);
        }catch(NoSuchAlgorithmException e){
            e.printStackTrace();
        }

        if(((keyHash.compareTo(nodes.get(0).hashValue)) >0) && ((keyHash.compareTo(nodes.get(1).hashValue)) <0 )){
            return nodes.get(1).portNumber;
        }
        else if(((keyHash.compareTo(nodes.get(1).hashValue)) >0) && ((keyHash.compareTo(nodes.get(2).hashValue)) <0 )){
            return nodes.get(2).portNumber;
        }
        else if(((keyHash.compareTo(nodes.get(2).hashValue)) >0) && ((keyHash.compareTo(nodes.get(3).hashValue)) <0 )){
            return nodes.get(3).portNumber;
        }
        else if(((keyHash.compareTo(nodes.get(3).hashValue)) >0) && ((keyHash.compareTo(nodes.get(4).hashValue)) <0 )){
            return nodes.get(4).portNumber;
        }
        else{
            return nodes.get(0).portNumber;
        }
    }

    public String getLatest(String sb){
        String[] replicas = sb.split("\\|");
        String[] value0 = replicas[0].split("#");
        String[] value1 = replicas[1].split("#");
        String[] value2 = replicas[2].split("#");


        if((value0[1].equals(value1[1])) && (value0[1].equals(value2[1]))){
            return value0[0]+"#"+value0[1];
        }
        else if((value0[1].compareTo(value1[1])) > 0 && (value0[1].compareTo(value2[1])) > 0){
            return value0[0]+"#"+value0[1];
        }
        else if((value1[1].compareTo(value0[1])) > 0 && (value1[1].compareTo(value2[1])) > 0){
            return value1[0]+"#"+value1[1];
        }
        else {
            return value2[0]+"#"+value2[1];
        }
    }

    public String[] get3successors(String port){

        if(port.equals(nodes.get(0).portNumber)){
            String[] stringtoreturn = {nodes.get(0).portNumber,nodes.get(1).portNumber,nodes.get(2).portNumber};
            return stringtoreturn;
        }
        else if(port.equals(nodes.get(1).portNumber)){
            String[] stringtoreturn = {nodes.get(1).portNumber,nodes.get(2).portNumber,nodes.get(3).portNumber};
            return stringtoreturn;

        }
        else if(port.equals(nodes.get(2).portNumber)){
            String[] stringtoreturn = {nodes.get(2).portNumber,nodes.get(3).portNumber,nodes.get(4).portNumber};
            return stringtoreturn;
        }
        else if(port.equals(nodes.get(3).portNumber)){
            String[] stringtoreturn = {nodes.get(3).portNumber,nodes.get(4).portNumber,nodes.get(0).portNumber};
            return stringtoreturn;
        }
        else {
            String[] stringtoreturn = {nodes.get(4).portNumber,nodes.get(0).portNumber,nodes.get(1).portNumber};
            return stringtoreturn;
        }
    }

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        current_port = portStr;
        contentProvider = getContext().getContentResolver();

        for(int i =0; i<remotePort.length; i++){
            addtoArrListandhashSet(portNums[i]);
        }

        assignpd();

        for(int j=0; j<nodes.size();j++){
            hashvalues.put(nodes.get(j).portNumber,nodes.get(j).hashValue);
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.d(TAG, "Can't create a ServerSocket");
        }
        Log.d("OnCreate","CurentPort: "+current_port);
        Log.d("OnCreate","PredPort1: "+predecessor_port1);
        Log.d("OnCreate","PredPort2: "+predecessor_port2);
        Log.d("OnCreate","SuccPort1: "+replication_port1);
        Log.d("OnCreate","SuccPort2: "+replication_port2);

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "OnCreate", current_port);

//        try {
//            Thread.sleep(3000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        return false;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        Log.d("Delete","Reached the method");

        String filename = selection;
        String port = belongsTo(filename);

        if(selectionArgs == null){
            if(selection.equals("@")){

            }
            else if(selection.equals("*")){

            }
            else{

                if(port.equals(current_port)){
                    try {
                        getContext().deleteFile(selection);
                    }
                    catch (Exception e) {
                    }

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteKeyR", selection,current_port);
                }

                else{
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteKey", selection,port);
                }

            }
        }

        else{
            try {
                getContext().deleteFile(selection);
            }
            catch (Exception e) {
            }
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

        FileOutputStream outputStream;

        Log.d("Insert","Method Called");

        String filename = (String)values.get("key");
        String string = (String) values.get("value");
        String value = "";

        String port = belongsTo(filename);

        Log.d("Insert-First","Key: "+filename+" belongs to : "+port);

        String[] all3ports = get3successors(port);

        Log.d("Insert-First","Key: "+filename+" All 3 ports are : "+all3ports[0]+"|"+all3ports[1]+"|"+all3ports[2]);

        if((current_port.equals(all3ports[0])) || (current_port.equals(all3ports[1])) || (current_port.equals(all3ports[2]))){

            if(port.equals(current_port)){
                long timestamp = System.currentTimeMillis();
                value = string +"#"+Long.valueOf(timestamp)+"#"+"O";
            }
            else{
                long timestamp = System.currentTimeMillis();
                value = string +"#"+Long.valueOf(timestamp)+"#"+"R:"+port;
            }

            try {
                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
                Log.d("insert_method","File write successful in current Node");
            } catch (Exception e) {
                Log.e("insert_method", "File write failed");
            }

            Log.d("Insert1of3", "Inserted Key: "+filename+" value is: "+value+" in port: "+current_port);

            StringBuilder otherports = new StringBuilder();

            for(int i=0; i<all3ports.length;i++){
                if(!(all3ports[i].equals(current_port))){
                    otherports.append(all3ports[i]);
                    otherports.append("#");
                }
            }

            Log.d("Insert1of3","Calling CLient task for other ports: "+otherports.toString());

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Insert", port,filename,string,otherports.toString());

        }

        else {

            StringBuilder otherports = new StringBuilder();

            for (int i = 0; i < all3ports.length; i++) {
                otherports.append(all3ports[i]);
                otherports.append("#");
            }

            Log.d("Insert-inanother", "Calling client task: " + port + filename + string+otherports.toString());

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Insert", port, filename, string, otherports.toString());

        }
            return null;
        }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {

        while(flag == false){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        Log.d("Query","Reached the method");

        String filename = selection;
        String port = belongsTo(filename);
        StringBuilder sb;

        Log.d("Query-First","Key: "+filename+" belongs to : "+port);

        String[] all3ports = get3successors(port);

        Log.d("Query-First","Key: "+filename+" All 3 ports are : "+all3ports[0]+"|"+all3ports[1]+"|"+all3ports[2]);

        if(selection.equals("@")){

            Log.d("Query@","ReachedCondition");

            File f = getContext().getFilesDir();
            File[] folderList = f.listFiles();
            Log.d("TT", Arrays.toString(folderList));
            String[] matrixColumns = {"key", "value"};
            MatrixCursor mco= new MatrixCursor(matrixColumns);

            HashMap<String, String> map1 = new HashMap<String, String>();
            HashMap<String, String> map2 = new HashMap<String, String>();
            HashMap<String, String> map3 = new HashMap<String, String>();
            HashSet<String> uniquekeys = new HashSet<String>();

            String ans = null;


            if (folderList != null) {

                for (File file : folderList) {

                    Log.d("Query@", "File is: " + file.getName());
                    sb = new StringBuilder();

                    try {
                        FileInputStream input = getContext().openFileInput(file.getName());
                        InputStreamReader input_reader = new InputStreamReader(input);
                        BufferedReader br = new BufferedReader(input_reader);

                        String line;
                        while ((line = br.readLine()) != null) {
                            sb.append(line);
                        }
                        String value1 = sb.toString();
                        String[] value2 = value1.split("#");
                        map1.put(file.getName(), value2[0] + "#" + value2[1]);

                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            BlockingQueue<String> queue3 = new ArrayBlockingQueue<String>(1);

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Query@", current_port, selection, queue3);

            try {
                ans = queue3.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            String[] twostrings = ans.toString().split("!");

            Log.d("Client-OnCreate", "TwoStrings: " + twostrings.toString());

            if (twostrings.length != 0) {
                if (twostrings.length == 2) {
                    String string1 = twostrings[0];
                    Log.d("Query@", "String1: " + string1);
                    String[] value3 = string1.split("\\|");
                    Log.d("Query@", "value3: " + value3.length);

                    for (int k = 0; k < value3.length; k++) {
                        Log.d("Client-OnCreate", "Splitting: " + value3[k]);
                        String value4 = value3[k];
                        String[] value5 = value4.split("#");
                        Log.d("Client-OnCreate", "After Splitting: " + value5.toString());
                        Log.d("Client-OnCreate", "Inserting in map2: " + "Key: " + value5[0] + ", val: " + value5[1] + "#" + value5[2]);
                        map2.put(value5[0], value5[1] + "#" + value5[2]);

                    }

                    String string2 = twostrings[1];

                    String[] value6 = string2.split("\\|");

                    for (int l = 0; l < value6.length; l++) {
                        String value7 = value6[l];
                        String[] value8 = value7.split("#");
                        Log.d("Client-OnCreate", "Inserting in map3: " + "Key: " + value8[0] + ", val: " + value8[1] + "#" + value8[2]);
                        map3.put(value8[0], value8[1] + "#" + value8[2]);

                    }
                } else if (twostrings.length == 1){
                    String string1 = twostrings[0];
                    Log.d("Client-OnCreate", "String1: " + string1);
                    String[] value3 = string1.split("\\|");
                    Log.d("Client-OnCreate", "value3" + value3.length);

                    for (int k = 0; k < value3.length; k++) {
                        Log.d("Client-OnCreate", "Splitting: " + value3[k]);
                        String value4 = value3[k];
                        String[] value5 = value4.split("#");
                        Log.d("Client-OnCreate", "After Splitting: " + value5.toString());
                        Log.d("Client-OnCreate", "Inserting in map2: " + "Key: " + value5[0] + ", val: " + value5[1] + "#" + value5[2]);
                        map2.put(value5[0], value5[1] + "#" + value5[2]);

                    }
                }
            }

            for (String key : map1.keySet()) {
                uniquekeys.add(key);
            }
            for (String key : map2.keySet()) {
                uniquekeys.add(key);
            }
            for (String key : map3.keySet()) {
                uniquekeys.add(key);
            }
            for (String s : uniquekeys) {

                StringBuilder sb2 = new StringBuilder();

                Log.d("Query", "sb2(1): " + sb2.toString());

                int flag = 0;

                if (map1.containsKey(s)) {
                    String val1 = map1.get(s);
                    sb2.append(val1);
                    sb2.append("|");
                    flag = 1;
                }
                Log.d("Query", "sb2(2): " + sb2.toString());

                if (map2.containsKey(s)) {
                    String val2 = map2.get(s);
                    sb2.append(val2);
                    sb2.append("|");
                    if (flag == 1) {
                        flag = 2;
                    } else {
                        flag = 1;
                    }
                }
                Log.d("Query", "sb2(3): " + sb2.toString());

                if (map3.containsKey(s)) {
                    String val3 = map3.get(s);
                    sb2.append(val3);
                    sb2.append("|");

                    if (flag == 2) {
                        flag = 3;
                    } else if (flag == 1) {
                        flag = 2;
                    } else {
                        flag = 1;
                    }
                }
                Log.d("Client-OnCreate", "sb2(4): " + sb2.toString());

                if (flag == 1) {
                    String repval = sb2.toString();
                    sb2.append(repval);
                    Log.d("Client-OnCreate", "sb2(5): " + sb2.toString());
                    sb2.append(repval);
                    Log.d("Client-OnCreate", "sb2(6): " + sb2.toString());
                } else if (flag == 2) {
                    String[] repval = sb2.toString().split("\\|");
                    sb2.append(repval[0]);
                    Log.d("Client-OnCreate", "sb2(7): " + sb2.toString());
                }


                String valueto_return = getLatest(sb2.toString());

                String[] f_valuetoreturn = valueto_return.split("#");

                Log.d("Query","Latest value: "+f_valuetoreturn[0]);

                mco.addRow(new Object[]{s, f_valuetoreturn[0]});
            }

            return mco;
        }

        else if(selection.equals("*")) {

            Log.d("Query@","ReachedCondition");

            File f = getContext().getFilesDir();
            File[] folderList = f.listFiles();
            Log.d("TT", Arrays.toString(folderList));
            String[] matrixColumns = {"key", "value"};
            MatrixCursor mco= new MatrixCursor(matrixColumns);

            String ans = null;


            if (folderList != null) {

                for (File file : folderList) {

                    Log.d("Query@", "File is: " + file.getName());
                    sb = new StringBuilder();

                    try {
                        FileInputStream input = getContext().openFileInput(file.getName());
                        InputStreamReader input_reader = new InputStreamReader(input);
                        BufferedReader br = new BufferedReader(input_reader);

                        String line;
                        while ((line = br.readLine()) != null) {
                            sb.append(line);
                        }
                        String value1 = sb.toString();
                        String[] value2 = value1.split("#");
                        mco.addRow(new Object[]{file.getName(), value2[0]});

                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            BlockingQueue<String> queue2 = new ArrayBlockingQueue<String>(1);

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Query*", current_port, selection, queue2);

            try {
                ans = queue2.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            String[] allvalues = ans.split("\\|");

            for(int i=0; i<allvalues.length; i++){
                String[] keyval = allvalues[i].split("#");
                mco.addRow(new Object[]{keyval[0], keyval[1]});
            }

            return mco;
        }

        else {

            sb = new StringBuilder();
            StringBuilder otherports = new StringBuilder();

            if ((current_port.equals(all3ports[0])) || (current_port.equals(all3ports[1])) || (current_port.equals(all3ports[2]))) {

                try {

                    FileInputStream input = getContext().openFileInput(selection);
                    InputStreamReader input_reader = new InputStreamReader(input);
                    BufferedReader br = new BufferedReader(input_reader);

                    String line;
                    while ((line = br.readLine()) != null) {
                        sb.append(line);
                    }

                } catch (Exception e) {
                }

                Log.d("Query", "Retrived file in current node and waiting for Replicas");

                sb.append("|");

                for (int i = 0; i < all3ports.length; i++) {
                    if (!(all3ports[i].equals(current_port))) {
                        otherports.append(all3ports[i]);
                        otherports.append("#");
                    }
                }

            }
            else{

                for (int i = 0; i < all3ports.length; i++) {
                    otherports.append(all3ports[i]);
                    otherports.append("#");
                }

                Log.d("Query", "File not present, waiting for all 3 copies");

            }

            BlockingQueue<String> queue1 = new ArrayBlockingQueue<String>(1);

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Query", current_port, selection, otherports.toString(), queue1);

            String ans = null;
            try {
                ans = queue1.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            sb.append(ans);

            String[] replicas = sb.toString().split("\\|");

            StringBuilder s = new StringBuilder();

            if (replicas.length == 1) {
                s.append(replicas[0]);
                s.append("|");
                s.append(replicas[0]);
                s.append("|");
                s.append(replicas[0]);
            } else if (replicas.length == 2) {
                s.append(replicas[0]);
                s.append("|");
                s.append(replicas[1]);
                s.append("|");
                s.append(replicas[1]);
            } else {
                s.append(replicas[0]);
                s.append("|");
                s.append(replicas[1]);
                s.append("|");
                s.append(replicas[2]);
            }


            Log.d("Query","string to be compared: "+s.toString());

            String valueto_return = getLatest(s.toString());

            String[] f_valuetoreturn = valueto_return.split("#");

            Log.d("Query","Latest value: "+f_valuetoreturn[0]);


            String[] matrixColumns = {"key", "value"};
            MatrixCursor mco = new MatrixCursor(matrixColumns);
            mco.addRow(new Object[]{selection, f_valuetoreturn[0]});
            mco.close();

            return mco;

        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {

                while (true) {
                    Socket socket = serverSocket.accept();

                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String message = in.readLine();

                    Log.d("Server ","Message Recieved: "+message);

                    String[] result = message.split("#");

                    Log.d("Server: Result String", Arrays.toString(result));

                    if(result[2].equals("OnCreate")){

                        File f = getContext().getFilesDir();
                        File[] folderList = f.listFiles();
                        StringBuilder sb;
                        StringBuilder stringtosend =new StringBuilder();
                        StringBuilder string1 = new StringBuilder();//map2
                        StringBuilder string2 = new StringBuilder();//map3

                        Log.d("Server-Queryfor@","number of keys present:"+folderList.length);

                        if(result[1].equals(predecessor_port1)){

                            if (folderList != null) {

                                for (File file : folderList) {

                                    Log.d("Server-Query@", "File is: " + file.getName());
                                    sb = new StringBuilder();

                                    try {
                                        FileInputStream input = getContext().openFileInput(file.getName());
                                        InputStreamReader input_reader = new InputStreamReader(input);
                                        BufferedReader br = new BufferedReader(input_reader);

                                        String line;
                                        while ((line = br.readLine()) != null) {
                                            sb.append(line);
                                        }
                                        String value1 = sb.toString();
                                        String[] value2 = value1.split("#");

                                        Log.d("Server-Query@","condition: "+value2[2]+"with: "+"R:"+result[1]);


                                        if ((value2[2].equals("R:"+predecessor_port1))) {
                                            string1.append(file.getName()+"#"+value2[0]+"#"+value2[1]);
                                            Log.d("Server-Query@","appending: "+file.getName()+"#"+value2[0]+"#"+value2[1]);
                                            string1.append("|");
                                        }
                                        else if(((value2[2].equals("R:"+predecessor_port2)))){
                                            string2.append(file.getName()+"#"+value2[0]+"#"+value2[1]);
                                            Log.d("Server-Query@","appending: "+file.getName()+"#"+value2[0]+"#"+value2[1]);
                                            string2.append("|");
                                        }

                                    } catch (FileNotFoundException e) {
                                        e.printStackTrace();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }

                            stringtosend.append(string1.toString());
                            stringtosend.append("!");
                            stringtosend.append(string2.toString());

                        }
                        else if(result[1].equals(predecessor_port2)){

                            if (folderList != null) {

                                for (File file : folderList) {

                                    Log.d("Server-Query@", "File is: " + file.getName());
                                    sb = new StringBuilder();


                                    try {
                                        FileInputStream input = getContext().openFileInput(file.getName());
                                        InputStreamReader input_reader = new InputStreamReader(input);
                                        BufferedReader br = new BufferedReader(input_reader);

                                        String line;
                                        while ((line = br.readLine()) != null) {
                                            sb.append(line);
                                        }
                                        String value1 = sb.toString();
                                        String[] value2 = value1.split("#");

                                        Log.d("Server-Query@","condition: "+value2[2]+"with: "+"R:"+result[1]);

                                        if (((value2[2].equals("R:"+predecessor_port2)))) {
                                            stringtosend.append(file.getName()+"#"+value2[0]+"#"+value2[1]);
                                            Log.d("Server-Query@","appending: "+file.getName()+"#"+value2[0]+"#"+value2[1]);
                                            stringtosend.append("|");
                                        }


                                    } catch (FileNotFoundException e) {
                                        e.printStackTrace();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }

                            stringtosend.append("!");
                        }
                        else if(result[1].equals(replication_port1)){

                            if (folderList != null) {

                                for (File file : folderList) {

                                    Log.d("Server-Query@", "File is: " + file.getName());
                                    sb = new StringBuilder();


                                    try {
                                        FileInputStream input = getContext().openFileInput(file.getName());
                                        InputStreamReader input_reader = new InputStreamReader(input);
                                        BufferedReader br = new BufferedReader(input_reader);

                                        String line;
                                        while ((line = br.readLine()) != null) {
                                            sb.append(line);
                                        }
                                        String value1 = sb.toString();
                                        String[] value2 = value1.split("#");

                                        Log.d("Server-Query@","Received value is: "+value2.toString());

                                        Log.d("Server-Query@","condition: "+value2[2]+"with: "+"R:"+result[1]);

                                        if (((value2[2].equals("R:"+predecessor_port1)))) {
                                            string2.append(file.getName()+"#"+value2[0]+"#"+value2[1]);
                                            Log.d("Server-Query@","appending R: "+file.getName()+"#"+value2[0]+"#"+value2[1]);
                                            string2.append("|");
                                        }

                                        else if(((value2[2].equals("O")))){
                                            string1.append(file.getName()+"#"+value2[0]+"#"+value2[1]);
                                            Log.d("Server-Query@","appending O: "+file.getName()+"#"+value2[0]+"#"+value2[1]);
                                            string1.append("|");
                                        }

                                    } catch (FileNotFoundException e) {
                                        e.printStackTrace();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }

                            Log.d("Server-Query@","String1: "+string1.toString());
                            Log.d("Server-Query@","String2: "+string2.toString());

                            stringtosend.append(string1.toString());
                            stringtosend.append("!");
                            stringtosend.append(string2.toString());
                        }
                        else if(result[1].equals(replication_port2)){

                            if (folderList != null) {

                                for (File file : folderList) {

                                    Log.d("Server-Query@", "File is: " + file.getName());
                                    sb = new StringBuilder();
                                    try {
                                        FileInputStream input = getContext().openFileInput(file.getName());
                                        InputStreamReader input_reader = new InputStreamReader(input);
                                        BufferedReader br = new BufferedReader(input_reader);

                                        String line;
                                        while ((line = br.readLine()) != null) {
                                            sb.append(line);
                                        }
                                        String value1 = sb.toString();
                                        String[] value2 = value1.split("#");

                                        Log.d("Server-Query@","condition: "+value2[2]+"with: "+"R:"+result[1]);

                                        if (((value2[2].equals("O")))) {
                                            stringtosend.append(file.getName()+"#"+value2[0]+"#"+value2[1]);
                                            Log.d("Server-Query@","appending: "+file.getName()+"#"+value2[0]+"#"+value2[1]);
                                            stringtosend.append("|");
                                        }

                                    } catch (FileNotFoundException e) {
                                        e.printStackTrace();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }

                            stringtosend.append("!");
                        }

                        Log.d("Server-Query@","stringtoSend"+stringtosend);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeBytes(stringtosend.toString()+"\n");
                        out.flush();

                    }

                    if(result[2].equals("Insert")){

                        String key = result[0];
                        String value1 = result[1];
                        String port = result[3];
                        String value="";
                        FileOutputStream outputStream;

                        Log.d("Server-Insert","key: "+key+"Value: "+value1+"Belongs to: "+port);

                        if(port.equals(current_port)){
                            long timestamp = System.currentTimeMillis();
                            value = value1 +"#"+Long.valueOf(timestamp)+"#"+"O";
                            Log.d("Server-Insert","Inserted key: "+key+" as direct in port: "+current_port);
                        }
                        else{
                            long timestamp = System.currentTimeMillis();
                            value = value1 +"#"+Long.valueOf(timestamp)+"#"+"R:"+port;
                            Log.d("Server-Insert","Inserted key: "+key+" as replica in port: "+current_port);
                        }
                        try {
                            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                            outputStream.write(value.getBytes());
                            outputStream.close();
                            Log.d("insert_method","File write successful in current Node");
                        } catch (Exception e) {
                            Log.e("insert_method", "File write failed");
                        }
                        Log.d("Server-Insert","Key: "+key+"is stored with value: "+value);

                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeBytes("Ack"+"\n");
                        out.flush();

                    }

                    if(result[2].equals("Query")){

                        StringBuilder sb = new StringBuilder();

                        try {

                            FileInputStream input = getContext().openFileInput(result[1]);
                            InputStreamReader input_reader = new InputStreamReader(input);
                            BufferedReader br = new BufferedReader(input_reader);

                            String line;
                            while ((line = br.readLine()) != null) {
                                sb.append(line);

                            }

                        } catch (Exception e) {
                        }

                        sb.append("|");

                        Log.d("Server-Query","value to send back: "+sb.toString());

                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeBytes(sb.toString()+"\n");
                        out.flush();

                    }


                    if(result[2].equals("Query*")){

                        Log.d("Server-Query* Reached ","on port: "+current_port);

                        StringBuilder sb;
                        StringBuilder stringtosend = new StringBuilder();

                        File f = getContext().getFilesDir();
                        File[] folderList = f.listFiles();

                        String ans = null;

                        if (folderList != null) {

                            for (File file : folderList) {

                                Log.d("Server-Query*", "File is: " + file.getName());
                                sb = new StringBuilder();

                                try {
                                    FileInputStream input = getContext().openFileInput(file.getName());
                                    InputStreamReader input_reader = new InputStreamReader(input);
                                    BufferedReader br = new BufferedReader(input_reader);

                                    String line;
                                    while ((line = br.readLine()) != null) {
                                        sb.append(line);
                                    }

                                    String value1 = sb.toString();
                                    String[] value2 = value1.split("#");

                                    stringtosend.append(file.getName());
                                    stringtosend.append("#");
                                    stringtosend.append(value2[0]);
                                    stringtosend.append("|");

                                } catch (FileNotFoundException e) {
                                    e.printStackTrace();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }

                        else{
                            stringtosend.append("None");
                        }

                        Log.d("Server-Query*","stringtoSend"+stringtosend);

                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeBytes(stringtosend.toString()+"\n");
                        out.flush();
                    }

                    if(result[2].equals("DeleteKey")){

                        contentProvider.delete(mUri,
                                result[1],null);
                    }

                    if(result[2].equals("DeleteKeyR")){

                        String[] s = {"R"};
                        contentProvider.delete(mUri,
                                result[1],s);
                    }

                    socket.close();
                }
            }catch(Exception e){
                e.printStackTrace();
            }
            return null;
        }
    }

    private class ClientTask extends AsyncTask<Object, Void, Void> {

        @Override
        protected Void doInBackground(Object... msgs) {

            try {

                String[] replicas = {replication_port1, replication_port2};
                String[] allotherports = {predecessor_port1, predecessor_port2, replication_port1, replication_port2};

                if(msgs[0].toString().equals("OnCreate")) {

                    Log.d("Client-OnCreate","Reached Method");

                    FileOutputStream outputStream;

                    File f = getContext().getFilesDir();
                    File[] folderList = f.listFiles();


                    HashMap<String, String> map1 = new HashMap<String, String>();
                    HashMap<String, String> map2 = new HashMap<String, String>();
                    HashMap<String, String> map3 = new HashMap<String, String>();
                    HashSet<String> uniquekeys = new HashSet<String>();

                    StringBuilder sb;
                    StringBuilder map2contents = new StringBuilder();
                    StringBuilder map3contents = new StringBuilder();
                    StringBuilder sbf = new StringBuilder();


                    if (folderList != null) {

                        for (File file : folderList) {

                            sb = new StringBuilder();

                            try {
                                FileInputStream input = getContext().openFileInput(file.getName());
                                InputStreamReader input_reader = new InputStreamReader(input);
                                BufferedReader br = new BufferedReader(input_reader);

                                String line;
                                while ((line = br.readLine()) != null) {
                                    sb.append(line);
                                }
                                String value1 = sb.toString();
                                String[] value2 = value1.split("#");
                                Log.d("OnCreate", "Inserting in map1: " + "Key: " + file.getName() + ", val: " + value2[0] + "#" + value2[1]);
                                //getContext().deleteFile(file.getName());
                                map1.put(file.getName(), value2[0] + "#" + value2[1]);

                            } catch (FileNotFoundException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    Log.d("Client-OnCreate","Number of keys in Map1: "+map1.size());

                    for (int i = 0; i < allotherports.length; i++) {

                        try {

                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(multiplyPortNumBy2(allotherports[i])));

                            String msgToSend = "OnCreate" + "#" + current_port + "#" + "OnCreate";

                            DataOutputStream out =
                                    new DataOutputStream(socket.getOutputStream());
                            out.writeBytes(msgToSend + "\n");
                            out.flush();

                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String rec_string = in.readLine();

                            Log.d("Client-OnCreate","rec_string from "+multiplyPortNumBy2(allotherports[i])+" is: "+rec_string);

                            if(rec_string != null){
                                if ((allotherports[i]).equals(replication_port1)) {
                                    String[] mapcontents = rec_string.split("!");
                                    if (mapcontents.length != 0) {

                                        if (mapcontents.length == 2) {
                                            map2contents.append(mapcontents[0]);
                                            map3contents.append(mapcontents[1]);
                                        } else {
                                            map2contents.append(mapcontents[0]);
                                        }

                                    }
                                }
                                else if ((allotherports[i]).equals(replication_port2)) {
                                    if(!(rec_string.equals("!"))){
                                        String[] mapcontents = rec_string.split("!");
                                        map3contents.append(mapcontents[0]);
                                    }

                                }
                                else if ((allotherports[i]).equals(predecessor_port1)) {
                                    String[] mapcontents = rec_string.split("!");
                                    if (mapcontents.length != 0) {
                                        if (mapcontents.length == 2) {
                                            map2contents.append(mapcontents[0]);
                                            map3contents.append(mapcontents[1]);
                                        } else {
                                            map2contents.append(mapcontents[0]);
                                        }
                                    }
                                }
                                else if ((allotherports[i]).equals(predecessor_port2)) {
                                    if(!(rec_string.equals("!"))){
                                        String[] mapcontents = rec_string.split("!");
                                        map2contents.append(mapcontents[0]);
                                    }
                                }
                            }


                            Log.d("Client-OnCreate", "Inserted contents in mapcontents");

                            in.close();
                            socket.close();

                        }catch (IOException e){
                            e.printStackTrace();
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                    }

                    Log.d("Client-OnCreate","map2contents: "+map2contents.toString());
                    Log.d("Client-OnCreate","map3contents: "+map3contents.toString());

                    sbf.append(map2contents.toString());
                    sbf.append("!");
                    sbf.append(map3contents.toString());


                    String[] twostrings = sbf.toString().split("!");

                    Log.d("Client-OnCreate", "TwoStrings: " + twostrings.toString());


                    if (twostrings.length != 0) {
                        if (twostrings.length == 2) {
                            String string1 = twostrings[0];
                            Log.d("Query@", "String1: " + string1);
                            String[] value3 = string1.split("\\|");
                            Log.d("Query@", "value3: " + value3.length);

                            for (int k = 0; k < value3.length; k++) {
                                Log.d("Client-OnCreate", "Splitting: " + value3[k]);
                                String value4 = value3[k];
                                String[] value5 = value4.split("#");
                                Log.d("Client-OnCreate", "After Splitting: " + value5.toString());
                                Log.d("Client-OnCreate", "Inserting in map2: " + "Key: " + value5[0] + ", val: " + value5[1] + "#" + value5[2]);
                                map2.put(value5[0], value5[1] + "#" + value5[2]);

                            }

                            String string2 = twostrings[1];

                            String[] value6 = string2.split("\\|");

                            for (int l = 0; l < value6.length; l++) {
                                String value7 = value6[l];
                                String[] value8 = value7.split("#");
                                Log.d("Client-OnCreate", "Inserting in map3: " + "Key: " + value8[0] + ", val: " + value8[1] + "#" + value8[2]);
                                map3.put(value8[0], value8[1] + "#" + value8[2]);

                            }
                        } else if (twostrings.length == 1){
                            String string1 = twostrings[0];
                            Log.d("Client-OnCreate", "String1: " + string1);
                            String[] value3 = string1.split("\\|");
                            Log.d("Client-OnCreate", "value3" + value3.length);

                            for (int k = 0; k < value3.length; k++) {
                                Log.d("Client-OnCreate", "Splitting: " + value3[k]);
                                String value4 = value3[k];
                                String[] value5 = value4.split("#");
                                Log.d("Client-OnCreate", "After Splitting: " + value5.toString());
                                Log.d("Client-OnCreate", "Inserting in map2: " + "Key: " + value5[0] + ", val: " + value5[1] + "#" + value5[2]);
                                map2.put(value5[0], value5[1] + "#" + value5[2]);

                            }
                        }
                    }

                    for (String key : map1.keySet()) {
                        uniquekeys.add(key);
                    }
                    for (String key : map2.keySet()) {
                        uniquekeys.add(key);
                    }
                    for (String key : map3.keySet()) {
                        uniquekeys.add(key);
                    }
                    for (String s : uniquekeys) {
                        String valuetoreturn = "";

                        StringBuilder sb2 = new StringBuilder();

                        Log.d("Query", "sb2(1): " + sb2.toString());

                        int flag = 0;

                        if (map1.containsKey(s)) {
                            String val1 = map1.get(s);
                            sb2.append(val1);
                            sb2.append("|");
                            flag = 1;
                        }
                        Log.d("Query", "sb2(2): " + sb2.toString());

                        if (map2.containsKey(s)) {
                            String val2 = map2.get(s);
                            sb2.append(val2);
                            sb2.append("|");
                            if (flag == 1) {
                                flag = 2;
                            } else {
                                flag = 1;
                            }
                        }
                        Log.d("Query", "sb2(3): " + sb2.toString());

                        if (map3.containsKey(s)) {
                            String val3 = map3.get(s);
                            sb2.append(val3);
                            sb2.append("|");

                            if (flag == 2) {
                                flag = 3;
                            } else if (flag == 1) {
                                flag = 2;
                            } else {
                                flag = 1;
                            }
                        }
                        Log.d("Client-OnCreate", "sb2(4): " + sb2.toString());

                        if (flag == 1) {
                            String repval = sb2.toString();
                            sb2.append(repval);
                            Log.d("Client-OnCreate", "sb2(5): " + sb2.toString());
                            sb2.append(repval);
                            Log.d("Client-OnCreate", "sb2(6): " + sb2.toString());
                        } else if (flag == 2) {
                            String[] repval = sb2.toString().split("\\|");
                            sb2.append(repval[0]);
                            Log.d("Client-OnCreate", "sb2(7): " + sb2.toString());
                        }

                        Log.d("Client-OnCreate", "Stringtocompare: " + sb2.toString());

                        valuetoreturn = getLatest(sb2.toString());

                        Log.d("Client-OnCreate", "valuetoreturn: " + valuetoreturn);

                        String keyport = belongsTo(s);
                        String value="";

                        if(keyport.equals(current_port)){
                           // long timestamp = System.currentTimeMillis();
                            value = valuetoreturn +"#"+"O";
                        }
                        else{
                            //long timestamp = System.currentTimeMillis();
                            value = valuetoreturn +"#"+"R:"+keyport;
                        }

                        try {
                            outputStream = getContext().openFileOutput(s, Context.MODE_PRIVATE);
                            outputStream.write(value.getBytes());
                            outputStream.close();
                            Log.d("Client-OnCreate","File write successful in current Node");
                        } catch (Exception e) {
                            Log.e("Client-OnCreate", "File write failed");
                        }
                    }

                    flag = true;
                }

                if(msgs[0].toString().equals("Query@")){

                    BlockingQueue<String> queue3 = (ArrayBlockingQueue<String>)  msgs[3];

                    StringBuilder map2contents = new StringBuilder();
                    StringBuilder map3contents = new StringBuilder();
                    StringBuilder sbf = new StringBuilder();

                    for (int i = 0; i < allotherports.length; i++) {

                        try {

                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(multiplyPortNumBy2(allotherports[i])));

                            String msgToSend = "OnCreate" + "#" + current_port + "#" + "OnCreate";

                            DataOutputStream out =
                                    new DataOutputStream(socket.getOutputStream());
                            out.writeBytes(msgToSend + "\n");
                            out.flush();

                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String rec_string = in.readLine();

                            Log.d("Client-OnCreate","rec_string from "+multiplyPortNumBy2(allotherports[i])+" is: "+rec_string);

                            if(rec_string != null){
                                if ((allotherports[i]).equals(replication_port1)) {
                                    String[] mapcontents = rec_string.split("!");
                                    if (mapcontents.length != 0) {

                                        if (mapcontents.length == 2) {
                                            map2contents.append(mapcontents[0]);
                                            map3contents.append(mapcontents[1]);
                                        } else {
                                            map2contents.append(mapcontents[0]);
                                        }

                                    }
                                }
                                else if ((allotherports[i]).equals(replication_port2)) {
                                    if(!(rec_string.equals("!"))){
                                        String[] mapcontents = rec_string.split("!");
                                        map3contents.append(mapcontents[0]);
                                    }

                                }
                                else if ((allotherports[i]).equals(predecessor_port1)) {
                                    String[] mapcontents = rec_string.split("!");
                                    if (mapcontents.length != 0) {
                                        if (mapcontents.length == 2) {
                                            map2contents.append(mapcontents[0]);
                                            map3contents.append(mapcontents[1]);
                                        } else {
                                            map2contents.append(mapcontents[0]);
                                        }
                                    }
                                }
                                else if ((allotherports[i]).equals(predecessor_port2)) {
                                    if(!(rec_string.equals("!"))){
                                        String[] mapcontents = rec_string.split("!");
                                        map2contents.append(mapcontents[0]);
                                    }
                                }
                            }


                            Log.d("Client-OnCreate", "Inserted contents in mapcontents");

                            in.close();
                            socket.close();

                        }catch (IOException e){
                            e.printStackTrace();
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                    }

                    Log.d("Client-OnCreate","map2contents: "+map2contents.toString());
                    Log.d("Client-OnCreate","map3contents: "+map3contents.toString());

                    sbf.append(map2contents.toString());
                    sbf.append("!");
                    sbf.append(map3contents.toString());

                    try {
                        queue3.put(sbf.toString());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }

                if (msgs[0].toString().equals("Insert")) {

                    String[] otherports = msgs[4].toString().split("#");

                    for(int i=0; i<otherports.length;i++){

                        try {

                            Log.d("Client-Insert", "Creating socket with: " + multiplyPortNumBy2(otherports[i]));

                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(multiplyPortNumBy2(otherports[i])));

                            String msgToSend = msgs[2].toString() + "#" + msgs[3].toString() + "#" + "Insert" + "#" + msgs[1].toString(); //key+val+tag+co-ord port

                            Log.d("Client-Insert", "msgToSend: " + msgToSend);

                            DataOutputStream out =
                                    new DataOutputStream(socket.getOutputStream());
                            out.writeBytes(msgToSend + "\n");
                            out.flush();

                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String rec_string = in.readLine();

                            if(rec_string != null){
                                if (rec_string.equals("Ack")) {
                                    in.close();
                                    socket.close();
                                }
                            }

                        }catch (IOException e){
                            e.printStackTrace();
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                    }
                }

                if (msgs[0].toString().equals("Query")) {

                    String[] otherports = msgs[3].toString().split("#");

                    StringBuilder sbfinal = new StringBuilder();

                   BlockingQueue<String> queue1 = (ArrayBlockingQueue<String>)  msgs[4];


                    for(int i=0; i<otherports.length;i++){

                        try {

                            Log.d("Client-Query", "Creating socket with: " + multiplyPortNumBy2(otherports[i]));

                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(multiplyPortNumBy2(otherports[i])));

                            String msgToSend = msgs[1].toString() + "#" + msgs[2].toString() + "#" + "Query"; //portasking+key+tag

                            Log.d("Client-Query", "msgToSend: " + msgToSend);

                            DataOutputStream out =
                                    new DataOutputStream(socket.getOutputStream());
                            out.writeBytes(msgToSend + "\n");
                            out.flush();

                            Log.d("Client-Query", "Wrote to socket:  waiting for value from: " + otherports[i]);

                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String rec_string = in.readLine();

                            Log.d("Client-Query", "rec_string: " + rec_string);

                            if(rec_string != null){
                                if (rec_string.equals("|")) {

                                } else {
                                    sbfinal.append(rec_string);
                                }
                            }

                            in.close();
                            socket.close();

                        }catch (IOException e){
                            e.printStackTrace();
                        }catch(Exception e){
                            e.printStackTrace();
                        }

                    }

                    Log.d("Client-Query","sbfinal to be populated in queue1: "+sbfinal.toString());

                    try {
                        queue1.put(sbfinal.toString());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if(msgs[0].toString().equals("Query*")){

                    StringBuilder sb = new StringBuilder();

                    BlockingQueue<String> queue2 = (ArrayBlockingQueue<String>)  msgs[3];

                    for(int i =0; i<allotherports.length;i++){

                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(multiplyPortNumBy2(allotherports[i])));

                            String msgToSend = msgs[0].toString() + "#" + msgs[1].toString() + "#" + "Query*";

                            Log.d("Client-Queryfor*", "msgTosend: " + msgToSend);

                            DataOutputStream out =
                                    new DataOutputStream(socket.getOutputStream());
                            out.writeBytes(msgToSend + "\n");
                            out.flush();

                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String rec_string = in.readLine();

                            Log.d("Client-Queryfor*", "rec_string: " + rec_string);

                            if(rec_string != null){

                                if(rec_string.equals("None")){

                                }
                                else{
                                    sb.append(rec_string);
                                }
                            }

                            in.close();
                            socket.close();
                        }catch (IOException e){
                            e.printStackTrace();
                        }catch(Exception e){
                            e.printStackTrace();
                        }

                    }

                    Log.d("Client-Queryfor*","sbf: "+sb.toString());

                    try {
                        queue2.put(sb.toString());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if(msgs[0].toString().equals("DeleteKey")){

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(multiplyPortNumBy2(msgs[2].toString())));

                        String msgToSend = msgs[0].toString() + "#" + msgs[1].toString() + "#" + "DeleteKey";

                        Log.d("Delete-Key", "msgTosend: " + msgToSend);

                        DataOutputStream out =
                                new DataOutputStream(socket.getOutputStream());
                        out.writeBytes(msgToSend + "\n");
                        out.flush();
                        out.close();
                        socket.close();

                }

                if(msgs[0].toString().equals("DeleteKeyR")){

                    for(int i=0; i<2;i++){

                        try {

                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(multiplyPortNumBy2(replicas[i])));

                            String msgToSend = msgs[0].toString() + "#" + msgs[1].toString() + "#" + "DeleteKeyR";

                            Log.d("Delete-Key", "msgTosend: " + msgToSend);

                            DataOutputStream out =
                                    new DataOutputStream(socket.getOutputStream());
                            out.writeBytes(msgToSend + "\n");
                            out.flush();
                            out.close();
                            socket.close();

                        }catch (IOException e){
                            e.printStackTrace();
                        }catch(Exception e){
                            e.printStackTrace();
                        }

                    }
                }

            }catch(UnknownHostException e){
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch(IOException e){
                Log.e(TAG, "ClientTask socket IOException");
            }catch(Exception e){
                e.printStackTrace();
            }

            return null;
        }

    }

}

package edu.buffalo.cse.cse486586.simpledht;


import java.io.ObjectOutputStream;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.net.InetSocketAddress;
import android.content.ContentProvider;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import android.database.Cursor;
import android.net.Uri;
import android.content.Context;
import android.database.MatrixCursor;
import java.net.ServerSocket;
import android.os.SystemClock;
import java.net.InetAddress;
import java.net.Socket;
import android.util.Log;
import java.io.Serializable;
import java.io.IOException;
import android.telephony.TelephonyManager;
import java.io.ObjectInputStream;
import java.io.IOException;
import android.content.ContentValues;


public class SimpleDhtProvider extends ContentProvider {
    static String TAG = SimpleDhtProvider.class.getSimpleName();
    static String[] REMOTE_PORT_ARRAY = {"11108","11112","11116","11120","11124"};
    static Hashtable<String, String> MyDB = new Hashtable<String, String>();
    static int SERVER_PORT = 10000;
    static int FIRST_NODE = 11108;
    String MyNodeHashID;
    String MyPreviousNodeHashID;


    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        InsertIntoRing(values.get("key").toString(), values.get("value").toString());
        return uri;
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if(selection.equals("*")) {
            DeleteAllFromRing(selection, MyPortNumber);
        }
        else if(selection.equals("@"))
        {
            MyDB.clear();
        }
        else
        {
            DeleteSingleValueFromRing(selection, MyPortNumber);
        }
        return 0;
    }


    String MyNextNodeHashID;
    int MyPortNumber;
    int MyPreviousNodePortNumber;
    int MyNextNodePortNumber;
    boolean ReceivedDataFromAllNodes = false;
    Hashtable<String, String> GetDataFromAllNodes=null;
    boolean ReceivedDataForQuery = false;
    String QueryResponse=null;

    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        MyPortNumber = Integer.parseInt(portStr) * 2;


        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            final ServerSocket serverSocket = new ServerSocket(SERVER_PORT);



            Executors.newSingleThreadExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    //Send Join Request to First Node
                    ServerTask(serverSocket);
                }
            });


        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket");
            return true;
        }

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */



        try {
            MyNodeHashID = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "OnCreate Issue");
            return false;
        }


        //initally I point to MySelf
        PointToMyself();

        if (MyPortNumber != FIRST_NODE) {

            Executors.newSingleThreadExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    //Send Join Request to First Node
                    ClientTask(ADD_NODE,0,FIRST_NODE, MyNodeHashID, null, MyPreviousNodePortNumber, MyNextNodePortNumber, null);
                }
            });


        }

        return true;
    }

    final static int NEXT_NODE = 0;
    final static int INSERT = 1;
    final static int SINGLEQUERY = 2;
    final static int RETURN_ALL = 3;
    final static int DELETE = 4;
    final static int DELETE_ALL = 5;
    final static int CLEAN_DB = 6;
    final static int ADD_NODE = 7;
    final static int INSERT_IN_BETWEEN = 8;



    public void PointToMyself() {

        MyPreviousNodePortNumber = MyPortNumber;
        MyNextNodePortNumber = MyPortNumber;
        MyPreviousNodeHashID = MyNodeHashID;
        MyNextNodeHashID = MyNodeHashID;
    }

    public static class Content implements Serializable
    {

        int Type;
        int source;
        String key;
        String Value;
        int PreviousNode;
        int NextNode;
        Hashtable<String, String> thisNodeDB;

        public Content(int Type, int source, String Key, String Value, int Previous, int Next,Hashtable<String, String> DB)
        {
            this.Type = Type;
            this.source=source;
            this.key = Key;
            this.Value = Value;
            this.PreviousNode = Previous;
            this.NextNode = Next;
            this.thisNodeDB = DB;
        }
    }


    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        //Write read data to cursor and return
        MatrixCursor matrix = new MatrixCursor(new String[] { "key", "value"});


      if (selection.equals("@"))
        {
            for(Entry<String, String> x : MyDB.entrySet()) {
                matrix.addRow(new String[]{x.getKey(), x.getValue()});
            }
            return matrix;
        }

        else if (selection.equals("*"))
        {
            if(MyPortNumber == MyNextNodePortNumber) //If Local or there is only 1 Node in Ring
            {
                for(Entry<String, String> x : MyDB.entrySet()) {
                    matrix.addRow(new String[]{x.getKey(), x.getValue()});
                }
                return matrix;
            }
            else
            {
                return ReturnAll(selection, new Hashtable<String, String>(), MyPortNumber);
            }
        }
        else {
            return ReturnSingleKey(selection, MyPortNumber);
        }
    }


    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }


    public void ServerTask(ServerSocket soc) {

        try {
            while(true) {
                Socket socket = soc.accept();
                ObjectInputStream objectInputStream =
                        new ObjectInputStream(socket.getInputStream());
                final Content cont = (Content) objectInputStream.readObject();

                switch (cont.Type) {
                    case SINGLEQUERY:   //When Querying Single Value
                        if (MyPortNumber == cont.source)  //If I looped and come back to myself
                        {
                            ReceivedDataForQuery = true;
                            QueryResponse = cont.Value;
                        }
                        else
                        {  //check with previous Node
                            ReturnSingleKey(cont.key, cont.source);
                        }
                        break;
                    case RETURN_ALL:
                        //If I completerd the circle and came back to myself(Who got query initially: Origin)
                        if (MyPortNumber == cont.source) {
                            ReceivedDataFromAllNodes = true;
                            GetDataFromAllNodes = cont.thisNodeDB;
                        } else {
                            cont.thisNodeDB.putAll(MyDB);
                            final Hashtable<String, String> temp = cont.thisNodeDB;
                            Executors.newSingleThreadExecutor().execute(new Runnable() {
                                @Override
                                public void run() {
                                    ClientTask(RETURN_ALL,cont.source,MyNextNodePortNumber, cont.key, null, cont.PreviousNode, 0, temp);
                                }
                            });

                        }
                        break;
                    case CLEAN_DB:
                        Log.e(TAG,"Clear MyDB for this Node");
                        MyDB.clear();
                        break;
                    case ADD_NODE:
                        AddNode(cont);
                        break;
                    case INSERT_IN_BETWEEN:   //Insert In Between
                        InsertInBetween(cont);
                        break;
                    case NEXT_NODE:
                        PointToNextNode(cont.NextNode);
                        break;
                    case INSERT:
                        InsertIntoRing(cont.key, cont.Value);
                        break;
                    case DELETE:
                        DeleteSingleValueFromRing(cont.key, cont.source);
                        break;
                    case DELETE_ALL:
                        if (MyPortNumber == cont.source)  //If there is only 1 Node in Ring
                        {
                            //I came back to the node which triggered delete all. DOnt Do anything Now
                        }
                        else
                        {
                            DeleteAllFromRing(cont.key, cont.source);
                        }
                        break;
                    default:
                        Log.e(TAG, "Default");
                }
            }
        } catch (UnknownHostException e)
        {Log.e(TAG, "ServerTask UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "ServerTask socket IOException");
        } catch (ClassNotFoundException e) {
            Log.e(TAG, "ClassNotFoundException");
        }
    }





    public void PointToNextNode(int port)
    {
        MyNextNodePortNumber = port;

        String x = String.valueOf(MyNextNodePortNumber);
        try {
            MyNextNodeHashID = genHash(x);
        }catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "issue PointToNextNode");
        }

    }

    public void InsertInBetween(Content cont)
    {
        MyPreviousNodeHashID = cont.key;
        MyNextNodeHashID = cont.Value;
        MyPreviousNodePortNumber = cont.PreviousNode;
        MyNextNodePortNumber = cont.NextNode;
    }

    public void AddNode(final Content cont) {

        if (check(cont.key))
        {
            //This will be first time (When there was only 1 AVD in RING)
            if (ThereIsOnly1NodeInRing())
            {
                InsertForFirstTime(cont);
            }
            else
            //There are more than 1 node .InsertNode In Between
            {
                InsertBetweenMeAndMyPrevious_MyPreviousPointsToThisNewNode(cont);
                SystemClock.sleep(30);
                InsertBetweenMeAndMyPrevious_MyPreviousPointsToNew(cont);
            }
        }
        else {

            //PAss Join Request to My Next Node
            Executors.newSingleThreadExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    ClientTask(cont.Type,0,MyNextNodePortNumber, cont.key, cont.Value, cont.PreviousNode, cont.NextNode,cont.thisNodeDB);
                }
            });

        }
    }


    public void InsertForFirstTime(Content cont) {

        ClientTask(INSERT_IN_BETWEEN,0,cont.PreviousNode, MyPreviousNodeHashID, MyNextNodeHashID, MyPreviousNodePortNumber, MyNextNodePortNumber, null);
        MyPreviousNodeHashID = cont.key;
        MyNextNodeHashID = cont.key;  //As there are total 2 elements next and previous are same
        MyPreviousNodePortNumber = cont.PreviousNode;
        MyNextNodePortNumber = cont.NextNode;
    }


    public boolean ThereIsOnly1NodeInRing() {

        if(MyPortNumber == MyPreviousNodePortNumber)
        {
            return true;
        }
        return false;
    }

    public void InsertBetweenMeAndMyPrevious_MyPreviousPointsToThisNewNode(Content cont) {

        //My Previous Node will Point now to this Node (Successor)
        ClientTask(NEXT_NODE,0,MyPreviousNodePortNumber, null, null, 0, cont.NextNode, null);
    }


    public void InsertBetweenMeAndMyPrevious_MyPreviousPointsToNew(Content cont) {

        //And , Insert this Node Between My Previous Node and Me
        //Previous of New is my previous
        //Next of New is Me
        ClientTask(INSERT_IN_BETWEEN,0,cont.PreviousNode, MyPreviousNodeHashID, MyNodeHashID, MyPreviousNodePortNumber, MyPortNumber, null);
        MyPreviousNodeHashID = cont.key;
        MyPreviousNodePortNumber = cont.PreviousNode;
    }


    public void InsertIntoRing(final String key, final String Value) {
        try {
            String gethash = genHash(key);
            if (check(gethash)) {
                MyDB.put(key, Value);
            }
            else {
                Executors.newSingleThreadExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        ClientTask(INSERT,0,MyNextNodePortNumber, key, Value, 0, 0, null);
                    }
                });


            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Issue InsertIntoRing");
        }
    }



    public void DeleteAllFromRing(final String key, final int NodeWhichReceivedCommadFirst) {

            MyDB.clear();
            Executors.newSingleThreadExecutor().execute(new Runnable()
            {
                @Override
                public void run()
                {
                    ClientTask(DELETE_ALL,NodeWhichReceivedCommadFirst,MyNextNodePortNumber, key, null, 0, 0, null);
                }
            });

    }



    public void DeleteSingleValueFromRing(final String key, final int NodeWhichReceivedCommadFirst)    {

            try {
                String gethash = genHash(key);
                if (check(gethash)) {
                    MyDB.remove(key);
                }
                else {

                    Executors.newSingleThreadExecutor().execute(new Runnable() {
                        @Override
                        public void run() {
                            ClientTask(DELETE,NodeWhichReceivedCommadFirst,MyNextNodePortNumber, key, null, 0, 0, null);
                        }
                    });

                }
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "Issue DeleteSingleValueFromRing");
            }
    }



    public Cursor ReturnAll(final String key, Hashtable<String, String> CombineAllDB, final int NodeWhichReceivedCommadFirst)
    {

        MatrixCursor matrix = new MatrixCursor(new String[] { "key", "value"});

        CombineAllDB.putAll(MyDB);
        final Hashtable<String, String> AllDB =CombineAllDB;
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                ClientTask(RETURN_ALL,NodeWhichReceivedCommadFirst,MyNextNodePortNumber, key, null, 0, 0, AllDB);
            }
        });

        //If this Node is Origin, wait and Get all data from all nodes
        if (MyPortNumber ==NodeWhichReceivedCommadFirst) {
            while (ReceivedDataFromAllNodes == false) {

            }

            ReceivedDataFromAllNodes = false;
            for (Entry<String, String> entry : GetDataFromAllNodes.entrySet()) {
                matrix.addRow(new String[]{entry.getKey(), entry.getValue()});
            }

            GetDataFromAllNodes = null;
        }
        return matrix;
    }




    public Cursor ReturnSingleKey(final String key, final int NodeWhichReceivedCommadFirst) {
        MatrixCursor matrix = new MatrixCursor(new String[] { "key", "value"});


        try {
            String gethash = genHash(key);
            if (check(gethash))
            {
                if (MyPortNumber == NodeWhichReceivedCommadFirst) {   //If I am the Previous Node
                    matrix.addRow(new String[]{key, MyDB.get(key)});
                }
                else
                {

                    Executors.newSingleThreadExecutor().execute(new Runnable() {
                        @Override
                        public void run() {
                            ClientTask(SINGLEQUERY,NodeWhichReceivedCommadFirst,NodeWhichReceivedCommadFirst, null, MyDB.get(key), 0, 0, null);
                        }
                    });

                }
                return matrix;
            }
            else {

                //Send Request to Next Node to Searh for Key
                Executors.newSingleThreadExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        ClientTask(SINGLEQUERY,NodeWhichReceivedCommadFirst,MyNextNodePortNumber, key, null, 0, 0, null);
                    }
                });


                //Loop in Ring and bring and give me value. I will wait till then
                if(MyPortNumber==NodeWhichReceivedCommadFirst) {
                    while (ReceivedDataForQuery == false) {

                    }
                    ReceivedDataForQuery = false;
                    matrix.addRow(new String[]{key, QueryResponse});
                    QueryResponse = null;
                }

                }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Issue ReturnSingleKey");
        }

        return matrix;
    }



    public boolean check(String key) {


        if(IsThereOnlyOneNodeInRing()==true)
        {
            return true;
        }

        if(CornorCase_IfBetweenFirstAndLastOfRing(key)==true)
        {
            return true;
        }


        if(InsertKeyAtCorrectLocation(key)==true)
        {
            return true;
        }

        return false;

    }




    public boolean IsThereOnlyOneNodeInRing() {

        if (MyNodeHashID.compareTo(MyPreviousNodeHashID) == 0)
        {
            return true;
        }
        return false;
    }


    public boolean CornorCase_IfBetweenFirstAndLastOfRing(String key) {

        if ((MyPreviousNodeHashID.compareTo(MyNodeHashID) > 0) && (key.compareTo(MyPreviousNodeHashID) < 0 && key.compareTo(MyNodeHashID) <= 0))
        {
            return true;
        }
        if ((MyPreviousNodeHashID.compareTo(MyNodeHashID) > 0) && (key.compareTo(MyPreviousNodeHashID) > 0 && key.compareTo(MyNodeHashID) > 0))
        {
            return true;
        }
        return false;
    }




    public boolean InsertKeyAtCorrectLocation(String key) {

        //If key is greater than previous and key is less than and weqal to MyNodeHashID (Insert key in betwee)
        if (key.compareTo(MyPreviousNodeHashID) > 0 && key.compareTo(MyNodeHashID) < 1)
        {
            return true;
        }
        return false;
    }

    public void ClientTask(int Type, int source,int fwdMsgToThisNode, String key, String Value, int PreviousNode,int NextNode, Hashtable<String, String> DB)
    {

        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),fwdMsgToThisNode));



            //socket = (InetAddress.getByAddress(new byte[] {10, 0, 2, 2}), fwdMsgToThisNode);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            Content cont = new Content(Type,source,key, Value, PreviousNode, NextNode,DB);
            objectOutputStream.writeObject(cont);


            objectOutputStream.close();
            socket.close();
        }catch (UnknownHostException e) {
            Log.e(TAG, "Client Unknown host exception!");
        } catch (IOException e) {
            Log.e(TAG, "Client Socket IO exception!");
        }
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


}


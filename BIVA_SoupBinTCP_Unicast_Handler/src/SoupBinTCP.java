/*
 * Copyright (C) 2018 Juan J. Martínez
 * 
 * All rights reserved. This complete software or any portion thereof
 * can be used as reference but may not be reproduced in any manner 
 * whatsoever without the express written permission of the owner.
 * 
 * The purpose of this is to be consulted and used as a referece of 
 * functionallyty.
 * 
 * Developed in Mexico City
 * First version, 2018
 *
 */

/**
 *
 * @author Juan J. Martínez
 * @email juanjmtzs@gmail.com
 * @phone +52-1-55-1247-8044
 * @linkedin https://www.linkedin.com/in/juanjmtzs/
 *
 */
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

public class SoupBinTCP {

    private final int USERNAME_LENGTH = 6;
    private final int PASSWORD_LENGTH = 10;
    private final int SESSION_LENGTH = 10;
    private final int SEQUENCE_NUMBER_LENGTH = 20;
    private final int LOGIN_REQUEST_LENGTH = 49;
    private final int LOGIN_LENGTH = USERNAME_LENGTH + PASSWORD_LENGTH + SESSION_LENGTH + SEQUENCE_NUMBER_LENGTH;
    private final int LOGIN_ACCEPT_LENGTH = SESSION_LENGTH + SEQUENCE_NUMBER_LENGTH;
    private static final int LOGOUT_LENGTH = 3;
    private static final int CLIENT_HEART_LENGTH = 3;
    private final static int CLIENT_HEARTBEAT_INTERVAL = 1000;
    private final String Username;
    private final String Password;
    private final String Session;
    private BigInteger SequenceNumber;
    private static long nextExpectedSequenceNumber = 0;
    private static ByteBuffer buffer;
    private long lastHeartbeatMillis = 0;
    private static Socket clientSocket;
    private static volatile boolean connected = false;
    private static int heart = 0;
    final private Potocol protocol;
    private static String session;
    private static String service;

    public SoupBinTCP() {
        Username = null;
        Password = null;
        Session = null;
        SequenceNumber = null;
        protocol = null;
    }

    public SoupBinTCP(String username, String password, String session, String host, int port, long sequenceNumber, Potocol protocol) {
        Username = username;
        Password = password;
        Session = session;
        try {
            clientSocket.connect(new InetSocketAddress(host, port), 25200000);
        } catch (IOException ex) {
            System.out.println(ex);
        }
        connected = true;
        SequenceNumber = BigInteger.valueOf(sequenceNumber);
        buffer.order(ByteOrder.BIG_ENDIAN);
        this.protocol = protocol;
    }

    private static class PacketType {

        public static final byte login_accepted_type = 'A';
        public static final byte server_heartbeat_type = 'H';
        public static final byte login_rejected_type = 'J';
        public static final byte login_request_type = 'L';
        public static final byte logout_request_type = 'O';
        public static final byte client_heartbeat_type = 'R';
        public static final byte sequenced_data_type = 'S';
        public static final byte unsequenced_data_type = 'U';
        public static final byte end_of_session_type = 'Z';
    }

    private class LoginReject {

        public static final byte not_authorised = 'A';
        public static final byte session_unavailable = 'S';
    }

    private static class Packet {

        private final Charset DEFAULT_CHARSET = Charset.forName("US-ASCII");
        private static final byte PADDING = ' ';
        private byte type;
        private int length = 1;

        Packet(byte type, int length) {
            this.type = type;
            this.length += length;
        }

        Packet(byte type) {
            this.type = type;
        }

        void padRight(ByteBuffer buffer, String s, long length) {
            buffer.put(s.getBytes(DEFAULT_CHARSET));
            for (int i = s.length(); i < length; ++i) {
                buffer.put(PADDING);
            }
        }

        void padLeft(ByteBuffer buffer, String s, long length) {
            for (int i = s.length(); i < length; ++i) {
                buffer.put(PADDING);
            }
            buffer.put(s.getBytes(DEFAULT_CHARSET));
        }

        String stripLeft(byte[] buffer, int offset, int length) {
            int i = offset;
            for (; i < length + offset; i++) {
                if (buffer[i] != PADDING) {
                    break;
                }
            }
            return new String(buffer, i, length - (i - offset), DEFAULT_CHARSET);
        }

        String stripRight(byte[] buffer, int offset, int length) {
            int i = offset + length - 1;
            for (; i >= offset; i--) {
                if (buffer[i] != PADDING) {
                    break;
                }
            }
            return new String(buffer, offset, i - offset + 1, DEFAULT_CHARSET);
        }

        void encode(ByteBuffer buffer) {
            buffer.putShort((short) length);
            buffer.put(type);
        }

        void decode(byte[] buffer) {
        }
    }

    public static void closeSocket() {
        if (clientSocket != null) {
            try {
                clientSocket.close();
            } catch (IOException ignore) {

                String messageError = "Socket closed" + "\n" + ignore;

                System.out.println(messageError);
            }

        }
    }

    private static void sendBuffer() throws IOException {

        send(buffer);
    }

    private static void send(ByteBuffer b) throws IOException {
        clientSocket.getOutputStream().write(b.array(), 0, b.position());
        clientSocket.getOutputStream().flush();
        b.clear();
    }

    private short readBytes(DataInputStream in) throws IOException {
        for (;;) {
            try {
                if (System.currentTimeMillis() - lastHeartbeatMillis > CLIENT_HEARTBEAT_INTERVAL) {
                    final Packet clientHeartBeat = new Packet(PacketType.client_heartbeat_type);
                    final ByteBuffer ByteBufferAux;
                    ByteBufferAux = ByteBuffer.allocate(CLIENT_HEART_LENGTH);
                    clientHeartBeat.encode(ByteBufferAux);

                    send(ByteBufferAux);
                    lastHeartbeatMillis = System.currentTimeMillis();
                }

                return in.readShort();
            } catch (SocketTimeoutException e) {

            }
        }
    }

    private void loopFactory() throws IOException {
        final DataInputStream in = new DataInputStream(clientSocket.getInputStream());
        while (connected == true) {

            buffer.clear();
            clientSocket.setSoTimeout(1000);
            short len = readBytes(in);
            clientSocket.setSoTimeout(0);
            byte type = in.readByte();
            for (short numBytes = 0; numBytes < len - 1;) {
                numBytes += in.read(buffer.array(), numBytes, len - numBytes - 1);
            }
            buffer.position(0);
            buffer.limit(len - 1);
            switch (type) {
                case PacketType.sequenced_data_type:

                    String JSON = protocol.parse(buffer, nextExpectedSequenceNumber);
                    System.out.println("," + JSON);
                    heart = 0;
                    ++nextExpectedSequenceNumber;

                    break;
                case PacketType.login_accepted_type:
                    loginAccepted();
                    break;
                case PacketType.login_rejected_type:
                    System.out.print("[ITCH-U]Login Rejected: ");
                    loginRejected();
                    return;
                case PacketType.server_heartbeat_type:
                    if (heart == 0) {

                    }
                    heart = 1;

                    break;
                case PacketType.end_of_session_type:
                    System.out.println("[SoupBinTCP-IN]End of Session");
                    logoff();

                    Runtime garbage1 = Runtime.getRuntime();
                    garbage1.gc();
                    break;
                case PacketType.unsequenced_data_type:
                    System.out.println("[SoupBinTCP-IN]Unsequenced Packet");
                    heart = 0;
                    break;
                default:
                    break;
            }
        }
    }

    @SuppressWarnings("empty-statement")
    private void loginAccepted() {
        class LoginAccepted extends Packet {

            public LoginAccepted() {
                super(PacketType.login_accepted_type, LOGIN_ACCEPT_LENGTH);
            }

            @Override
            void decode(byte[] buffer) {
                session = stripRight(buffer, 0, SESSION_LENGTH);
                String sequenceNumber = stripLeft(buffer, SESSION_LENGTH, SEQUENCE_NUMBER_LENGTH);
                System.out.print("[{\"Service Name\":\"" + service + "\",");
                System.out.print("\"Session\":" + session + ",");
                System.out.println("\"Next SeqNum\":" + sequenceNumber + "}");

                nextExpectedSequenceNumber = Long.parseLong(sequenceNumber);
            }
        };
        new LoginAccepted().decode(buffer.array());

    }

    private void loginRejected() {
        byte reason = buffer.get();
        String rejection = "";
        switch (reason) {
            case LoginReject.not_authorised:
                rejection = "Login Information is not recognized: Please verify User, Password, IP and Port values";
                break;
            case LoginReject.session_unavailable:
                rejection = "Session " + Session + " is not available";
                break;
            default:
                rejection = "Unsupported Rejection";
                break;
        }
        System.out.println("Login Rejected: " + rejection);
    }

    private void login() throws IOException {
        final Packet login = new Packet(PacketType.login_request_type, LOGIN_LENGTH);

        final ByteBuffer ByteBufferAux;
        ByteBufferAux = ByteBuffer.allocate(LOGIN_REQUEST_LENGTH);

        login.encode(ByteBufferAux);
        login.padRight(ByteBufferAux, Username, USERNAME_LENGTH);
        login.padRight(ByteBufferAux, Password, PASSWORD_LENGTH);
        if (Session != null) {
            login.padRight(ByteBufferAux, Session, SESSION_LENGTH);
        } else {
            login.padRight(ByteBufferAux, "", SESSION_LENGTH);
        }
        login.padLeft(ByteBufferAux, SequenceNumber.toString(), SEQUENCE_NUMBER_LENGTH);

        System.out.println("[SoupBinTCP-OUT] Logon");
        send(ByteBufferAux);
        lastHeartbeatMillis = System.currentTimeMillis();
    }

    private static void logoff() {
        try {
            final Packet logoff = new Packet(PacketType.logout_request_type);
            final ByteBuffer ByteBufferAux;
            ByteBufferAux = ByteBuffer.allocate(LOGOUT_LENGTH);
            logoff.encode(ByteBufferAux);
            System.out.println("[SoupBinTCP-OUT] Logoff");
            send(ByteBufferAux);

        } catch (IOException e) {

            String messageError = "Socket closed" + "\n" + e;

            System.out.println(messageError);

        }
    }

    public void start() throws IOException {
        login();
        loopFactory();
    }

    public static void stop() {
        connected = false;
        logoff();

        if (nextExpectedSequenceNumber > 0) {

            System.out.println("Disconnected session with last sequence number: " + nextExpectedSequenceNumber);

        }

    }

    public static void usageInstructions() {
        System.out.println("Usage: java -jar $myJar $username $password $session $hostname $port $seqnum");
        System.out.println("");
        System.out.println("   username  - user id associated with the SOUP port (6 characters right padded with spaces)");
        System.out.println("   password  - password for the SOUP port to which this test client is connecting (10 characters right padded with spaces)");
        System.out.println("   hostname  - hostname of the SOUP server");
        System.out.println("   port      - port to connect on the SOUP host");
        System.out.println("   seqnum    - next expected sequence number for this session, a value 1 is start of session and 0 the most recently generated message");
    }

    public static void kill() {

        stop();
        closeSocket();

    }

    public static void StartHandling(String[] args) throws IOException {
        clientSocket = new Socket();
        buffer = ByteBuffer.allocate(4 * 1024);

        if (args.length != 6) {
            usageInstructions();
            args = new String[6];
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("   Enter Username:");
            args[0] = br.readLine();
            System.out.println("   Enter Password:");
            args[1] = br.readLine();
            System.out.println("   Enter Hostname:");
            args[2] = br.readLine();
            System.out.println("   Enter Port:");
            args[3] = br.readLine();
            System.out.println("   Enter SeqNum:");
            args[4] = br.readLine();
            System.out.println("   Enter Folder:");
            args[5] = br.readLine();

        }
        try {

            try {
                DateFormat dateFormat = new SimpleDateFormat("ddMMyyyy");
                Date date = new Date();
                System.setOut(new PrintStream(new File(args[5], Instant.now().toEpochMilli() + "_BIVAUnicast_" + dateFormat.format(date) + ".log")));
            } catch (Exception e) {
                System.out.println("Error " + e);

            }

            final SoupBinTCP s = new SoupBinTCP(args[0], args[1], " ", args[2], Integer.parseInt(args[3]), Long.parseLong(args[4]), new DecoderITCHBIVA());

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    s.stop();
                }
            });
            s.start();
        } catch (IOException | NumberFormatException e) {

            String messageError = "While Running: " + "\n\n" + e.getMessage();

            if (e.getMessage() == "null") {

                System.out.println(messageError);

            } else if ((e + "").equals("java.net.ConnectException: Connection refused: connect")) {
                messageError = "Something Went Wrong: " + "\n\n" + "Connection Refused, Please Verify.";
                System.out.println(messageError);
            }
            closeSocket();
        }
    }

    public static void gapFilled() {

        stopGapFilled();
        closeSocket();

    }

    public static void stopGapFilled() {
        connected = false;
        logoff();
        if (nextExpectedSequenceNumber > 0) {

            System.out.println("Snapshot Completed.");

        }

    }

}

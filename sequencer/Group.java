package sequencer;

import java.net.*;
import java.util.*;
import java.io.*;
import java.rmi.*;

public class Group implements Runnable {
    // declaring necessary variables here
    protected InetAddress grpIpAddress;
    protected int port = 4446;
    protected boolean status;
    MulticastSocket multicastSock;
    DatagramPacket packet;
    DatagramSocket socket;
    InetAddress lclhost;
    String myAddr;
    Sequencer sequencer;
    SequencerImpl sequencerimp;
    MsgHandler handler;
    int maxbuf = 1024;
    Thread heartBeater;
    long lastSequenceRecd;
    long lastSequenceSent;
    long lastSendTime;
    String myName;

    public Group(String host, MsgHandler handler, String senderName)
            throws GroupException, NotBoundException, SequencerException, IOException {
        lastSequenceRecd = -1L;
        lastSequenceSent = -1L;

        /*
         * Get any object with caption MySequencer from the RMI registry
         * must be of type - interface sequencer
         */
        sequencer = (Sequencer) Naming.lookup("/MySequencer");

        // get the address of the current host
        lclhost = InetAddress.getLocalHost();

        // combine the localhost address with sender's name
        myAddr = senderName + lclhost;

        /*
         * fetch group IP address from `join` in sequencer implementation
         * as well as the last received sequence number
         */
        SequencerJoinInfo info = sequencer.join(myAddr);
        grpIpAddress = info.addr;
        lastSequenceRecd = info.sequence;

        // create a multicast socket where to listen
        multicastSock = new MulticastSocket(4446);

        // allow group join next
        multicastSock.joinGroup(new InetSocketAddress(grpIpAddress, port),
                NetworkInterface.getByInetAddress(InetAddress.getLocalHost()));

        this.handler = handler;

        // create a thread
        Thread myThread = new Thread(this);

        // connect to the run method
        myThread.start();
        heartBeater = new HeartBeater(5);
        heartBeater.start();
    }

    public void send(byte[] msg) throws GroupException {
        // send the given message to all instances of Group using the same sequencer
        // check for an available multicast socket
        if (multicastSock != null) {
            try {
                // sending message to the sequencer so that its marshalled
                lastSequenceSent = lastSequenceSent++;

                System.out.println(
                        "Message being passed to sequencer has " + myAddr + "," + msg + "," + lastSequenceSent);

                sequencer.send(myAddr, msg, lastSequenceSent, lastSequenceRecd);

                // changing last send time to long
                lastSendTime = (new Date()).getTime();
            } catch (Exception e) {
                System.out.println("Couldnt contact sequencer because of " + e);
                throw new GroupException("Failed to send message to sequencer");
            }
        } else {
            throw new GroupException("Failed to join group");
        }
    }

    public void leave() throws GroupException {
        // leave group
        // check for available multicast socket group
        if (multicastSock != null) {
            try {
                socket = new DatagramSocket();
                multicastSock.leaveGroup(new InetSocketAddress(grpIpAddress, port),
                        NetworkInterface.getByInetAddress(InetAddress.getLocalHost()));
            } catch (Exception e) {
                throw new GroupException("Failed to leave group");
            }
        }
    }

    public void run() {
        // repeatedly: listen to MulticastSocket created in constructor, and on receipt
        // of a datagram call "handle" on the instance
        // of Group.MsgHandler which was supplied to the constructor
        try {
            while (true) {
                System.out.println("If you can see this, it means our thread is running");
                byte[] buffer = new byte[maxbuf];

                // create the datagram packet to recieve the multicast message
                packet = new DatagramPacket(buffer, buffer.length);
                multicastSock.receive(packet);

                // unmashalling here
                ByteArrayInputStream bstream = new ByteArrayInputStream(buffer, 0, packet.getLength());
                DataInputStream dstream = new DataInputStream(bstream);
                long gotSequence = dstream.readLong();
                int count = dstream.read(buffer);
                long wantSeq = lastSequenceRecd + 1L;
                if (lastSequenceRecd >= 0 && wantSeq < gotSequence) {
                    for (long getSeq = wantSeq; getSeq < gotSequence; getSeq++) {
                        byte[] extra = sequencer.getMissing(myAddr, getSeq);
                        int countExtra = extra.length;
                        System.out.println("Get missing sequence number" + getSeq);
                        handler.handle(countExtra, extra);
                    }
                }
                lastSequenceRecd = gotSequence;
                handler.handle(count, buffer);
            }
        } catch (Exception e) {
            System.out.println("Error encountered in Thread process " + e);
        }

    }

    public interface MsgHandler {
        public void handle(int count, byte[] msg);
    }

    public class GroupException extends Exception {
        public GroupException(String s) {
            super(s);
        }
    }

    public class HeartBeater extends Thread {
        // This thread sends heartbeat messages when required
        public void run() {
            do
                try {
                    do
                        Thread.sleep(period * 1000);
                    while ((new Date()).getTime() - lastSendTime < (long) (period * 1000));
                    sequencer.heartbeat(myName, lastSequenceRecd);

                } catch (Exception error) {
                }
            while (true);
        }

        int period;

        public HeartBeater(int period) {
            this.period = period;
        }
    }
}

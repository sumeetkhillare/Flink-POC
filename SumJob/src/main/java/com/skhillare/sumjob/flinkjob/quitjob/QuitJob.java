package com.skhillare.sumjob.flinkjob.quitjob;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

public class QuitJob extends Exception{
    public QuitJob(String m1, String inetAddress, int port) throws IOException {
        super(m1);
        //Close running server
        Socket socket = new Socket();
        SocketAddress socketAddress=new InetSocketAddress(inetAddress, port);
        socket.bind(socketAddress);
        socket.close();
    }
}

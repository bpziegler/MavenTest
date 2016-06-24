package com.qualia.hbasetest;
//
//
//import java.io.UnsupportedEncodingException;
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
//import org.apache.hadoop.hbase.thrift.generated.BatchMutation;
//import org.apache.hadoop.hbase.thrift.generated.Hbase;
//import org.apache.hadoop.hbase.thrift.generated.IOError;
//import org.apache.hadoop.hbase.thrift.generated.Mutation;
//import org.apache.thrift.TException;
//import org.apache.thrift.protocol.TBinaryProtocol;
//import org.apache.thrift.protocol.TProtocol;
//import org.apache.thrift.transport.TSocket;
//import org.apache.thrift.transport.TTransport;
//
//
//public class TestHBase {
//
//    // Helper to translate strings to UTF8 bytes
//    private static ByteBuffer bytes(String s) {
//        try {
//            return ByteBuffer.wrap(s.getBytes("UTF-8"));
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//            return null;
//        }
//    }
//
//
//    public static void main(String[] args) throws IOError, TException {
//        System.out.println("Hello");
//
//        TTransport transport = new TSocket("localhost", 9090);
//        TProtocol protocol = new TBinaryProtocol(transport, true, true);
//        Hbase.Client client = new Hbase.Client(protocol);
//
//        transport.open();
//
//        System.out.println("scanning tables...");
//        List<ByteBuffer> result = client.getTableNames();
//        for (ByteBuffer buffer : result) {
//            String s = new String(buffer.array());
//            System.out.println(s);
//        }
//
//        ByteBuffer testTable = bytes("test");
//        ByteBuffer testKey = bytes("key1");
//        ByteBuffer testString = bytes("This is a test string");
//
//        BatchMutation batchMutation = new BatchMutation();
//
//        List<Mutation> mutations;
//        mutations = new ArrayList<Mutation>();
//        Mutation mutation = new Mutation(false, bytes("f1:col"), bytes("value1"), false);
//        mutations.add(mutation);
//        batchMutation.addToMutations(mutation);
//        batchMutation.setRow(testKey);
//        List<BatchMutation> batchList = Arrays.asList(batchMutation);
//        client.mutateRow(testTable, testKey, mutations, null);
//        client.mutateRows(testTable, batchList, null);
//
//        transport.close();
//    }
//
//}

package com.qualia.map;


import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Point;
import java.awt.Polygon;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ComponentEvent;
import java.awt.event.ComponentListener;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.Timer;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.io.Files;


public class MapPanel extends JPanel implements ComponentListener {

    private static final long serialVersionUID = 5264589271178258155L;

    private static final int POINT_RADIUS = 3;

    private ObjectMapper mapper = new ObjectMapper();
    private int frame;

    private Timer timer;
    private long startTime = System.currentTimeMillis();

    private Map<String, Polygon> polygons = new HashMap<String, Polygon>();
    private List<Point> points = new ArrayList<Point>();

    private Splitter splitter = Splitter.on(",");

    private Double minLat;
    private Double maxLat;
    private Double minLong;
    private Double maxLong;

    private ArrayNode stateAry;


    public MapPanel() {
        super();
        timer = new Timer(1000 / 60, new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                repaint();
            }
        });
        timer.start();
        loadStates();
        addComponentListener(this);
    }


    private void loadPoints() {
        points.clear();

        int width = getWidth();
        int height = getHeight();

        try {
            List<String> lines = Files.readLines(new File("points.txt"), Charsets.UTF_8);
            for (String oneLine : lines) {
                List<String> parts = splitter.splitToList(oneLine);
                double lat = Double.valueOf(parts.get(0));
                double lng = Double.valueOf(parts.get(1));
                int x = fixPoint(lng, minLong, maxLong, width);
                int y = height - fixPoint(lat, minLat, maxLat, height);
                Point point = new Point(x, y);
                points.add(point);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private void loadStates() {
        try {
            String jsonStr = Files.toString(new File("states.json"), Charsets.UTF_8);
            JsonNode jsonTree = mapper.readTree(jsonStr);
            stateAry = (ArrayNode) jsonTree.get("states").get("state");
            calcBounds(stateAry);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private void calcBounds(ArrayNode stateAry) {
        List<Double> lats = new ArrayList<Double>();
        List<Double> longs = new ArrayList<Double>();

        for (JsonNode oneState : stateAry) {
            String stateName = oneState.get("name").asText();
            if (stateName.equals("Hawaii") || stateName.equals("Alaska"))
                continue;
            ArrayNode pointAry = (ArrayNode) oneState.get("point");
            for (JsonNode onePoint : pointAry) {
                double lat = onePoint.get("lat").asDouble();
                double lng = onePoint.get("lng").asDouble();
                lats.add(lat);
                longs.add(lng);
            }
        }

        minLat = Collections.min(lats);
        maxLat = Collections.max(lats);
        minLong = Collections.min(longs);
        maxLong = Collections.max(longs);

        System.out.println("minLat = " + minLat);
        System.out.println("maxLat = " + maxLat);
        System.out.println("minLong = " + minLong);
        System.out.println("maxLong = " + maxLong);
    }


    private void createPolygons(ArrayNode stateAry) {
        polygons.clear();

        int width = getWidth();
        int height = getHeight();

        for (JsonNode oneState : stateAry) {
            // System.out.println(oneState.toString());
            String stateName = oneState.get("name").asText();
            if (stateName.equals("Hawaii") || stateName.equals("Alaska"))
                continue;
            ArrayNode pointAry = (ArrayNode) oneState.get("point");
            Polygon polygon = new Polygon();
            for (JsonNode onePoint : pointAry) {
                double lat = onePoint.get("lat").asDouble();
                double lng = onePoint.get("lng").asDouble();
                int x = fixPoint(lng, minLong, maxLong, width);
                int y = height - fixPoint(lat, minLat, maxLat, height);
                polygon.addPoint(x, y);
            }
            polygons.put(stateName, polygon);
        }
    }


    private int fixPoint(double value, Double min, Double max, int newLength) {
        return (int) ((value - min) / (max - min) * newLength);
    }


    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);

        frame++;

        long elap = System.currentTimeMillis() - startTime;

        String s = "Frame " + frame;
        g.setColor(Color.BLACK);
        // g.drawString(s, 16, 16);

        for (Polygon polygon : polygons.values()) {
            g.setColor(Color.BLUE);
            g.drawPolygon(polygon);
        }

        for (Point point : points) {
            g.setColor(Color.RED);
            g.drawOval(point.x-1, point.y-1, POINT_RADIUS, POINT_RADIUS);
        }
    }


    private static void createAndShowGUI() {
        // Create and set up the window.
        JFrame frame = new JFrame("Qualia Map");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        MapPanel mapPanel = new MapPanel();
        mapPanel.setPreferredSize(new Dimension(1024, 768));
        frame.getContentPane().add(mapPanel);

        // Display the window.
        frame.pack();
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);
    }


    public static void main(String[] args) {
        // Schedule a job for the event-dispatching thread:
        // creating and showing this application's GUI.
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                createAndShowGUI();
            }
        });
    }


    @Override
    public void componentResized(ComponentEvent e) {
        createPolygons(stateAry);
        loadPoints();
    }


    @Override
    public void componentMoved(ComponentEvent e) {
    }


    @Override
    public void componentShown(ComponentEvent e) {
    }


    @Override
    public void componentHidden(ComponentEvent e) {
    }

}

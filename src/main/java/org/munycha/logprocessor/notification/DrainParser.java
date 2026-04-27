package org.munycha.logprocessor.notification;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class DrainParser {

    private static final String WILDCARD = "<*>";
    private static final Pattern NUMBER_TOKEN = Pattern.compile("^\\d+$");

    private final double simThreshold;
    private final int maxChildren;
    private final int maxClusters;

    // length → firstToken → list of clusters
    private final Map<Integer, Map<String, List<LogCluster>>> root = new HashMap<>();
    private int clusterCount = 0;

    public DrainParser() {
        this(0.5, 100, 1000);
    }

    public DrainParser(double simThreshold, int maxChildren, int maxClusters) {
        this.simThreshold = simThreshold;
        this.maxChildren = maxChildren;
        this.maxClusters = maxClusters;
    }

    public String parseTemplate(String message) {
        String[] tokens = message.split("\\s+");
        int length = tokens.length;

        Map<String, List<LogCluster>> lengthGroup =
                root.computeIfAbsent(length, k -> new HashMap<>());

        String firstKey = isNumber(tokens[0]) ? WILDCARD : tokens[0];
        if (!lengthGroup.containsKey(firstKey) && lengthGroup.size() >= maxChildren) {
            firstKey = WILDCARD;
        }

        List<LogCluster> clusters =
                lengthGroup.computeIfAbsent(firstKey, k -> new ArrayList<>());

        LogCluster best = null;
        double bestSim = -1;
        for (LogCluster cluster : clusters) {
            double sim = similarity(tokens, cluster.template);
            if (sim > bestSim) {
                bestSim = sim;
                best = cluster;
            }
        }

        if (best != null && bestSim >= simThreshold) {
            updateTemplate(best.template, tokens);
            return String.join(" ", best.template);
        }

        if (clusterCount < maxClusters) {
            LogCluster newCluster = new LogCluster(Arrays.copyOf(tokens, tokens.length));
            clusters.add(newCluster);
            clusterCount++;
            return String.join(" ", newCluster.template);
        }

        return WILDCARD;
    }

    private double similarity(String[] tokens, String[] template) {
        int matches = 0;
        for (int i = 0; i < template.length; i++) {
            if (!template[i].equals(WILDCARD) && template[i].equals(tokens[i])) {
                matches++;
            }
        }
        return (double) matches / template.length;
    }

    private void updateTemplate(String[] template, String[] tokens) {
        for (int i = 0; i < template.length; i++) {
            if (!template[i].equals(WILDCARD) && !template[i].equals(tokens[i])) {
                template[i] = WILDCARD;
            }
        }
    }

    private boolean isNumber(String token) {
        return NUMBER_TOKEN.matcher(token).matches();
    }

    private static class LogCluster {
        String[] template;

        LogCluster(String[] template) {
            this.template = template;
        }
    }
}

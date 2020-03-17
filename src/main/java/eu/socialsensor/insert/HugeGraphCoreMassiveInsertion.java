/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package eu.socialsensor.insert;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import eu.socialsensor.utils.TaiShiDataUtils;
import eu.socialsensor.utils.Utils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.T;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;

import eu.socialsensor.graphdatabases.HugeGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.jetty.util.ConcurrentHashSet;

import javax.annotation.Nullable;

public class HugeGraphCoreMassiveInsertion extends InsertionBase<Integer,String> {

    private static final int EDGE_BATCH_NUMBER = 500;

    private ExecutorService pool = Executors.newFixedThreadPool(8);

    private Set<Integer> allVertices = new HashSet<>();
    private Set<String> allvertice2 = new HashSet<>();
    private List<Triple<String,String, Map<String,Object>>> vertices2;
    private List<Integer> vertices;
    private List<Pair<Integer, Integer>> edges;
    private List<Triple<Pair<String,String>,String, Map<String,Object>>> edges2;

    private final HugeGraph graph;
    private final VertexLabel vl;

    public HugeGraphCoreMassiveInsertion(HugeGraph graph) {
        super(GraphDatabaseType.HUGEGRAPH_CORE, null);
        this.graph = graph;
        this.vl = this.graph.vertexLabel(HugeGraphDatabase.NODE);
        this.reset();
        reset2();
    }

    private void reset() {
        this.vertices = new ArrayList<>();
        this.edges = new ArrayList<>(EDGE_BATCH_NUMBER);
    }

    private void reset2()
    {
        this.vertices2 = new ArrayList<>();
        this.edges2 = new ArrayList<>(EDGE_BATCH_NUMBER);
    }

    @Override
    protected Integer getOrCreate(String value) {
        Integer v = Integer.valueOf(value);

        if (!this.allVertices.contains(v)) {
            this.allVertices.add(v);
            this.vertices.add(v);
        }
        return v;
    }

    @Override
    protected void relateNodes(Integer src, Integer dest) {
        this.edges.add(Pair.of(src, dest));
        if (this.edges.size() >= EDGE_BATCH_NUMBER) {
            this.batchCommit();
            this.reset();
        }
    }

    private void batchCommit() {
        List<Integer> vertices = this.vertices;
        List<Pair<Integer, Integer>> edges = this.edges;

        this.pool.submit(() -> {
            for (Integer v : vertices) {
                this.graph.addVertex(T.id, v, T.label, HugeGraphDatabase.NODE);
            }
            HugeVertex source;
            HugeVertex target;
            for (Pair<Integer, Integer> e: edges) {
                source = new HugeVertex(this.graph,
                                        IdGenerator.of(e.getLeft()), this.vl);
                target = new HugeVertex(this.graph,
                                        IdGenerator.of(e.getRight()), this.vl);
                source.addEdge(HugeGraphDatabase.SIMILAR, target);
            }
            this.graph.tx().commit();
        });
    }

    @Override
    protected void post() {
        batchCommit2();
        reset2();
        batchCommit();
        reset();

        this.pool.shutdown();
        try {
            this.pool.awaitTermination(60 * 5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 加载自定义类型数据
     * 使用需自行实现
     * 如果id="n"，则没有id,使用list中的偏移量作为临时id
     * @param id
     * @param properties
     * @return
     */
    @Override
    public String getOrCreateCust(String label, @Nullable String id, Map<String,Object> properties)
    {
        if (Objects.isNull(id)) {
            this.vertices2.add(Triple.of(label, "n", properties));
            return String.valueOf(vertices2.size() - 1); //index of the list
        }
        if (!this.allvertice2.contains(id)) {
            this.allvertice2.add(id);
            this.vertices2.add(Triple.of(label, id, properties));
        }
        return id;
    }
    /**
     * 加载自定义类型数据
     * 使用需自行实现
     * 如果properties=null,则没有属性写入
     * @param src
     * @param dest
     * @param properties
     */
    @Override
    public void relateNodesCust(String label,final String src, final String dest,Map<String,Object> properties)
    {
        this.edges2.add(Triple.of(Pair.of(src, dest), label, properties));
        if (this.edges2.size() >= EDGE_BATCH_NUMBER) {
            this.batchCommit2();
            this.reset2();
        }
    }

    private ConcurrentHashSet<String> edgeCache = new ConcurrentHashSet<>();
    private void batchCommit2() {
        List<Triple<String,String, Map<String,Object>>> vertices = this.vertices2;
        List<Triple<Pair<String,String>,String, Map<String,Object>>> edges = this.edges2;

        this.pool.submit(() -> {
            Map<String, HugeVertex> cache = new HashMap<>();
            int i=0;
            for (Triple<String,String, Map<String,Object>> v : vertices) {
                if (v.getMiddle().equals("n")) {
                    HugeVertex vertex = (HugeVertex)this.graph.addVertex(
                            T.label, v.getLeft(), Utils.mapTopair(v.getRight()));
                    //index of the list
                    cache.put(String.valueOf(i), vertex);
                } else {
                    this.graph.addVertex(
                            T.id, v.getMiddle(),
                            T.label, v.getLeft(),
                            Utils.mapTopair(v.getRight()));
                }
                i++;
            }
            HugeVertex source;
            HugeVertex target;
            for (Triple<Pair<String,String>,String, Map<String,Object>> e: edges) {
                Pair<String, String> srcTarget = e.getLeft();
                String label = e.getMiddle();
                if (cache.containsKey(srcTarget.getLeft())) {
                    //系统自动创建id
                    source = cache.get(srcTarget.getLeft());
                } else {
                    //自定义id
                    source = new HugeVertex(
                            this.graph,
                            IdGenerator.of(srcTarget.getLeft()),
                            this.graph.vertexLabel(
                                    TaiShiDataUtils.getVertexLabel(label,true)));
                }

                if (cache.containsKey(srcTarget.getRight())) {
                    target = cache.get(srcTarget.getRight());
                } else {
                    target = new HugeVertex(
                            this.graph,
                            IdGenerator.of(srcTarget.getRight()),
                            this.graph.vertexLabel(
                                    TaiShiDataUtils.getVertexLabel(label, false)));
                }
                String srcTargetIdStr = source.id() + ">" + target.id();
                if (!edgeCache.contains(srcTargetIdStr)) {
                    source.addEdge(label, target,Utils.mapTopair(e.getRight()));
                    edgeCache.add(srcTargetIdStr);
                }
            }
            this.graph.tx().commit();
        });
    }
}

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

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import com.baidu.hugegraph.structure.HugeVertex;
import eu.socialsensor.utils.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeGraph;
import eu.socialsensor.graphdatabases.HugeGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.HugeGraphUtils;

import javax.annotation.Nullable;

public class HugeGraphCoreSingleInsertion extends InsertionBase<Vertex,Vertex> {

    private final HugeGraph graph;

    public HugeGraphCoreSingleInsertion(HugeGraph graph, File resultPath) {
        super(GraphDatabaseType.HUGEGRAPH_CORE, resultPath);
        this.graph = graph;
    }

    @Override
    protected Vertex getOrCreate(String value) {
        Vertex vertex = null;
        if (!HugeGraphUtils.isStringEmpty(value)) {
            Integer id = Integer.valueOf(value);
            Iterator<Vertex> vertices = this.graph.vertices(id);
//            vertex = this.graph.vertices(id).next();
            if (!vertices.hasNext()) {
//                vertex = this.graph.addVertex(T.label, HugeGraphDatabase.NODE,
//                                              HugeGraphDatabase.NODE_ID, id);
                vertex = this.graph.addVertex(org.apache.tinkerpop.gremlin.structure.T.id, id, org.apache.tinkerpop.gremlin.structure.T.label, HugeGraphDatabase.NODE);
                this.graph.tx().commit();
            }else {
                vertex = vertices.next();
            }
        }
        return vertex;
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest) {
        src.addEdge(HugeGraphDatabase.SIMILAR, dest);
        this.graph.tx().commit();
    }

    /*
    custom dataset
     */

    @Override
    public Vertex getOrCreateCust(String label, @Nullable String id, Map<String, Object> properties)
    {
        Iterator<Vertex> vertices = null;
        if(Objects.nonNull(id)){
            //get vertex
            vertices = this.graph.vertices(id);
            if (vertices.hasNext()) {
                return vertices.next();
            }
        }

        try {
            if (Objects.isNull(id)) {
                //add vertex dirct
                return this.graph.addVertex(
                        Utils.assemble(Utils.mapTopair(properties), T.label, label));
            } else {
                return this.graph.addVertex(
                        Utils.assemble(
                                Utils.mapTopair(properties),
                                T.id, id,
                                T.label, label));
            }
        }finally {
            this.graph.tx().commit();
        }
    }

    @Override
    public void relateNodesCust(String label,final  Vertex src,final  Vertex dest,Map<String,Object> properties)
    {
        //get or create : edge is single
        Iterator<Edge> edges = src.edges(Direction.OUT, label);
        if (!edgeHasExist(edges,dest)) {
            src.addEdge(label, dest, Utils.mapTopair(properties));
            this.graph.tx().commit();
        }

    }

    private static boolean edgeHasExist(Iterator<Edge> edges,final  Vertex dest)
    {
        if (!edges.hasNext()) {
            return false;
        }
        while (edges.hasNext()) {
            if (edges.next().inVertex().equals(dest)) {
                return true;
            }
        }
        return false;
    }
}

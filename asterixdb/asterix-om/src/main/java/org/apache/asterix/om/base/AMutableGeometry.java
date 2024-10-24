/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.om.base;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class AMutableGeometry extends AGeometry {
    private Geometry geometry;
    private final WKTReader wktReader = new WKTReader();

    public AMutableGeometry(Geometry geom) {
        super(geom);
        this.geometry = geom;
    }

    public void setValue(Geometry geom) {
        this.geometry = geom;
    }

    public Geometry getGeometry() {
        return this.geometry;
    }

    public void parseWKT(String wkt) {
        try {
            this.geometry = wktReader.read(wkt);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}

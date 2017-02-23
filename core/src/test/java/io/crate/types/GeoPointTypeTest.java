/*
 * Licensed to Crate.io Inc. (Crate) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file to
 * you under the Apache License, Version 2.0 (the "License");  you may not
 * use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, to use any modules in this file marked as "Enterprise Features",
 * Crate must have given you permission to enable and use such Enterprise
 * Features and you must have a valid Enterprise or Subscription Agreement
 * with Crate.  If you enable or use the Enterprise Features, you represent
 * and warrant that you have a valid Enterprise or Subscription Agreement
 * with Crate.  Your use of the Enterprise Features if governed by the terms
 * and conditions of your Enterprise or Subscription Agreement with Crate.
 */

package io.crate.types;

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GeoPointTypeTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws Throwable {
        Double[] p1 = new Double[]{41.2, -37.4};

        BytesStreamOutput out = new BytesStreamOutput();
        DataTypes.GEO_POINT.writeValueTo(out, p1);

        StreamInput in = StreamInput.wrap(out.bytes());
        Double[] p2 = DataTypes.GEO_POINT.readValueFrom(in);

        assertThat(p1, equalTo(p2));
    }

    @Test
    public void testWktToGeoPointValue() throws Exception {
        Double[] value = DataTypes.GEO_POINT.value("POINT(1 2)");
        assertThat(value[0], is(1.0d));
        assertThat(value[1], is(2.0d));
    }

    @Test
    public void testInvalidWktToGeoPointValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot convert \"POINT(54.321 -123.456)\" to geo_point." +
            " Bad Y value -123.456 is not in boundary Rect(minX=-180.0,maxX=180.0,minY=-90.0,maxY=90.0)");
        DataTypes.GEO_POINT.value("POINT(54.321 -123.456)");
    }

    @Test
    public void testValueConversionFromList() throws Exception {
        Double[] value = DataTypes.GEO_POINT.value(Arrays.asList(10.0, 20.2));
        assertThat(value[0], is(10.0d));
        assertThat(value[1], is(20.2d));
    }

    @Test
    public void testConversionFromObjectArrayOfIntegers() throws Exception {
        Double[] value = DataTypes.GEO_POINT.value(new Object[]{1, 2});
        assertThat(value[0], is(1.0));
        assertThat(value[1], is(2.0));
    }

    @Test
    public void testConversionFromIntegerArray() throws Exception {
        Double[] value = DataTypes.GEO_POINT.value(new Integer[]{1, 2});
        assertThat(value[0], is(1.0));
        assertThat(value[1], is(2.0));
    }

    @Test
    public void testInvalidLatitude() throws Exception {
        expectedException.expectMessage("Failed to validate geo point [lon=54.321000, lat=-123.456000], not a valid location.");
        DataTypes.GEO_POINT.value(new Double[]{54.321, -123.456});
    }

    @Test
    public void testInvalidLongitude() throws Exception {
        expectedException.expectMessage("Failed to validate geo point [lon=-187.654000, lat=123.456000], not a valid location.");
        DataTypes.GEO_POINT.value(new Double[]{-187.654, 123.456});
    }
}

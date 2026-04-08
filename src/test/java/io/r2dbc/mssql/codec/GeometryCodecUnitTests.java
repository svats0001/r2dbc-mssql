/*
 * Copyright 2018-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.mssql.codec;

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.Geometry;
import com.microsoft.sqlserver.jdbc.SQLServerException;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TestByteBufAllocator;

/**
 * Unit tests for {@link GeometryCodec}.
 *
 * @author svats0001
 */
public class GeometryCodecUnitTests {

    static final TypeInformation GEOMETRY = builder().withLengthStrategy(LengthStrategy.LONGLENTYPE).withServerType(SqlServerType.GEOMETRY).build();
    
    @Test
    void shouldEncodeGeometry() throws SQLServerException {
        
        Encoded encoded = GeometryCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.out(), Geometry.STGeomFromText("POINT(30 10)", 0));
        
        EncodedAssert.assertThat(encoded).isEqualToHex("00 00 00 00 16 00 00 00 40 24 00 00 00 00 00 00 40 3E 00 00 00 00 00 00 0C 01 00 00 00 00");
        assertThat(encoded.getFormalType()).isEqualTo("geometry");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = GeometryCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("00 00 FF FF");
        assertThat(encoded.getFormalType()).isEqualTo("geometry");
    }

    @Test
    void shouldBeAbleToEncodeNull() {

        assertThat(GeometryCodec.INSTANCE.canEncodeNull(Geometry.class)).isTrue();
    }

    @Test
    void shouldBeAbleToDecodeGeometry() {

        assertThat(GeometryCodec.INSTANCE.canDecode(ColumnUtil.createColumn(GEOMETRY), Geometry.class)).isTrue();
    }

    @Test
    void shouldDecodeGeometry() throws SQLServerException {
        
        ByteBuf buffer = HexUtils.decodeToByteBuf("00000000160000004024000000000000403E0000000000000C0100000000");

        Geometry decoded = GeometryCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(GEOMETRY), Geometry.class);

        assertThat(decoded).isEqualTo(Geometry.STGeomFromText("POINT(30 10)", 0));
    }

}
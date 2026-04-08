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

import com.microsoft.sqlserver.jdbc.Geography;
import com.microsoft.sqlserver.jdbc.SQLServerException;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TestByteBufAllocator;

/**
 * Unit tests for {@link GeographyCodec}.
 *
 * @author svats0001
 */
public class GeographyCodecUnitTests {

    static final TypeInformation GEOGRAPHY = builder().withLengthStrategy(LengthStrategy.LONGLENTYPE).withServerType(SqlServerType.GEOGRAPHY).build();

    @Test
    void shouldEncodeGeography() throws SQLServerException {

        Encoded encoded = GeographyCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.out(), Geography.STGeomFromText("POINT(144.9631 -37.8136)", 4326));
        
        EncodedAssert.assertThat(encoded).isEqualToHex("00 00 00 00 16 00 00 00 E6 10 00 00 01 0C 47 03 78 0B 24 E8 42 C0 E2 58 17 B7 D1 1E 62 40");
        assertThat(encoded.getFormalType()).isEqualTo("geography");
    }
    
    @Test
    void shouldEncodeNull() {

        Encoded encoded = GeographyCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("00 00 FF FF");
        assertThat(encoded.getFormalType()).isEqualTo("geography");
    }

    @Test
    void shouldBeAbleToEncodeNull() {

        assertThat(GeographyCodec.INSTANCE.canEncodeNull(Geography.class)).isTrue();
    }

    @Test
    void shouldBeAbleToDecodeGeography() {

        assertThat(GeographyCodec.INSTANCE.canDecode(ColumnUtil.createColumn(GEOGRAPHY), Geography.class)).isTrue();
    }

    @Test
    void shouldDecodeGeography() throws SQLServerException {

        ByteBuf buffer = HexUtils.decodeToByteBuf("16000000E6100000010C4703780B24E842C0E25817B7D11E6240");

        Geography decoded = GeographyCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(GEOGRAPHY), Geography.class);
        Geography geographyExpected = Geography.STGeomFromText("POINT(144.9631 -37.8136)", 4326);

        assertThat(decoded.STAsText()).isEqualTo(geographyExpected.STAsText());
        assertThat(decoded.getSrid()).isEqualTo(geographyExpected.getSrid());
    }
}

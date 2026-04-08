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

import com.microsoft.sqlserver.jdbc.Geography;
import com.microsoft.sqlserver.jdbc.SQLServerException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TdsDataType;
import io.r2dbc.mssql.message.type.TypeInformation;

/**
 * Codec for date types that are represented as {@link Geography}.
 *
 * <ul>
 * <li>Server types: {@link SqlServerType#GEOGRAPHY}</li>
 * <li>Java type: {@link Geography}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @author svats0001
 */
final class GeographyCodec extends AbstractCodec<Geography> {

    /**
     * Singleton instance.
     */
    static final GeographyCodec INSTANCE = new GeographyCodec();

    private static final byte[] NULL = ByteArray.fromBuffer((alloc) ->
    {
        ByteBuf buffer = alloc.buffer(4);
        Encode.uShort(buffer, SqlServerType.GEOGRAPHY.getMaxLength());
        Encode.uShort(buffer, Length.USHORT_NULL);

        return buffer;
    });
    
    private GeographyCodec() {
        super(Geography.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Geography value) {
        return RpcEncoding.encodeLongLenTypeStrategyUDTByteArray(allocator, SqlServerType.GEOGRAPHY, value.serialize(), false);
    }

    @Override
    public boolean canEncodeNull(SqlServerType serverType) {
        return serverType == SqlServerType.GEOGRAPHY;
    }

    @Override
    public Encoded encodeNull(ByteBufAllocator allocator, SqlServerType serverType) {
        return RpcEncoding.encodeLongLenTypeStrategyUDTByteArray(allocator, SqlServerType.GEOGRAPHY, NULL, true);
    }

    @Override
    Encoded doEncodeNull(ByteBufAllocator allocator) {
        return RpcEncoding.encodeLongLenTypeStrategyUDTByteArray(allocator, SqlServerType.GEOGRAPHY, NULL, true);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return typeInformation.getServerType().equals(SqlServerType.GEOGRAPHY);
    }

    @Override
    Geography doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends Geography> valueType) {

        if (length.isNull()) {
            return null;
        }

        byte[] finalData = new byte[length.getLength()];
        buffer.readBytes(finalData);

        try {
            return Geography.deserialize(finalData);
        } catch (SQLServerException exc) {
            return null;
        }

    }
}
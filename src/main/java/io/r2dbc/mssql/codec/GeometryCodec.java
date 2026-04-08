package io.r2dbc.mssql.codec;

import com.microsoft.sqlserver.jdbc.Geometry;
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
 * Codec for date types that are represented as {@link Geometry}.
 *
 * <ul>
 * <li>Server types: {@link SqlServerType#GEOMETRY}</li>
 * <li>Java type: {@link Geometry}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @author svats0001
 */
public class GeometryCodec extends AbstractCodec<Geometry> {
    
    /**
     * Singleton instance.
     */
    static final GeometryCodec INSTANCE = new GeometryCodec();

    private static final byte[] NULL = ByteArray.fromBuffer((alloc) ->
    {
        ByteBuf buffer = alloc.buffer(4);
        Encode.uShort(buffer, SqlServerType.GEOMETRY.getMaxLength());
        Encode.uShort(buffer, Length.USHORT_NULL);

        return buffer;
    });
    
    private GeometryCodec() {
        super(Geometry.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Geometry value) {
        return RpcEncoding.encodeLongLenTypeStrategyByteArray(allocator, SqlServerType.GEOMETRY, value.serialize());
    }

    @Override
    public boolean canEncodeNull(SqlServerType serverType) {
        return serverType == SqlServerType.GEOMETRY;
    }

    @Override
    public Encoded encodeNull(ByteBufAllocator allocator, SqlServerType serverType) {
        return new Encoded(TdsDataType.UDT, () -> Unpooled.wrappedBuffer(NULL));
    }

    @Override
    Encoded doEncodeNull(ByteBufAllocator allocator) {
        return new Encoded(TdsDataType.UDT, () -> Unpooled.wrappedBuffer(NULL));
    }
    
    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return typeInformation.getServerType().equals(SqlServerType.GEOMETRY);
    }

    @Override
    Geometry doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends Geometry> valueType) {

        if (length.isNull()) {
            return null;
        }
        
        int dataLength = length.getLength();
        byte[] finalData = new byte[dataLength];
        buffer.readRetainedSlice(dataLength).readBytes(finalData);

        try {
            return Geometry.deserialize(finalData);
        } catch (SQLServerException exc) {
            return null;
        }

    }
}

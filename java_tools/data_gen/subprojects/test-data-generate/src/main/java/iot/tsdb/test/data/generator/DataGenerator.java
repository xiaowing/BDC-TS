package iot.tsdb.test.data.generator;

import com.alibaba.tsdb.service.api.Alitsdb;
import com.google.common.collect.AbstractIterator;
import iot.tsdb.test.data.meta.DataSetMeta;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.Random;

import static iot.tsdb.test.data.meta.DataConfiguration.CSV_SPLITOR;
import static iot.tsdb.test.data.meta.DataConfiguration.fields;
import static iot.tsdb.test.data.meta.DataConfiguration.getArea;
import static iot.tsdb.test.data.meta.DataConfiguration.getBjlx;
import static iot.tsdb.test.data.meta.DataConfiguration.getDistrict;
import static iot.tsdb.test.data.meta.DataConfiguration.getLine;
import static iot.tsdb.test.data.meta.DataConfiguration.getMpid;
import static iot.tsdb.test.data.meta.DataConfiguration.getProvince;
import static iot.tsdb.test.data.meta.DataConfiguration.getSystem;

/**
 * timestamp,provice,city,system,mpid,cuserid...
 */
public class DataGenerator extends AbstractIterator<byte[]> {
    private DecimalFormat df = new DecimalFormat("0.00");

    private final Random random;
    protected final DataSetMeta meta;
    private final int userType;
    private final boolean aliTSDB;
    private final String metric;

    private int currentUserCount;
    private int currentTimeSeriesIndex;

    public DataGenerator(DataSetMeta meta, long seed, int userType, boolean aliTSDB, String metric) {
        this.meta = meta;
        this.userType = userType;
        random = new Random(seed);
        this.aliTSDB = aliTSDB;
        this.metric = metric;
    }

    @Override
    protected byte[] computeNext() {
        int userId = currentUserId();
        if (userId > meta.getEndUserId()) {
            return endOfData();
        }

        long timestamp = meta.calculateTimestamp(currentTimeSeriesIndex);
        currentTimeSeriesIndex++;

        if (isTimeEnd()) {
            nextUser();
        }

        if (aliTSDB) {
            return toAliPoint(metric, timestamp, userId, userType);
        } else {
            String line = toLine(timestamp, userId, userType) + "\n";
            return line.getBytes(StandardCharsets.UTF_8);
        }
    }


    private boolean isTimeEnd() {
        return currentTimeSeriesIndex >= meta.getLineCountPerUser();
    }

    private int currentUserId() {
        return meta.getStartUserId() + currentUserCount;
    }

    private void nextUser() {
        currentTimeSeriesIndex = 0;
        currentUserCount++;
    }

    private String toLine(long timestamp, int cuserid, int userType) {
        LineBuilder lineBuilder = new LineBuilder();
        lineBuilder.append(timestamp)
                .append(cuserid)
                .append(getProvince(cuserid))
                .append(getDistrict(cuserid))
                .append(getSystem(cuserid))
                .append(getMpid(cuserid))
                .append(getBjlx(cuserid, userType))
                .append(getLine(cuserid))
                .append(getArea(cuserid));

        for (String field : fields) {
            double value = random.nextDouble() * 1000000;
            lineBuilder.append(df.format(value));
        }
        return lineBuilder.build();
    }

    private class LineBuilder {
        private StringBuilder stringBuilder;

        LineBuilder() {
            stringBuilder = new StringBuilder(512);
        }

        LineBuilder append(Object o) {
            stringBuilder.append(o);
            stringBuilder.append(CSV_SPLITOR);
            return this;
        }

        String build() {
            if (stringBuilder.length() > 1) {
                return stringBuilder.substring(0, stringBuilder.length() - 1);
            }
            return "";
        }
    }

    private byte[] toAliPoint(String metric, long timestamp, int cuserid, int userType) {
        // electric,AREA=area_0,BJLX=3,DISTRICT=zhuhai,LINE=line_0,MPID=00000,PROVINCE=gd,SYSTEM=TMR,ZCUSID=0
        String sb = metric +
                "," + "AREA" + "=" + getArea(cuserid) +
                "," + "BJLX" + "=" + getBjlx(cuserid, userType) +
                "," + "DISTRICT" + "=" + getDistrict(cuserid) +
                "," + "LINE" + "=" + getLine(cuserid) +
                "," + "MPID" + "=" + getMpid(cuserid) +
                "," + "PROVINCE" + "=" + getProvince(cuserid) +
                "," + "SYSTEM" + "=" + getSystem(cuserid) +
                "," + "ZCUSID" + "=" + cuserid;
        // ----------------- MultifieldPoint ---------------
        // fields
//        Map<String, Double> allFields = new HashMap<>();
//        for (String field : fields) {
//            double value = random.nextDouble() * 1000000;
//            allFields.put(field, Double.valueOf(df.format(value)));
//        }
//        return Alitsdb.MultifieldPoint
//                .newBuilder()
//                .setTimestamp(timestamp)
//                .setSerieskey(sb.toString())
//                .putAllFields(allFields)
//                .build()
//                .toByteString()
//                .toStringUtf8();

        // ----------------- MputPoint ---------------
//        final Alitsdb.MputPoint.Builder builder = Alitsdb.MputPoint
//                .newBuilder()
//                .setTimestamp(timestamp)
//                .setSerieskey(sb.toString());
//        for (String ignored : fields) {
//            double value = random.nextDouble() * 1000000;
//            builder.addFvalues(Double.parseDouble(df.format(value)));
//        }
//        return builder.build()
//                .toByteString()
//                .toStringUtf8();

        // ----------------- MputRequest -----------------
        final Alitsdb.MputRequest.Builder finalBuilder = Alitsdb.MputRequest.newBuilder();
        final Alitsdb.MputPoint.Builder builder = Alitsdb.MputPoint
                .newBuilder()
                .setTimestamp(timestamp)
                .setSerieskey(sb);
        for (String field : fields) {
            finalBuilder.addFnames(field);
            double value = random.nextDouble() * 1000000;
            builder.addFvalues(Double.parseDouble(df.format(value)));
        }
        final Alitsdb.MputRequest build = finalBuilder.addPoints(builder.build()).build();
        final byte[] dataBytes = build.toByteArray();
        final long unsignedLong = Integer.toUnsignedLong(dataBytes.length/* + Long.BYTES*/);
        ByteBuffer buffer = ByteBuffer.allocate((dataBytes.length + Long.BYTES) * 2)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(unsignedLong)
                .put(dataBytes);
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}

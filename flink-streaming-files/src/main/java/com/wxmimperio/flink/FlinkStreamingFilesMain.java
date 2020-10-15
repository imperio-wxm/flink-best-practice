package com.wxmimperio.flink;

import com.wxmimperio.flink.bean.AppInfocProtoFlink;
import com.wxmimperio.flink.writer.ParquetProtoWriters;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FlinkStreamingFilesMain {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10 * 1000);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());


        DataStream<byte[]> sourceDs = env.addSource(new DynamicConfigSource());

        String filePath = "/Users/weiximing/code/github/flink-best-practice/flink-streaming-files/src/output";


        DateTimeBucketAssigner bucketAssigner = new DateTimeBucketAssigner<>("yyyy-MM-dd");
        final StreamingFileSink sink = StreamingFileSink
                .forBulkFormat(new Path(filePath), ParquetProtoWriters.forType(AppInfocProtoFlink.AppEvent.class))
                .withNewBucketAssigner(bucketAssigner)
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        sourceDs.addSink(sink);

        env.execute("Streaming files...");
    }

    public static class DynamicConfigSource implements SourceFunction<byte[]> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<byte[]> ctx) throws Exception {
            long idx = 1;
            while (isRunning) {
                ctx.collect(getData2());
                idx++;
                //TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }


    /**
     * "player.ugc-video-detail.tab.comments.click"
     * app_info {
     * app_id: 1
     * platform: 3
     * buvid: "XYF3BA5FC3285373338D5762001B106457D18"
     * chid: "xiaomi"
     * brand: "xiaomi"
     * device_id: "PQU0AjVTNQdiADYAfAB8"
     * model: "Redmi Note 8"
     * osver: "9"
     * fts: 1570380234
     * uid: 10168
     * api_level: 28
     * abi: "arm64-v8a"
     * bilifp: "9941d65bf82a7ef91d037799eaed856d20191007004354aa7b337c386fd3a072"
     * }
     * runtime_info {
     * network: WIFI
     * oid: "46000"
     * version: "6.4.2"
     * version_code: "6042010"
     * logver: "0.2.21"
     * ff_version: "3026"
     * }
     * mid: "33936615"
     * ctime: 1594731535184
     * log_id: "001538"
     * sn: 2366864
     * event_category: CLICK
     * app_click_info {
     * }
     * page_type: 1
     * sn_gen_time: 1594731536199
     * upload_time: 1594731536680
     * data_real_ip: "183.199.58.17"
     * gateway_request_url: "/log/pbmobile/realtime?android"
     * gateway_receive_time: "1594731549744"
     *
     * @return
     */
    private static byte[] getData() {
        AppInfocProtoFlink.AppEvent.Builder builder = AppInfocProtoFlink.AppEvent.newBuilder();
        builder.setEventId("player.ugc-video-detail.tab.comments.click");

        AppInfocProtoFlink.AppInfo.Builder appBuilder = AppInfocProtoFlink.AppInfo.newBuilder();
        appBuilder.setAppId(1);
        appBuilder.setPlatform(3);
        appBuilder.setBuvid("XYF3BA5FC3285373338D5762001B106457D18");
        appBuilder.setChid("xiaomi");
        appBuilder.setBrand("xiaomi");
        appBuilder.setDeviceId("PQU0AjVTNQdiADYAfAB8");
        appBuilder.setModel("Redmi Note 8");
        appBuilder.setOsver("9");
        appBuilder.setFts(1570380234);
        appBuilder.setUid(10168);
        appBuilder.setApiLevel(28);
        appBuilder.setAbi("arm64-v8a");
        appBuilder.setBilifp("9941d65bf82a7ef91d037799eaed856d20191007004354aa7b337c386fd3a072");
        builder.setAppInfo(appBuilder.build());

        AppInfocProtoFlink.AppRuntimeInfo.Builder runTimeBuilder = AppInfocProtoFlink.AppRuntimeInfo.newBuilder();
        runTimeBuilder.setNetwork(AppInfocProtoFlink.RuntimeNetWork.WIFI);
        runTimeBuilder.setOid("46000");
        runTimeBuilder.setVersion("6.4.2");
        runTimeBuilder.setVersionCode("6042010");
        runTimeBuilder.setLogver("0.2.21");
        runTimeBuilder.setFfVersion("3026");
        builder.setRuntimeInfo(runTimeBuilder.build());

        builder.setMid("33936615");
        builder.setCtime(1594731535184L);
        builder.setLogId("001538");
        builder.setSn(2366864L);
        builder.setEventCategory(AppInfocProtoFlink.EventCategory.CLICK);
        builder.setPageType(1);
        builder.setSnGenTime(1594731536199L);
        builder.setUploadTime(1594731536680L);
        builder.setDataRealIp("183.199.58.17");
        builder.setGatewayRequestUrl("/log/pbmobile/realtime?android");
        builder.setGatewayReceiveTime("1594731549744");

        AppInfocProtoFlink.AppClickInfo.Builder appClickBuilder = AppInfocProtoFlink.AppClickInfo.newBuilder();
        builder.setAppClickInfo(appClickBuilder.build());

        return builder.build().toByteArray();
    }

    /**
     * event_id: "app.active.speed.sys"
     * app_info {
     * app_id: 1
     * platform: 1
     * buvid: "cf78f64731912e0c8641210c87be27e4"
     * chid: "AppStore"
     * brand: "Apple"
     * device_id: "cf78f64731912e0c8641210c87be27e4"
     * model: "iPhone 8"
     * osver: "13.5.1"
     * fts: 1557878417351
     * uid: 381444478
     * bilifp: "00D555B4B93276C274E6F1B4EB4206D1201910030901221BFB89B481D676C35F"
     * }
     * runtime_info {
     * network: WIFI
     * oid: "46007"
     * version: "6.3.0"
     * version_code: "10070"
     * logver: "3.0.0"
     * ff_version: "3059"
     * }
     * mid: "381444478"
     * ctime: 1594880878340
     * log_id: "001538"
     * sn: 3610197
     * event_category: SYSTEM
     * app_click_info {
     * }
     * extended_fields {
     * key: "f_name"
     * value: "BBLive"
     * }
     * extended_fields {
     * key: "f_level"
     * value: "10"
     * }
     * extended_fields {
     * key: "f_type"
     * value: "2"
     * }
     * extended_fields {
     * key: "f_duration"
     * value: "6"
     * }
     * extended_fields {
     * key: "f_session"
     * value: "8AEDDACE5B9D171CCDFD4BFF40F8BFB6.1"
     * }
     * page_type: 1
     * sn_gen_time: 1594880878349
     * upload_time: 1594880882626
     * data_real_ip: "120.243.160.49"
     * gateway_request_url: "/log/pbmobile/unrealtime?ios"
     * gateway_receive_time: "1594880882692"
     *
     * @return
     */
    private static byte[] getData2() {
        AppInfocProtoFlink.AppEvent.Builder builder = AppInfocProtoFlink.AppEvent.newBuilder();
        builder.setEventId("app.active.speed.sys");

        AppInfocProtoFlink.AppInfo.Builder appBuilder = AppInfocProtoFlink.AppInfo.newBuilder();
        appBuilder.setAppId(1);
        appBuilder.setPlatform(1);
        appBuilder.setBuvid("cf78f64731912e0c8641210c87be27e4");
        appBuilder.setChid("AppStore");
        appBuilder.setBrand("Apple");
        appBuilder.setDeviceId("cf78f64731912e0c8641210c87be27e4");
        appBuilder.setModel("iPhone 8");
        appBuilder.setOsver("13.5.1");
        appBuilder.setFts(1557878417351L);
        appBuilder.setUid(381444478);
        appBuilder.setBilifp("00D555B4B93276C274E6F1B4EB4206D1201910030901221BFB89B481D676C35F");
        builder.setAppInfo(appBuilder.build());

        AppInfocProtoFlink.AppRuntimeInfo.Builder runTimeBuilder = AppInfocProtoFlink.AppRuntimeInfo.newBuilder();
        runTimeBuilder.setNetwork(AppInfocProtoFlink.RuntimeNetWork.WIFI);
        runTimeBuilder.setOid("46007");
        runTimeBuilder.setVersion("6.3.0");
        runTimeBuilder.setVersionCode("10070");
        runTimeBuilder.setLogver("3.0.0");
        runTimeBuilder.setFfVersion("3059");
        builder.setRuntimeInfo(runTimeBuilder.build());

        builder.setMid("381444478");
        builder.setCtime(1594880878340L);
        builder.setLogId("001538");
        builder.setSn(3610197L);
        builder.setEventCategory(AppInfocProtoFlink.EventCategory.SYSTEM);

        Map<String, String> map = new HashMap<>();
        map.put("f_name", "BBLive");
        map.put("f_level", "10");
        map.put("f_type", "2");
        map.put("f_duration", "6");
        map.put("f_session", "8AEDDACE5B9D171CCDFD4BFF40F8BFB6.1");
        builder.putAllExtendedFields(map);

        builder.setPageType(1);
        builder.setSnGenTime(1594880878349L);
        builder.setUploadTime(1594880882626L);
        builder.setDataRealIp("120.243.160.49");
        builder.setGatewayRequestUrl("/log/pbmobile/unrealtime?ios");
        builder.setGatewayReceiveTime("1594880882692");

        AppInfocProtoFlink.AppClickInfo.Builder appClickBuilder = AppInfocProtoFlink.AppClickInfo.newBuilder();
        builder.setAppClickInfo(appClickBuilder.build());

        return builder.build().toByteArray();
    }
}

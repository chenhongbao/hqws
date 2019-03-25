package flyingbot.it.hq.ws.resources;

public class Constants {
    // JSON format constant
    public final static String DataTag = "data";
    public final static String TypeTag = "type";
    public final static String SequenceTag = "sequence";
    public final static String MetadataTag = "_METADATA_";
    // Find dominant instrument tags
    public final static String PidTag = "ProductID";
    public final static String InstrumentIDTag = "InstrumentID";

    // JSON type field, marking the data is historical
    public final static String OldCandleType = "OldCandle";
    public final static String OldMarketDataType = "OldMarketData";
    // LRU size
    public final static int LRUSize = 50;
    // Heartbeat message
    public final static String HeartbeatMsg = "{\"sequence\":0,\"type\":\"Heartbeat\",\"data\":[]}";
    // Default listening port
    public final static int DefaultListenPort = 9101;
    // JSON request types
    public final static String DataType_Sub = "Subscription";
    public final static String DataType_Uns = "Unsubscription";
    public final static String FindDominant = "FindDominantInstrument";

    // JSON repsonse types
    public final static String DominantInstrumentType = "DominantInstrument";

    // Candle settings
    public final static int Candle1440_MaxNum = 240;
    public final static int Candle60_MaxNum = 1440;
    public final static int Candle15_MaxNum = 5760;
    public final static int Candle5_MaxNum = 17280;

    // Candle periods
    public final static int Candle_1440m = 1440;
    public final static int Candle_60m = 60;
    public final static int Candle_15m = 15;
    public final static int Candle_5m = 5;
    public final static int Candle_1m = 1;

    // Query MD number, will send the number of MDs to clients
    public final static int MarketDataToClient_Num = 10;
    // Default cached market data number
    public static final int DefaultCachedMarketData_Num = 10;

    // URL parameter
    public final static String candleNum = "candlenumber";

    // Null subscription
    public final static String NullInstrument = "x0";

    // Default initial sent history candles
    public final static int DefaultInitCandleNumber = 360;

    // URI
    public final static String wsURI = "/hqws";
    public final static String subProtocol = "flyingbot_hq_json_ws";

    // URI key
    public final static String URIKey = "client.ws.URI";

    // Heartbeat sending interval (ms)
    public final static int WsHeartbeat_Intvl = 17 * 1000;

    // Incoming charset
    public final static String WsIncomingCharset = "UTF-8";

    // HTTP max content
    public final static int HTTP_MaxContentLength = 64 * 1024;

    // Configuration tags
    public final static String ConfigTag_URL = "URL";
    public final static String ConfigTag_User = "Username";
    public final static String ConfigTag_Pwd = "Password";
    public final static String ConfigTag_Port = "Port";
    public final static String ConfigTag_IP = "IP";

}

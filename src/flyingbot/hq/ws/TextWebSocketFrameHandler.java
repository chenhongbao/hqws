package flyingbot.hq.ws;

import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;

import flyingbot.hq.ws.system.HQServerContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;

/**
 * WebSocket subprotocol is defined in {@link HQServerContext.subProtocol}, make sure they are matched.
 * 
 * Two ways to subscribe an instrument,
 * <li>URI path mapping, ws://localhost:8080/(service-path)/(instrument). 
 *     Service path is defined in {@link HQServerContext.wsURI}.
 * <li>Send un/subscription JSON data.
 * 
 * JSON un/subscription format,
 * 
 * <pre>
 * {
 *     'sequence' : 12345£¬ //Request sequence number
 *     'type' : 'Subscription', //Operation: un/subscribe
 *     'data' : ["rb1905", "c1905"] //Instruments
 * }
 * </pre>
 * 
 * Client will not have response if subscription fails, in the cases that there are not available instrument.
 */
public class TextWebSocketFrameHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {

	// Logger instance
	HQServerContext svrCtx;
	
	// InstrumentID regex
	Pattern patt;
	
	// Subscription record
	HashSet<String> subs;
	
	// JSON request types
	public final static String DataType_Sub = "Subscription";
	public final static String DataType_Uns = "Unsubscription";
	
	// URL parameter
	public final static String candleNum = "candlenumber";
	
	// Default initial sent history candles
	public final static int DefaultInitCandleNumber = 360;
	int numberCandle = DefaultInitCandleNumber;
	
	public TextWebSocketFrameHandler(HQServerContext ctx) {
		this.svrCtx = ctx;
		this.subs = new HashSet<String>();
		this.patt = Pattern.compile("[a-zA-Z]+[0-9]+");
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {	
		String rawInst;

		if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {	
			// Will not use HTTP anymore.
			ctx.pipeline().remove(HttpRequestHandler.class);
			
			// Process subscription
			String path = (String)svrCtx.getChannelParameter(ctx.channel());
			svrCtx.removeChannelParameter(ctx.channel());
			
			// Parse instrument id and candle number
			if (path.indexOf('?') != -1) {
				rawInst = path.substring(0, path.indexOf('?'));
				numberCandle = Integer.parseInt(getURLValue(candleNum, 
						path.substring(path.indexOf('?') + 1)));
			}
			else {
				rawInst = path;
			}
			
			// Test if the path matches instrument id
			Matcher m = patt.matcher(rawInst);
			if (m.find()) {
				String inst = rawInst.substring(m.start());
				if (subs.contains(inst)) {
					svrCtx.LOG.warning("Client subscribe duplicated instrument, " + inst);
				}
				else {
					// Subscribe
					subs.add(inst);
					svrCtx.subcribers.subscribe(inst, ctx.channel());
					
					// Send initial history data
					svrCtx.subcribers.sendHistoryData(inst, ctx.channel(), numberCandle);
				}
			}
			else {
				// Wrong input from client
				svrCtx.LOG.warning("Client subscribe invalid instrument, PATH: " + path);
				ctx.close();
			}
		}
		else {
			super.userEventTriggered(ctx, evt);
		}
	}
	
	protected String getURLValue(String key, String url) {
		int p = url.indexOf(key + "=");
		if (p == -1) {
			return "";
		}
		int q = url.indexOf('&', p + key.length() + 1);
		if (q == -1) {
			return url.substring(p + key.length() + 1);
		}
		else {
			return url.substring(p + key.length() + 1, q);
		}
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame msg) throws Exception {
		try {
			String text = msg.text();
			JSONObject o = new JSONObject(text);
			
			// Check the type
			if (o.getString("type").compareTo(DataType_Sub) != 0 
					&& o.getString("type").compareTo(DataType_Uns) != 0) {
				svrCtx.LOG.warning("JSON request error, type is supposed to be Subscription/Unsubscription, received " + o.getString("type"));
				return;
			}
			
			// Parse un/subscription
			JSONArray arr = o.getJSONArray("data");
			if (arr.length() > 0) {
				for (int i = 0; i < arr.length(); ++i) {
					String inst = arr.getString(i);
					if (o.getString("type").compareTo(DataType_Sub) == 0) {
						// Duplicated subscription
						if (subs.contains(inst)) {
							svrCtx.LOG.warning("Client subscribe duplicated instrument, " + inst);
						}
						else {
							// Subscribe
							svrCtx.subcribers.subscribe(inst, ctx.channel());
							
							// Send initial history data
							svrCtx.subcribers.sendHistoryData(inst, ctx.channel(), numberCandle);
						}
					}
					else {
						// Unsubscribe
						svrCtx.subcribers.unSubscribe(inst, ctx.channel());
					}
				}
			}
		} catch (Exception e) {
			svrCtx.LOG.warning("Parse JSON failed, " + e.getMessage());
		}
	}	

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		svrCtx.LOG.warning("WebSocket error. " + ctx.channel() + ", " + cause.getMessage());
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// Remove parameter
		svrCtx.removeChannelParameter(ctx.channel());
		
		// Don't need to un-subscribe instruments when Channel is closed.
		// The ChannelGroup will manage the closed channels.
	}
}

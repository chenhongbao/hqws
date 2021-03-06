package flyingbot.it.hq.ws;

import flyingbot.it.hq.ws.system.HQServerContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static flyingbot.it.hq.ws.resources.Constants.*;

/**
 * WebSocket subprotocol is defined in {@link HQServerContext}.subprotocol, make
 * sure they are matched.
 * 
 * Two ways to subscribe an instrument,
 * <li>URI path mapping, ws://localhost:8080/(service-path)/(instrument).
 * Service path is defined in {@link HQServerContext}.wsURI.
 * <li>Send un/subscription JSON data.
 * 
 * JSON un/subscription format,
 * 
 * <pre>
 * {
 *     'sequence' : 12345 //Request sequence number
 *     'type' : 'Subscription', //Operation: un/subscribe
 *     'data' : ["rb1905", "c1905"] //Instruments
 * }
 * </pre>
 * 
 * Add GET parameter 'candlenumber' to the URL and get the given number of old canldes.
 * <li> ws://localhost:8080/(service-path)/(instrument)?candlenumber=555
 * 
 * Client will not have response if subscription fails, in the cases that there
 * are not available instrument.
 */
public class TextWebSocketFrameHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {

	// Logger instance
	HQServerContext svrCtx;

	// InstrumentID regex
	Pattern patt;

	int numberCandle = DefaultInitCandleNumber;

	public TextWebSocketFrameHandler(HQServerContext ctx) {
		this.svrCtx = ctx;
		this.patt = Pattern.compile("[a-zA-Z]+[0-9]+");
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		String rawInst;

		if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
			// Will not use HTTP anymore.
			ctx.pipeline().remove(HttpRequestHandler.class);

			// Process subscription
            String path = svrCtx.channelParameter(ctx.channel(), URIKey);

			// Parse instrument id and candle number
			if (path.indexOf('?') != -1) {
				rawInst = path.substring(0, path.indexOf('?'));
				int tmp = Integer.parseInt(getURLValue(candleNum, path.substring(path.indexOf('?') + 1)));
				if (tmp > 0)
				{
					numberCandle = tmp;
				}	
			} else {
				rawInst = path;
			}

			// Test if the path matches instrument id
            Matcher m = patt.matcher(rawInst.trim());
			if (m.find()) {
                String inst = rawInst.substring(m.start(), m.end());

                // Null instrument, keep connection open without actual subscription
                if (inst.compareTo(NullInstrument) != 0) {
                    subscribeInstrument(ctx, inst);
                }
			} else {
				// Wrong input from client
				svrCtx.LOG.warning("Client subscribe invalid instrument, PATH: " + path);
				ctx.close();
			}
		} else {
			super.userEventTriggered(ctx, evt);
		}
	}

    protected void subscribeInstrument(ChannelHandlerContext ctx, String inst) {
        if (svrCtx.isSubscribed(ctx.channel(), inst)) {
            // Duplicated subscription
            svrCtx.LOG.warning("Client subscribe duplicated instrument, " + inst);
        } else {
            // Subscribe
            svrCtx.addInstrument(ctx.channel(), inst);
			svrCtx.subscribers.subscribe(inst, ctx.channel());

            // Send initial history data
			svrCtx.subscribers.sendHistoryData(inst, ctx.channel(), numberCandle);

            // Log info
            svrCtx.LOG.info("Client joins subscription pool, " + inst + ", " + ctx.channel());
        }
    }

    protected void unsubscribeInstrument(ChannelHandlerContext ctx, String inst) {
        // Unsubscribe
		svrCtx.subscribers.unSubscribe(inst, ctx.channel());
        svrCtx.removeInstrument(ctx.channel(), inst);

        // Log info
        svrCtx.LOG.info("Remove client from subscription pool, " + inst + ", " + ctx.channel());
    }

	protected void sendDominantInstrument(ChannelHandlerContext ctx, String pid) {
		String inst = svrCtx.subscribers.sendDominantInstrument(pid, ctx.channel());

		// Log info
		svrCtx.LOG.info("Find dominate instrument, " + pid + "->" + inst + ", " + ctx.channel());
	}

	protected String getURLValue(String key, String url) {
		int p = url.indexOf(key + "=");
		if (p == -1) {
			return "";
		}
		int q = url.indexOf('&', p + key.length() + 1);
		if (q == -1) {
			return url.substring(p + key.length() + 1);
		} else {
			return url.substring(p + key.length() + 1, q);
		}
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame msg) throws Exception {
		try {
			String text = msg.text();
			JSONObject o = new JSONObject(text);

			// Parse un/subscription
			JSONArray arr = o.getJSONArray(DataTag);
			if (arr.length() < 1) {
				return;
			}

            String type = o.getString(TypeTag);

			// Try subscription
			for (int i = 0; i < arr.length(); ++i) {
				String inst = arr.getString(i);
                if (type.compareTo(DataType_Sub) == 0) {
					subscribeInstrument(ctx, inst);
                } else if (type.compareTo(DataType_Uns) == 0) {
					unsubscribeInstrument(ctx, inst);
                } else if (type.compareTo(FindDominant) == 0) {
					sendDominantInstrument(ctx, inst);
				} else {
                    svrCtx.LOG.warning("JSON request type error, received " + type);
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
        // Remove bundle
        svrCtx.removeChannel(ctx.channel());

		// Don't need to un-subscribe instruments when Channel is closed.
		// The ChannelGroup will manage the closed channels.
		svrCtx.LOG.info("Disconnect " + ctx.channel());
	}
}

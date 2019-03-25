package flyingbot.it.hq.ws;

import flyingbot.it.hq.ws.system.HQServerContext;
import flyingbot.it.util.Common;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;

import static flyingbot.it.hq.ws.resources.Constants.URIKey;
import static flyingbot.it.hq.ws.resources.Constants.wsURI;

public class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
	// Server context
	HQServerContext svrCtx;
	
	public HttpRequestHandler(HQServerContext ctx) {
		this.svrCtx = ctx;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
		// Process websocket prefix
		String reqUri = request.uri();
		svrCtx.LOG.info("Client connected, " + ctx.channel() + ", URI: " + reqUri);
		
		// Check if client requests the right resource
		if (reqUri.startsWith(wsURI)) {
			// Set path parameter
			String param = reqUri.substring(wsURI.length());
            svrCtx.channelParameter(ctx.channel(), URIKey, param);
			
			// Reset URI for WebSocketProtocolHandler, or it will block the request
			request.setUri(wsURI);
			
			// Fire next handler
			ctx.fireChannelRead(request.retain());
		}
		else {
			svrCtx.LOG.warning("Client requests wrong resource, " + ctx.channel() + ", URI: " + reqUri);
			ctx.close();
		}
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		// Log error
		svrCtx.LOG.warning("Client request error, " + cause.getMessage());
		Common.PrintException(cause);
		
		// Close connection
		ctx.close();
	}
}

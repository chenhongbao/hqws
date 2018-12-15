package flyingbot.hq.ws;

import dmkp.common.util.Common;
import flyingbot.hq.ws.system.HQServerContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;

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
		if (reqUri.startsWith(HQServerContext.wsURI)) {
			// Set path parameter
			String param = reqUri.substring(HQServerContext.wsURI.length());
			svrCtx.setChannelParameter(ctx.channel(), param);
			
			// Reset URI for WebSocketProtocolHandler, or it will block the request
			request.setUri(HQServerContext.wsURI);
			
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

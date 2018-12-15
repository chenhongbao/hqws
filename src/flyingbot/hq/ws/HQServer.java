package flyingbot.hq.ws;

import java.net.InetSocketAddress;
import org.json.JSONObject;

import dmkp.common.util.Common;
import flyingbot.hq.ws.system.HQInsideServer;
import flyingbot.hq.ws.system.HQServerContext;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.oio.OioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

public class HQServer {
	
	public static class ChatServerInitializer extends ChannelInitializer<Channel> {
	
		// Server context
		private HQServerContext svrCtx;
		
		public ChatServerInitializer(HQServerContext ctx) {
			this.svrCtx = ctx;
		}

		@Override
		protected void initChannel(Channel ch) throws Exception {
			ChannelPipeline pipeline = ch.pipeline();
			
			// Add listener and handlers
			pipeline.addLast(new HttpServerCodec());
			pipeline.addLast(new ChunkedWriteHandler());
			pipeline.addLast(new HttpObjectAggregator(64 *1024));
			pipeline.addLast(new HttpRequestHandler(svrCtx));
			
			// After update to WebSocket protocol, the handler will replace HttpRequestDecoder
			// with WebSocketFrameDecoder, and HttpResponseEncoder with WebSocketFrameEncoder, and
			// any other ChannelHandler that are not used any more.
			pipeline.addLast(new WebSocketServerProtocolHandler(HQServerContext.wsURI, HQServerContext.subProtocol));
			
			pipeline.addLast(new TextWebSocketFrameHandler(svrCtx));
		}
		
	}
	
	// For short request like WebSocket, Old IO is more efficient.
	private final EventLoopGroup group = new OioEventLoopGroup();
	
	// Channel
	private Channel channel;
	
	// Default listening port
	private final static int DefaultListenPort = 9101;
	
	// Logger
	private static HQServerContext serverCtx;
	
	public ChannelFuture start(InetSocketAddress address) {
		// Create server bootstrap
		ServerBootstrap bootstrap = new ServerBootstrap();
		
		// Setup bootstrap
		bootstrap.group(group);
		bootstrap.channel(OioServerSocketChannel.class);
		bootstrap.childHandler(new ChatServerInitializer(serverCtx));
		
		// Bind address
		ChannelFuture future = bootstrap.bind(address);
		future.syncUninterruptibly();
		
		// Server listening channel, for client's connection
		channel = future.channel();
		
		// Return statement
		return future;
	}
	
	public void destroy() {
		if (channel != null) {
			channel.close();
		}
		
		// Close connection
		serverCtx.subcribers.closeAll();
		group.shutdownGracefully();
	}
	
	protected static int getListenPort() {
		// Get Class path
		StackTraceElement[] traces = Thread.currentThread().getStackTrace();
		Class<?> clz = null;
		try {
			clz = Class.forName(traces[1].getClassName());
		} catch (ClassNotFoundException e) {
			Common.PrintException(e);
			return DefaultListenPort;
		}
		
		// Load JSON as stream
		JSONObject o = Common.LoadJSONObject(clz.getResourceAsStream("port.json"));
		if (!o.has("Port")) {
			serverCtx.LOG.warning("Reading listening port JSON failed, listen on DEFAULT port: " + DefaultListenPort);
			return DefaultListenPort;
		}
		else {
			return o.getInt("Port");
		}
	}
	
	public static void main(String[] args) {
		// Create system context
		serverCtx = new HQServerContext();
		
		// Run inside HQ server
		Common.GetSingletonExecSvc().execute(new HQInsideServer(serverCtx));

		// Prepare binding address
		int port = getListenPort();
		System.out.println("行情WS服务监听端口：" + port);
		serverCtx.LOG.info("行情WS服务监听端口：" + port);
		
		// Create server instance
		final HQServer endpoint = new HQServer();
		ChannelFuture future = endpoint.start(new InetSocketAddress(port));
		
		// Setup destroy
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				endpoint.destroy();
			}
		});
		
		// Close server channel
		future.channel().closeFuture().syncUninterruptibly();
	}

}

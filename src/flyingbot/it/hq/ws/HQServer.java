package flyingbot.it.hq.ws;

import flyingbot.it.hq.ws.resources.Constants;
import flyingbot.it.hq.ws.system.HQInsideServer;
import flyingbot.it.hq.ws.system.HQServerContext;
import flyingbot.it.util.Common;
import flyingbot.it.util.Result;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.json.JSONObject;

import java.net.InetSocketAddress;

import static flyingbot.it.hq.ws.resources.Constants.*;

public class HQServer {

	protected static int getListenPort() {
		// Load JSON as stream
		JSONObject o = Common.LoadJSONObject(Constants.class.getResourceAsStream("port.json"));
		if (!o.has(ConfigTag_Port)) {
			serverCtx.LOG.warning("Reading listening port JSON failed, listen on DEFAULT port: " + DefaultListenPort);
			return DefaultListenPort;
		} else {
			return o.getInt(ConfigTag_Port);
		}
	}
	
	// For short request like WebSocket, Old IO is more efficient.
	private final EventLoopGroup group = new NioEventLoopGroup();
	
	// Channel
	private Channel channel;

	// Logger
	private static HQServerContext serverCtx;
	
	// HQInsideServer, ref holder
	@SuppressWarnings("unused")
	private static HQInsideServer insideServer;
	
	public ChannelFuture start(InetSocketAddress address) {
		// Create server bootstrap
		ServerBootstrap bootstrap = new ServerBootstrap();
		
		// Setup bootstrap
		bootstrap.group(group);
		bootstrap.channel(NioServerSocketChannel.class);
		bootstrap.childHandler(new ChatServerInitializer(serverCtx));
		
		// Bind address
		ChannelFuture future = bootstrap.bind(address);
		future.syncUninterruptibly();
		
		// Server listening channel, for client's connection
		channel = future.channel();
		
		// Heartbeat deamon
		Common.GetSingletonExecSvc().execute(new Runnable() {

			@Override
			public void run() {
				while (true) {
					try {
						// prime number
						Thread.sleep(WsHeartbeat_Intvl);
					} catch (InterruptedException e) {
					}
					
					// Send heartbeats
					Result r = serverCtx.subscribers.SendHeartbeatAll();
					if (r.equals(Result.Error)) {
						serverCtx.LOG.warning(r.Message);
					}
				}
			}
			
		});
		
		// Return statement
		return future;
	}
	
	public void destroy() {
		if (channel != null) {
			channel.close();
		}
		
		// Close connection
		serverCtx.subscribers.closeAll();
		group.shutdownGracefully();
	}

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
			pipeline.addLast(new HttpObjectAggregator(HTTP_MaxContentLength));
			pipeline.addLast(new HttpRequestHandler(svrCtx));

			// After update to WebSocket protocol, the handler will replace HttpRequestDecoder
			// with WebSocketFrameDecoder, and HttpResponseEncoder with WebSocketFrameEncoder, and
			// any other ChannelHandler that are not used any more.
			pipeline.addLast(new WebSocketServerProtocolHandler(wsURI, subProtocol));

			pipeline.addLast(new TextWebSocketFrameHandler(svrCtx));
		}

	}
	
	public static void main(String[] args) {
		// Create system context
		serverCtx = new HQServerContext();
		
		// Run inside HQ server
		insideServer = new HQInsideServer(serverCtx);

		// Prepare binding address
		int port = getListenPort();
		System.out.println("HQWS is listening on port: " + port);
		serverCtx.LOG.info("HQWS is listening on port: " + port);
		
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

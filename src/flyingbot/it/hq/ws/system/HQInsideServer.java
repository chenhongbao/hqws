package flyingbot.it.hq.ws.system;

import flyingbot.it.data.hq.Candle;
import flyingbot.it.data.hq.MarketData;
import flyingbot.it.hq.ws.resources.Constants;
import flyingbot.it.net.tcp.SocketDuplex;
import flyingbot.it.util.Common;
import flyingbot.it.util.Result;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.HashSet;

import static flyingbot.it.hq.ws.resources.Constants.*;

public class HQInsideServer implements Runnable {
	// Server context instance
	HQServerContext svrCtx;

	private int port = 0;

	// Alive connection
	private HashSet<SocketDuplex> connections;

	protected int getListenPort() {
		// Load JSON as stream
		JSONObject o = Common.LoadJSONObject(Constants.class.getResourceAsStream("in_port.json"));
		if (!o.has(ConfigTag_Port)) {
			svrCtx.LOG.warning("Reading listening port JSON failed, listen on DEFAULT port: " + DefaultListenPort);
			return DefaultListenPort;
		} else {
			return o.getInt(ConfigTag_Port);
		}
	}

	public HQInsideServer(HQServerContext ctx) {
		this.svrCtx = ctx;
		this.connections = new HashSet<SocketDuplex>();
		this.port = getListenPort();

		// Run server
		Common.GetSingletonExecSvc().execute(this);
	}

	@SuppressWarnings("resource")
	@Override
	public void run() {
		ServerSocket ss = null;

		// Listen on port
		try {
			ss = new ServerSocket(port);

			// Log info
			System.out.println("HQInside is listening on port: " + port);
			svrCtx.LOG.info("HQInside is listening on port: " + port);
		} catch (IOException e) {
			Common.PrintException(e);
			return;
		}

		while (true) {
			try {
				Socket s = ss.accept();

				// Filter IP
				InetSocketAddress addr = (InetSocketAddress) s.getRemoteSocketAddress();
				String remoteIP = addr.getAddress().getHostAddress();

				InputStream is = Constants.class.getResourceAsStream("ip.json");
				if (!Common.VerifyIP(remoteIP, is)) {
					svrCtx.LOG.warning("Declined connection from " + remoteIP);
					s.close();
					continue;
				}

				// Accept connection and start session
				connections.add(new HQInsideSession(s));
			} catch (IOException e) {
				svrCtx.LOG.warning("Accepting client connection error, " + e.getMessage());
			}
		}
	}

	class HQInsideSession extends SocketDuplex {

		public HQInsideSession(Socket s) {
			super(s);
		}

		@Override
		public void OnConnect() {
			svrCtx.LOG.info("Initiate inside market data session from " + GetInetAddress().getHostAddress());
		}

		@Override
		public void OnStream(byte[] Data) {
			// Encode the string in utf-8
			String text = new String(Data, Charset.forName(WsIncomingCharset));

			// Decide the type of JSON
			try {
				// Parse JSON
				// Compatible for both array and object
				if (text.trim().startsWith("[")) {
					JSONArray arr = new JSONArray(text);
					onJSONArray(arr);
				}
				else if (text.trim().startsWith("{")) {
					JSONObject o = new JSONObject(text);
					onJSONObject(o);
				}
				else {
					svrCtx.LOG.warning("Invalid JSON string, " + text);
				}
			} catch (JSONException e) {
				svrCtx.LOG.warning("Parsing JSON market data failed, " + e.getMessage());
			}
		}

		@Override
		public void OnDisconnect() {
			svrCtx.LOG.info("Disconnnect inside market data session from " + GetInetAddress().getHostAddress());
		}

		@Override
		public void OnHearbeatError(Result Reason) {
		}

		public void onJSONArray(JSONArray arr) {
			for (int i = 0; i < arr.length(); ++i) {
				JSONObject o = arr.getJSONObject(i);
				onJSONObject(o);
			}
		}

		public void onJSONObject(JSONObject o) {
			Result res;

			// Get type
			String type = o.getString(MetadataTag);
			switch (type) {
			case MarketData.DataType:
				res = svrCtx.subscribers.onMarketData(MarketData.Parse(o));
				break;
			case Candle.DataType:
				res = svrCtx.subscribers.onCandle(Candle.Parse(o));
				break;
			default:
				res = new Result(Result.Error, -1, "Unknown market data type, " + type);
			}

			// Process result
			if (res.equals(Result.Error)) {
				svrCtx.LOG.warning("Sending data failed, " + res.Message);
			}
		}

	}

}

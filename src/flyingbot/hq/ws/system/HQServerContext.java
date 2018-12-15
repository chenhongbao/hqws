package flyingbot.hq.ws.system;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import org.json.JSONObject;

import dmkp.common.util.Common;
import flyingbot.data.log.SocketLoggerFactory;
import io.netty.channel.Channel;

public class HQServerContext {
	// URI
	public static String wsURI = "/hqws";
	public static String subProtocol = "flyingbot_hq_json_ws";
	
	// Logger instance
	public Logger LOG;
	
	// Marketdata
	public HQSubscribers subcribers;
	
	// Channel parameters
	protected ReentrantReadWriteLock cpLock;
	protected HashMap<Channel, Object> channelParams;
	
	public HQServerContext() {
		// Initialize logger
		initLogger();
		
		// Channel parameters
		cpLock = new ReentrantReadWriteLock(); 
		channelParams = new HashMap<Channel, Object>();
		
		// Create marketdata
		subcribers = new HQSubscribers(LOG);
	}
	
	private void initLogger() {	
		// Read configuration
		JSONObject obj = Common.LoadJSONObject(this.getClass().getResourceAsStream("log_addr.json"));
		if (obj != null && obj.has("IP") && obj.has("Port")) {
			int port = 0;
			String ip = null;
			
			// Get fields
			ip = obj.getString("IP");
			port = obj.getInt("Port");
			
			// Create logger instance
			LOG = SocketLoggerFactory.GetInstance(this.getClass().getCanonicalName(), ip, port);
		}
		else {
			Common.PrintException("去读日志服务配置失败");
		}
	}
	
	/**
	 * Set mapped object for the specific Channel.
	 * @param c Channel
	 * @param v Value, an Object instance
	 */
	public void setChannelParameter(Channel c, Object v) {
		cpLock.writeLock().lock();
		channelParams.put(c, v);
		cpLock.writeLock().unlock();
	}
	
	/**
	 * Get the mapped object for the specific {@link Channel}.
	 * @param c Channel having the object.
	 * @return object mapped for the channel, or null if no object found.
	 */
	public Object getChannelParameter(Channel c) {
		cpLock.readLock().lock();
		Object ret = channelParams.get(c);
		cpLock.readLock().unlock();
		return ret;
	}

	/**
	 * Remove object mapped to the Channel.
	 * @param c Channel that owns the object.
	 * @return true on success, false if no object associated with the key.
	 */
	public boolean removeChannelParameter(Channel c) {
		cpLock.writeLock().lock();
		Object ret = channelParams.remove(c);
		cpLock.writeLock().unlock();
		return ret != null;
	}
} 

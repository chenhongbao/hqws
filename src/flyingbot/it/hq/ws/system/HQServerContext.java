package flyingbot.it.hq.ws.system;

import flyingbot.it.data.log.SocketLoggerFactory;
import flyingbot.it.hq.ws.resources.Constants;
import flyingbot.it.util.Common;
import io.netty.channel.Channel;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import static flyingbot.it.hq.ws.resources.Constants.ConfigTag_IP;
import static flyingbot.it.hq.ws.resources.Constants.ConfigTag_Port;

public class HQServerContext {

	
	// Logger instance
	public Logger LOG;
	
	// Marketdata
	public HQSubscribers subscribers;
	protected HashMap<Channel, ChannelParameterBundle> channelParams;
	
	// Channel parameters
	protected ReentrantReadWriteLock cpLock;
	public HQServerContext() {
		// Initialize logger
		initLogger();

		// Channel parameters
		cpLock = new ReentrantReadWriteLock();
		channelParams = new HashMap<>();

		// Create marketdata
		subscribers = new HQSubscribers(LOG);
	}

	/**
	 * Set mapped object for the specific Channel.
	 *
	 * @param c     Channel
	 * @param key   key
	 * @param value Value
	 */
	public void channelParameter(Channel c, String key, String value) {
		cpLock.writeLock().lock();

		if (!channelParams.containsKey(c)) {
			channelParams.put(c, new ChannelParameterBundle());
		}

		// Add parameter
		channelParams.get(c).parameter(key, value);

		cpLock.writeLock().unlock();
	}
	
	private void initLogger() {	
		// Read configuration
		JSONObject obj = Common.LoadJSONObject(Constants.class.getResourceAsStream("log_addr.json"));
		if (obj != null && obj.has(ConfigTag_IP) && obj.has(ConfigTag_Port)) {
			int port = 0;
			String ip = null;
			
			// Get fields
			ip = obj.getString(ConfigTag_IP);
			port = obj.getInt(ConfigTag_Port);
			
			// Create logger instance
			LOG = SocketLoggerFactory.GetInstance(this.getClass().getCanonicalName(), ip, port);
		}
		else {
            Common.PrintException("Logging server info missing");
		}
	}
	
	/**
	 * Get the mapped object for the specific {@link Channel}.
	 * @param c Channel having the object.
	 * @return object mapped for the channel, or null if no object found.
	 */
	public String channelParameter(Channel c, String key) {
		String ret = null;
		cpLock.readLock().lock();

		if (channelParams.containsKey(c)) {
			ret = channelParams.get(c).parameter(key);
		}

		cpLock.readLock().unlock();
		return ret;
	}
	
	/**
	 * Remove object mapped to the Channel.
	 * @param c Channel that owns the object.
	 * @return true on success, false if no object associated with the key.
	 */
	public boolean removeChannel(Channel c) {
		cpLock.writeLock().lock();
		Object ret = channelParams.remove(c);
		cpLock.writeLock().unlock();
		return ret != null;
	}

	/**
	 * Remove key-value pair on channel.
	 *
	 * @param c   channel
	 * @param key key
	 * @return true if success
	 */
	public boolean removeParameter(Channel c, String key) {
		String ret = null;
		cpLock.writeLock().lock();

		if (channelParams.containsKey(c)) {
			ret = channelParams.get(c).removeParameter(key);
		}

		cpLock.writeLock().unlock();
		return ret != null;
	}

	/**
	 * Add subscribed isntrument record.
	 *
	 * @param c    channel
	 * @param inst instrument
	 */
	public void addInstrument(Channel c, String inst) {
		cpLock.writeLock().lock();

		if (!channelParams.containsKey(c)) {
			channelParams.put(c, new ChannelParameterBundle());
		}

		// Add instrument
		channelParams.get(c).instrument(inst);

		cpLock.writeLock().unlock();
	}

	/**
	 * Remove instrument subscription record.
	 *
	 * @param c    channel
	 * @param inst instrument
	 * @return true if success
	 */
	public boolean removeInstrument(Channel c, String inst) {
		boolean ret = false;
		cpLock.writeLock().lock();

		if (channelParams.containsKey(c)) {
			ret = channelParams.get(c).removeInstrument(inst);
		}

		cpLock.writeLock().unlock();
		return ret;
	}

	/**
	 * Test if instrument has been subscribed
	 *
	 * @param c    channel
	 * @param inst instrument
	 * @return true if given instrument is subscribed
	 */
	public boolean isSubscribed(Channel c, String inst) {
		boolean ret = false;
		cpLock.writeLock().lock();

		if (channelParams.containsKey(c)) {
			ret = channelParams.get(c).instruments().contains(inst);
		}

		cpLock.writeLock().unlock();
		return ret;
	}

	public static class ChannelParameterBundle {
		/**
		 * Hash map for parameters
		 */
		HashMap<String, String> params;

		/**
		 * Subscribed instruments
		 */
		HashSet<String> subs;

		public ChannelParameterBundle() {
			params = new HashMap<String, String>();
			subs = new HashSet<String>();
		}

		public void parameter(String key, String value) {
			params.put(key, value);
		}

		public String parameter(String key) {
			return params.get(key);
		}

		public String removeParameter(String key) {
			return params.remove(key);
		}

		public Map<String, String> parameters() {
			return params;
		}

		public void instrument(String inst) {
			subs.add(inst);
		}

		public boolean removeInstrument(String inst) {
			return subs.remove(inst);
		}

		public Set<String> instruments() {
			return subs;
		}
	}
} 

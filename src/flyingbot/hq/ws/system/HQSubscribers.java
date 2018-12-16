package flyingbot.hq.ws.system;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import dmkp.common.util.Result;
import flyingbot.data.hq.Candle;
import flyingbot.data.hq.MarketData;
import flyingbot.data.hq.TransferredData;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.concurrent.ImmediateEventExecutor;

public class HQSubscribers {
	
	// Subscription group
	protected ReentrantReadWriteLock rwLock;
	protected HashMap<String, ChannelGroup> subscription;
	
	// Market data keeper
	public HQDataKeeper dataKeeper;
	
	// JSON format constant
	public final static String DataTag = "data";
	public final static String TypeTag = "type";
	public final static String SequenceTag = "sequence";
	
	// Send timeout
	public final static int SendTimeoutMillis = 5000;
	
	// Sequence
	AtomicLong sequence;
	
	// Logger instance
	Logger LOG;
	
	public HQSubscribers(Logger log) {
		// Create data keeper
		dataKeeper = new HQDataKeeper(log);
		
		// Atom seq
		sequence = new AtomicLong(0);
		
		// Set logger
		LOG = log;
		
		// Create subscription record
		rwLock = new ReentrantReadWriteLock();
		subscription = new HashMap<String, ChannelGroup>();
	}
	
	public Result subscribe(String inst, Channel c) {
		// Initialize ChannelGroup when not exist
		rwLock.writeLock().lock();
		if (!subscription.containsKey(inst)) {
			subscription.put(inst, new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE));
		}
		
		// Add Channel to group if no subscription found
		if (!subscription.get(inst).contains(c)) {
			// Add to ChannelGroup
			subscription.get(inst).add(c);
			
			// unlock
			rwLock.writeLock().unlock();
			return new Result();
		}
		else {
			// unlock
			rwLock.writeLock().unlock();
			return new Result(Result.Error, -1, "Duplecated subscription.");
		}
	}

	public Result unSubscribe(String inst, Channel c) {
		rwLock.writeLock().lock();
		
		// Check if instrument recod found
		if (!subscription.containsKey(inst)) {
			//unlock
			rwLock.writeLock().unlock();
			return new Result(Result.Error, -1, "Instrument record not found.");
		}
		
		if (!subscription.get(inst).contains(c)) {
			//unlock
			rwLock.writeLock().unlock();
			return new Result(Result.Error, -1, "Not subscribe the instrument yet.");
		}
		
		// Remove subscription record
		subscription.get(inst).remove(c);
		return new Result();
	}
	
	public Result onMarketData(MarketData d) {
		Result r = sendData(d.InstrumentID, MarketData.DataType, d);
		dataKeeper.onMarketData(d);
		return r;
	}
	
	public Result onCandle(Candle c) {
		Result r = sendData(c.InstrumentID, Candle.DataType, c);
		dataKeeper.onCandle(c);
		return r;
	}
	
	public void sendHistoryData(String inst, Channel c, int number) {
		// Get all periods
		Set<Integer> periods = dataKeeper.getCandlePeriods(inst);
		if (periods.size() < 1) {
			LOG.warning("Periods not found, " + inst);
			return;
		}
		
		// Get candles for each period
		for (int p : periods) {
			List<Candle> candles = dataKeeper.getCandles(inst, p, number);
			if (candles == null || candles.size() < 1) {
				LOG.warning("Candles not found, " + inst);
				continue;
			}
			JSONObject[] arr = new JSONObject[candles.size()];
			
			// Set array
			int i = 0;
			for (Candle cd : candles) {
				arr[i++] = cd.ToJSON();
			}
			
			// Send data
			String msg = wrapData(Candle.DataType, sequence.incrementAndGet(), arr);
			sendChannelData(c, msg);	
		}
		
		// Send market data
		List<MarketData> l = dataKeeper.queryMarketData(inst, 10);
		if (l.size() > 0) {
			// Prepare JSON
			JSONObject[] arr = new JSONObject[l.size()];
			int i = 0;
			for (MarketData d : l) {
				arr[i++] = d.ToJSON();
			}
			
			// Get JSON string
			String msg = wrapData(MarketData.DataType, sequence.incrementAndGet(), arr);
			sendChannelData(c, msg);
		}
	}
	
	protected void sendChannelData(Channel c, String msg) {
		try {
			boolean ret = c.writeAndFlush(new TextWebSocketFrame(msg)).await(SendTimeoutMillis);
			if (!ret) {
				LOG.warning("Sending timeout, CLIENT: " + c);
			}
		} catch (InterruptedException e) {
			LOG.warning("Sending data failed, " + c + ", " + e.getMessage());
		}
	}
	
	protected Result sendData(String inst, String type, TransferredData<?> d) {
		JSONObject[] arr = new JSONObject[1];
		arr[0] = d.ToJSON();
		return broadcast(inst, wrapData(type, sequence.incrementAndGet(), arr));
	}
	
	/**
	 * Server-sent market data format,
	 * <pre>
	 * {
	 * 	 "sequence" : 123456,
	 *	 "type" : "marketdata",
	 *	 "data" : [{}, {}]
	 * }
	 * </pre>
	 */
	protected String wrapData(String Type, long Sequence, JSONObject[] Data) {
		JSONObject obj = new JSONObject();
		JSONArray arr = new JSONArray();
		obj.put(TypeTag, Type);
		obj.put(SequenceTag, Sequence);
		for (JSONObject single : Data) {
			arr.put(single);
		}
		obj.put(DataTag, arr);
		return obj.toString(0);
	}
	
	protected Result broadcast(String inst, String msg) {
		Result res = new Result();
		rwLock.readLock().lock();
		
		// Check if instrument recod found
		if (!subscription.containsKey(inst)) {
			//unlock
			rwLock.readLock().unlock();
			return new Result(Result.Success, 0, "No subscription.");
		}
		
		// Get the ChannelGroup
		ChannelGroup g = subscription.get(inst);
		if (g == null) {
			//unlock
			rwLock.readLock().unlock();
			return new Result(Result.Error, -1, "Subscription null ref.");
		}
		
		// Write data to group
		try {
			boolean ret = g.writeAndFlush(new TextWebSocketFrame(msg)).await(SendTimeoutMillis);
			if (!ret) {
				res = new Result(Result.Error, -1, "Sending timeout, " + inst + ", msg length: " + msg.length() + " bytes.");
			}
		} catch (InterruptedException e) {
			res = new Result(Result.Error, -1, "Sending data failed, " + e.getMessage());
		}
		
		// unlock
		rwLock.readLock().unlock();
		return res;
	}
	
	public void closeAll() {
		for (ChannelGroup g : subscription.values()) {
			g.close();
		}
	}
}

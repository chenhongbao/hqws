package flyingbot.hq.ws.system;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import dmkp.common.util.Common;
import flyingbot.data.hq.Candle;
import flyingbot.data.hq.MarketData;

public class HQDataKeeper {
	
	class InstCandlePack {
		
		// Default cached market data number
		public static final int DefaultCachedMarketData = 10;
		
		// Older data to left, newer data to right. Send data client in this order,
		// so client will receive older data earlier than newer.
		//
		// Candle cache, key: period in minutes, value: candle list
		HashMap<Integer, LinkedList<Candle>> candles;
		
		// Market data cache
		LinkedList<MarketData> mds;
		
		// Sync
		ReadWriteLock wrLock, wrLock0;
		
		public InstCandlePack() {
			wrLock = new ReentrantReadWriteLock();
			wrLock0 = new ReentrantReadWriteLock();
			mds = new LinkedList<MarketData>();
			candles = new HashMap<Integer, LinkedList<Candle>>();
		}
		
		/**
		 * Query candles. Return all candles if available candles are less than those requested.
		 * @param period Period in minutes
		 * @param reversedNumber Number of candles return
		 * @return Candle list
		 */
		public List<Candle> queryCandle(int period, int reversedNumber) {
			// Always return an instance, prohibit null exception
			List<Candle> ret = new LinkedList<Candle>();
			
			// Sync
			wrLock.readLock().lock();
			
			// Get candles in the period
			if (candles.containsKey(period)) {
				LinkedList<Candle> lst = candles.get(period);
				if (lst.size() < 1) {
					wrLock.readLock().unlock();
					return ret;
				}
				
				// Calculate the number of candles return
				int len = Math.min(lst.size(), reversedNumber); 
				
				// subList([inclusive], [exclusive])
				ret.addAll(candles.get(period).subList(lst.size() - len, lst.size()));
			}
			
			// unlock
			wrLock.readLock().unlock();
			return ret;
		}
		
		public void insertCandle(Candle Cnd) {
			// Sync
			wrLock.writeLock().lock();
			
			// Create candle list if not exists
			if (!candles.containsKey(Cnd.Period)) {
				candles.put(Cnd.Period, new LinkedList<Candle>());
			}
			
			// Mark if candle has been inserted before the last
			boolean inserted = false;
			
			// Insert candles, assuming they have been sorted.
			LinkedList<Candle> lst = candles.get(Cnd.Period);
			if (lst.size() < 1) {
				lst.add(Cnd);
				wrLock.writeLock().unlock();
				return;
			}
			for (int i = lst.size() - 1; i >= 0; --i) {
				if (Cnd.SerialNo.compareTo(lst.get(i).SerialNo) == 0) {
					// Repeated candle
					wrLock.writeLock().unlock();
					return;
				}
				
				// Newer to the right, older to the left
				if (Cnd.SerialNo.compareTo(lst.get(i).SerialNo) > 0) {
					lst.add(i + 1, Cnd);
					inserted = true;
					break;
				}
			}
			
			// Candle not inserted, append it to the front
			if (!inserted) {
				lst.addFirst(Cnd);
			}
			wrLock.writeLock().unlock();
		}
		
		public List<MarketData> queryMarketData(int Number) {
			List<MarketData> ret = new LinkedList<MarketData>();
			if (Number < 1) {
				return ret;
			}
			
			// Sync
			wrLock0.readLock().lock();
			ret.addAll(mds.subList(0, Math.min(mds.size(), Number)));
			wrLock0.readLock().unlock();
			return ret;
		}
		
		public void insertMarketData(MarketData Md) {
			if (Md == null) {
				return;
			}
			wrLock0.writeLock().lock();
			while (mds.size() >= DefaultCachedMarketData) {
				// FIFO
				mds.removeFirst();
			}
			
			// Add element to the back, send to client from left to right, old to new
			mds.addLast(Md);
			wrLock0.writeLock().unlock();
		}
		
		public Set<Integer> getCandlePeriods() {
			HashSet<Integer> ret = new HashSet<Integer>();
			ret.addAll(candles.keySet());
			return ret;
		}
	}
	
	// Load info from JSON config
	String URL, userName, password, connStr;

	// Database connection and statement
	Connection connDB = null;
	PreparedStatement statementDB = null;
	
	// Logger instance
	Logger LOG = null;
	
	// lock
	ReadWriteLock lock;
	
	// Candle cache
	Map<String, InstCandlePack> instPacks;
	
	// Timestamp for last access DB
	long _LastAccessDB = 0;
	public static long _ReconnectMillis = 1000 * 60 * 60;
	
	static String _QuerySql = "SELECT `JSON` FROM `candledb`.`candle_01` "
			+ "WHERE `InstrumentID` = ? AND `Period` = ? "
			+ "ORDER BY `SerialNo` DESC LIMIT ?";

	public HQDataKeeper(Logger Log) {
		LOG = Log;
		lock = new ReentrantReadWriteLock();
		instPacks = new HashMap<String, InstCandlePack>();
		try {
			loadConfiguration();
		} catch (Exception e) {
			LOG.severe("º”‘ÿ¿Ø÷Úœﬂ ˝æ›ø‚≈‰÷√¥ÌŒÛ£¨" + e.getMessage());
		}
	}
	
	public boolean removeInstPack(String inst) {
		lock.writeLock().lock();
		boolean ret = instPacks.remove(inst) != null;
		lock.writeLock().unlock();	
		return ret;
	}
	
	public List<MarketData> queryMarketData(String InstrumentID, int Number) {
		boolean has = false;
		
		// Check if market data record exists
		lock.readLock().lock();
		has = instPacks.containsKey(InstrumentID);
		lock.readLock().unlock();
		
		if (!has) {
			lock.writeLock().lock();
			
			// Reconfirm
			if (!instPacks.containsKey(InstrumentID)) {
				instPacks.put(InstrumentID, new InstCandlePack());
			}
			
			lock.writeLock().unlock();
		}
		
		// read lock
		lock.readLock().lock();
		List<MarketData> lst = instPacks.get(InstrumentID).queryMarketData(Number);
		lock.readLock().unlock();
		
		return lst;
	}
	
	public Set<Integer> getCandlePeriods(String inst) {
		// Return default values
		Set<Integer> r = new HashSet<Integer>();
		r.add(1);
		r.add(5);
		r.add(15);
		r.add(60);
		r.add(1440);
		return r;
	}
	
	public List<Candle> getCandles(String InstrumentID, int Period, int ReversedNumber) {
		InstCandlePack icp = null;
		
		// Check id record exists
		lock.readLock().lock();
		icp = instPacks.get(InstrumentID);
		lock.readLock().unlock();
		
		// Create candle record if not exists
		if (icp == null) {
			lock.writeLock().lock();
			// Re-confirm
			if (!instPacks.containsKey(InstrumentID)) {
				instPacks.put(InstrumentID, new InstCandlePack());
			}
			
			// Get instance again
			icp = instPacks.get(InstrumentID);
			lock.writeLock().unlock();
		}
		
		// Query candles
		List<Candle> lst = icp.queryCandle(Period, ReversedNumber);
		
		// Have enough data
		if (lst != null && lst.size() >= ReversedNumber) {
			return lst;
		}
		else {
			// Not enough data, query from DB
			lst = loadCandleFromDB(InstrumentID, Period, ReversedNumber);
			
			if (lst != null) {
				// Merge newly queried data into cache
				// (cache may have been removed at this point, but it doesn't matter)
				for (Candle cnd : lst) {
					icp.insertCandle(cnd);
				}
			}
			
			// Look up candle from cache again.
			// If the original cache has been removed,it will still do query 
			// and send candles to client, but it won't put it to cache.
			lst = icp.queryCandle(Period, ReversedNumber);
			
			return lst;
		}
	}
	
	public void onCandle(Candle Cnd) {
		// Sync
		lock.writeLock().lock();
		if (!instPacks.containsKey(Cnd.InstrumentID)) {
			instPacks.put(Cnd.InstrumentID, new InstCandlePack());
		}
		lock.writeLock().unlock();
		
		// Sync
		lock.readLock().lock();
		InstCandlePack icp = instPacks.get(Cnd.InstrumentID); 
		lock.readLock().unlock();
		
		// Update candle
		icp.insertCandle(Cnd);	
	}
	
	public void onMarketData(MarketData Md) {
		// Check if record exists
		lock.writeLock().lock();
		if (!instPacks.containsKey(Md.InstrumentID)) {
			// Create new instance
			instPacks.put(Md.InstrumentID, new InstCandlePack());
		}
		lock.writeLock().unlock();
		
		
		// Sync
		lock.readLock().lock();
		InstCandlePack icp = instPacks.get(Md.InstrumentID); 
		lock.readLock().unlock();
		
		// Update market data
		icp.insertMarketData(Md);	
	}
	
	synchronized private List<Candle> loadCandleFromDB(String InstrumentID, int Period, int ReversedNumber) {
		List<Candle> lst = null;
		try {
			// Connect database if not connected
			connectDatabase();
			
			// Set params
			statementDB.setString(1, InstrumentID);
			statementDB.setInt(2, Period);
			statementDB.setInt(3, ReversedNumber);
			ResultSet rs = statementDB.executeQuery();
			
			// Create return
			lst = new LinkedList<Candle>();
			while (rs.next()) {
				String text = rs.getString(1);
				Candle cnd = Candle.Parse(new JSONObject(text));
				lst.add(cnd);
			}
			
			// Update timestamp
			_LastAccessDB = System.currentTimeMillis();
			
			LOG.info("Load candles " + InstrumentID + ", PERIOD: " + Period + "m, TOTAL: " + ReversedNumber);
		} catch (SQLException e) {
			LOG.severe("Query candles from DB failed, " + InstrumentID + ", PERIOD: " + Period + "m, " + e.getMessage());
		} catch (JSONException e) {
			LOG.severe("Parse candle JSON failed, " + e.getMessage() + ", " + e.getMessage());
		}
		return lst;
	}
	
	private void loadConfiguration() throws Exception {
		JSONObject obj = Common.LoadJSONObject(this.getClass().getResourceAsStream("candledb_addr.json"));
		if (obj.has("URL") && obj.has("Username") && obj.has("Password")) {
			URL = obj.getString("URL");
			userName = obj.getString("Username");
			password = obj.getString("Password");
			connStr = URL
					+ "?characterEncoding=utf8&useSSL=false"
					+ "&serverTimezone=UTC&rewriteBatchedStatements=true";
		}
		Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
	}

	private void connectDatabase() throws SQLException {
		// Check if we need re-connect
		long cur = System.currentTimeMillis();
		if (cur - _LastAccessDB > HQDataKeeper._ReconnectMillis) {
			resetDatabase();
			_LastAccessDB = cur;
			return;
		}
	}
	
	private void resetDatabase() throws SQLException {
		if (connDB == null || connDB.isClosed()) {
			initDatabase();
			return;
		}
		if (statementDB != null && !statementDB.isClosed())
		{
			statementDB.close();
		}
		connDB.close();
		initDatabase();
	}

	private void initDatabase() throws SQLException {
		connDB = DriverManager.getConnection(connStr, userName, password);
		statementDB = connDB.prepareStatement(_QuerySql);
	}
}

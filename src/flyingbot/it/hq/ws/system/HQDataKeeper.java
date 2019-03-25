package flyingbot.it.hq.ws.system;

import flyingbot.it.data.hq.Candle;
import flyingbot.it.data.hq.MarketData;
import flyingbot.it.hq.ws.resources.Constants;
import flyingbot.it.util.Common;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static flyingbot.it.hq.ws.resources.Constants.*;

public class HQDataKeeper {

    public static long reconnectMillis = 1000 * 60 * 60;

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

	// Find product id from instrument id
	Pattern patt;

    // Timestamp for last access DB
    long _LastAccessDB = 0;
    static String querySql = "SELECT `JSON` FROM `candledb`.`candle_01` "
            + "WHERE `InstrumentID` = ? AND `Period` = ? "
            + "ORDER BY `SerialNo` DESC";
	/**
	 * Dominant instruments.
	 * ProductID -> InstrumentID
	 */
	ConcurrentHashMap<String, String> domiInsts;

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
		} else {
			// Not enough data and hasn't fetched DB, query from DB
            if (!icp.hasFetchedDB(Period)) {
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

				// set mark
                icp.hasFetchedDB(Period, true);
			}

			return lst;
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

	synchronized private List<Candle> loadCandleFromDB(String InstrumentID, int Period, int ReversedNumber) {
		List<Candle> lst = null;
		try {
			// Connect database if not connected
			connectDatabase();

			// Set params
			statementDB.setString(1, InstrumentID);
			statementDB.setInt(2, Period);

			// log start
			LOG.info("Start loading candles from DB, " + InstrumentID
                    + ", period: " + Period + "m, wanted: " + ReversedNumber);

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

			// log end
            LOG.info("Loaded candles " + InstrumentID + ", PERIOD: " + Period + "m, found: " + lst.size());
		} catch (SQLException e) {
			LOG.severe("Query candles from DB failed, " + InstrumentID + ", PERIOD: " + Period + "m, " + e.getMessage());
		} catch (JSONException e) {
			LOG.severe("Parse candle JSON failed, " + e.getMessage() + ", " + e.getMessage());
		}
		return lst;
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

	/**
	 * Open interest for current dominant instruments
	 */
	ConcurrentHashMap<String, Double> domiOpenIns;

	public HQDataKeeper(Logger Log) {
		LOG = Log;
		lock = new ReentrantReadWriteLock();
		instPacks = new HashMap<>();
		domiInsts = new ConcurrentHashMap<>();
		domiOpenIns = new ConcurrentHashMap<>();
		patt = Pattern.compile("[0-9]+$");
		try {
			loadConfiguration();
		} catch (Exception e) {
			LOG.severe("Loading configuration failed, " + e.getMessage());
		}
	}

	public void updateDominantInstrument(MarketData md) {
		Matcher m = patt.matcher(md.InstrumentID);
		if (!m.matches()) {
			return;
		}

		// Find product id
		String pid = md.InstrumentID.substring(0, m.start());
		if (!domiInsts.containsKey(pid) || domiOpenIns.get(pid) < md.OpenInterest) {
			domiInsts.put(pid, md.InstrumentID);
			domiOpenIns.put(pid, md.OpenInterest);
		}
	}

	public String getDominantInstrument(String productID) {
		if (!domiInsts.containsKey(productID)) {
			return "";
		} else {
			return domiInsts.get(productID);
		}
	}

    private void connectDatabase() throws SQLException {
        // Check if we need re-connect
        long cur = System.currentTimeMillis();
        if (cur - _LastAccessDB > HQDataKeeper.reconnectMillis) {
            resetDatabase();
            _LastAccessDB = cur;
            return;
        }
    }

    private void loadConfiguration() throws Exception {
		JSONObject obj = Common.LoadJSONObject(Constants.class.getResourceAsStream("candledb_addr.json"));
		if (obj.has(ConfigTag_URL) && obj.has(ConfigTag_User) && obj.has(ConfigTag_Pwd)) {
			URL = obj.getString(ConfigTag_URL);
			userName = obj.getString(ConfigTag_User);
			password = obj.getString(ConfigTag_Pwd);
            connStr = URL
                    + "?characterEncoding=utf8&useSSL=false"
                    + "&serverTimezone=UTC&rewriteBatchedStatements=true";
        }
        Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
    }

    private void initDatabase() throws SQLException {
        connDB = DriverManager.getConnection(connStr, userName, password);
        statementDB = connDB.prepareStatement(querySql);
    }

    private void resetDatabase() throws SQLException {
        if (connDB == null || connDB.isClosed()) {
            initDatabase();
            return;
        }
        if (statementDB != null && !statementDB.isClosed()) {
            statementDB.close();
        }
        connDB.close();
        initDatabase();
    }

	class InstCandlePack {
		/*
		 * Older data to left, newer data to right. Send data client in this order,
		 * so client will receive older data earlier than newer.
		 * Candle cache, key: period in minutes, value: candle list
		 */
        HashMap<Integer, ConcurrentSkipListSet<Candle>> candles;

		// Market data cache
		LinkedList<MarketData> mds;

		// Sync
		ReadWriteLock wrLock, wrLock0;

		// has been updated from db
        ConcurrentHashMap<Integer, Boolean> hasFetchedDB;

		public InstCandlePack() {
			wrLock = new ReentrantReadWriteLock();
			wrLock0 = new ReentrantReadWriteLock();

			// container
			mds = new LinkedList<MarketData>();
            candles = new HashMap<Integer, ConcurrentSkipListSet<Candle>>();

            // hasn't fetched data from DB
            this.hasFetchedDB = new ConcurrentHashMap<Integer, Boolean>();
		}

		/**
		 * Query candles. Return all candles if available candles are less than those requested.
         * Older data to left, newer to right.
		 * @param period Period in minutes
		 * @param reversedNumber Number of candles return
		 * @return Candle list
		 */
		public List<Candle> queryCandle(int period, int reversedNumber) {
			// Always return an instance, prohibit null exception
            LinkedList<Candle> ret = new LinkedList<Candle>();

			// Sync
			wrLock.readLock().lock();

			// Get candles in the period
			if (candles.containsKey(period)) {
                ConcurrentSkipListSet<Candle> lst = candles.get(period);
				if (lst.size() < 1) {
					wrLock.readLock().unlock();
					return ret;
				}

				// Calculate the number of candles return
				int len = Math.min(lst.size(), reversedNumber);

                // sub_set([inclusive], [exclusive])
                Iterator<Candle> desc_iter = lst.descendingIterator();
                while (--len >= 0 && desc_iter.hasNext()) {
                    ret.addFirst(desc_iter.next());
                }
			}

			// unlock
			wrLock.readLock().unlock();
			return ret;
		}

        public void hasFetchedDB(int period, boolean h) {
            this.hasFetchedDB.put(period, h);
		}

        public boolean hasFetchedDB(int period) {
            return this.hasFetchedDB.containsKey(period) && this.hasFetchedDB.get(period);
		}

		public void insertCandle(Candle Cnd) {
			// Sync
			wrLock.writeLock().lock();

			// Create candle list if not exists
			if (!candles.containsKey(Cnd.Period)) {
                candles.put(Cnd.Period, new ConcurrentSkipListSet<Candle>());
			}

            // Insert candles to sorted set
            // The candle provides comparable interface that put older data to left and newer to right.
            candles.get(Cnd.Period).add(Cnd);

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
			while (mds.size() >= DefaultCachedMarketData_Num) {
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
}

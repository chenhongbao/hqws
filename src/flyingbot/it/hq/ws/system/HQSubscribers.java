package flyingbot.it.hq.ws.system;

import flyingbot.it.data.hq.Candle;
import flyingbot.it.data.hq.MarketData;
import flyingbot.it.data.hq.TransferredData;
import flyingbot.it.util.Common;
import flyingbot.it.util.Result;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class HQSubscribers {

    // JSON format constant
    public final static String DataTag = "data";
    public final static String TypeTag = "type";
    public final static String SequenceTag = "sequence";
    // JSON type field, marking the data is historical
    public final static String OldCandleType = "OldCandle";
    public final static String OldMarketDataType = "OldMarketData";
    // Send timeout
    public final static int SendTimeoutMillis = 5000;
    // LRU size
    public final static int LRUSize = 50;
    // Heartbeat message
    public final static String HeartbeatMsg = "{\"sequence\":0,\"type\":\"Heartbeat\",\"data\":[]}";
    // Market data keeper
    public HQDataKeeper dataKeeper;
    // Subscription group
    protected ReentrantReadWriteLock rwLock;
    protected HashMap<String, ChannelGroup> subscription;
    // LRU
    protected ModifiedLRU lru;
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

        // LRU
        lru = new ModifiedLRU(LRUSize);

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

            // Refresh LRU
            refreshLRU(inst);

            return new Result();
        } else {
            // unlock
            rwLock.writeLock().unlock();
            return new Result(Result.Error, -1, "Duplecated subscription.");
        }
    }

    protected void refreshLRU(String inst) {
        if (!lru.contains(inst)) {
            LOG.info("New cache: " + inst);
        }

        // Refresh LRU
        String r = lru.refreshInst(inst);
        if (r != null) {
            boolean ret = dataKeeper.removeInstPack(r);
            if (ret) {
                LOG.info("Remove " + r + " from cache.");
            } else {
                LOG.warning("Remove " + r + " from cache failed.");
            }
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
        // Forward data
        Result r = sendData(d.InstrumentID, MarketData.DataType, d);

        // Only update data that is marked in LRU
        if (lru.contains(d.InstrumentID)) {
            dataKeeper.onMarketData(d);
        }
        return r;
    }

    public Result onCandle(Candle c) {
        Result r = sendData(c.InstrumentID, Candle.DataType, c);
        if (lru.contains(c.InstrumentID)) {
            dataKeeper.onCandle(c);
        }
        return r;
    }

    public void sendHistoryData(String inst, Channel c, int number) {
        // Get all periods
        Set<Integer> periods = dataKeeper.getCandlePeriods(inst);
        if (periods.size() < 1) {
            LOG.warning("Periods not found, " + inst);
            return;
        }

        try {
            // Get candles for each period
            for (int p : periods) {
                // Change number of candles to send.
                // Day candles are a lot less than 1m candles.
                int num = number;

                // 240 days a year
                if (p == 1440 && number > 240) {
                    num = 240;
                }

                // 6 housr each day
                if (p == 60 && number > 1440) {
                    num = 1440;
                }

                // 4 quarters each hour
                if (p == 15 && number > 5760) {
                    num = 5760;
                }

                // 3 5-minutes each quarter
                if (p == 5 && number > 17280) {
                    num = 17280;
                }

                // Query candles
                List<Candle> candles = dataKeeper.getCandles(inst, p, num);
                if (candles == null || candles.size() < 1) {
                    LOG.warning("Candles not found, " + inst);
                    continue;
                }

                // Prepare array
                JSONObject[] arr = new JSONObject[candles.size()];

                // Set array
                int i = 0;
                for (Candle cd : candles) {
                    arr[i++] = cd.ToJSON();
                }

                // Send data
                String msg = wrapData(OldCandleType, sequence.incrementAndGet(), arr);
                sendChannelData(c, msg);

                // log
                LOG.info("Sent candles," + inst + "(" + p + "m, " + candles.size() + ")" + " to " + c);
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
                String msg = wrapData(OldMarketDataType, sequence.incrementAndGet(), arr);
                sendChannelData(c, msg);

                // log
                LOG.info("Sent mds," + inst + "(" + l.size() + ")" + " to " + c);
            }
        } catch (Exception e) {
            // log exception
            LOG.severe(e.getMessage());
            Common.PrintException(e);
        }
    }

    protected void sendChannelData(Channel c, String msg) {
        try {
            c.writeAndFlush(new TextWebSocketFrame(msg));
        } catch (Exception e) {
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
     * 	 "type" : "marketdata",
     * 	 "data" : [{}, {}]
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
            g.writeAndFlush(new TextWebSocketFrame(msg));

        } catch (Exception e) {
            res = new Result(Result.Error, -1, "Sending data failed, " + e.getMessage());
        }

        // unlock
        rwLock.readLock().unlock();
        return res;
    }

    public Result SendHeartbeatAll() {
        Result res = new Result();
        rwLock.readLock().lock();

        for (ChannelGroup g : subscription.values()) {
            if (g == null) {
                continue;
            }

            try {
                g.writeAndFlush(new TextWebSocketFrame(HQSubscribers.HeartbeatMsg));
            } catch (Exception e) {
                res = new Result(Result.Error, -1, "Sending heartbeat failed, " + e.getMessage());
                break;
            }
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

    class ModifiedLRU {
        // Subscribed instruments
        protected HashSet<String> instSet;

        // LRU list
        protected LinkedList<String> instList;
        protected ReentrantReadWriteLock lock;

        // Cache size
        int size;

        public ModifiedLRU(int size) {
            if (size < 1) {
                size = 1;
            }
            this.size = size;
            instSet = new HashSet<String>();
            instList = new LinkedList<String>();
            lock = new ReentrantReadWriteLock();
        }

        /**
         * Refresh LRU list and return the removed element (outbound size)
         *
         * @param inst Instrument
         * @return Instrument which is removed out of the list or null if nothing performed
         */
        public String refreshInst(String inst) {
            String ret = null;
            lock.writeLock().lock();

            // New instrument
            if (!instSet.contains(inst)) {
                // Check if needs remove the last element
                if (instList.size() >= this.size) {
                    ret = instList.pollLast();
                    instSet.remove(ret);
                }

                // Update list
                instSet.add(inst);
                instList.addLast(inst);
            } else {
                int index = -1;
                // Remove the current element
                for (int i = 0; i < instList.size(); ++i) {
                    if (instList.get(i).compareToIgnoreCase(inst) == 0) {
                        instList.remove(i);
                        index = i;
                        break;
                    }
                }

                // Move element to left
                if (index <= 0) {
                    index = 0;
                } else {
                    --index;
                }
                instList.add(index, inst);
            }

            lock.writeLock().unlock();
            return ret;
        }

        public boolean contains(String inst) {
            boolean ret = false;
            lock.readLock().lock();
            ret = instSet.contains(inst);
            lock.readLock().unlock();
            return ret;
        }

        public List<String> getAll() {
            List<String> ret = new ArrayList<String>();
            lock.readLock().lock();
            ret.addAll(instList);
            lock.readLock().unlock();
            return ret;
        }
    }
}

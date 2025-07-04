# shared/constants.py

# Queue-related constants
class Exchanges:
    MARKET_DATA = "market_data"
    STRATEGY = "strategy"
    EXECUTION = "execution"
    SYSTEM = "system"

class Queues:
    EXTERNAL_DATA = "external_data"
    CANDLES = "candles_data"
    EVENTS = "data_events"
    SIGNALS = "strategy_signals"
    ORDERS = "execution_orders"
    SYSTEM_EVENTS = "system_events"
    TEST_DATA = "test_data"
    TEST_RESULTS = "test_results"

class RoutingKeys:
    # Market data
    EXTERNAL_NEW = "external.new.{exchange}.{symbol}.{timeframe}"
    CANDLE_NEW = "candle.new.{exchange}.{symbol}.{timeframe}"
    CANDLE_ALL = "candle.new.#"
    DATA_EVENT_HANDLE = "data.event.#"
    
    # Strategy
    SIGNAL_ALL = "signal.#"
    ORDER_BLOCK_DETECTED = "signal.orderblock.detected.{exchange}.{symbol}.{timeframe}"
    ORDER_EXECUTED = "signal.order.executed.{exchange}.{symbol}"
    
    # Execution - Simplified to exchange and symbol level
    ORDER_NEW = "order.new.{exchange}.{symbol}"
    ORDER_CANCELLED = "order.cancelled.{exchange}.{symbol}"
    ORDER_FAILED = "order.failed.{exchange}.{symbol}"
    ORDER_ALL = "order.#"
    
    # System
    SYSTEM_ALERT = "system.alert"
    SYSTEM_HEARTBEAT = "system.heartbeat"

# Cache-related constants
class CacheKeys:
    # Market data
    CANDLE_HISTORY_REST_API_DATA = "historical:candle:{exchange}:{symbol}:{timeframe}"
    CANDLE_LIVE_WEBSOCKET_DATA = "live:candle:{exchange}:{symbol}:{timeframe}"
    CANDLE_LAST_UPDATED = "candle:last_updated:{exchange}:{symbol}:{timeframe}"
    
    # Order blocks
    ORDER_BLOCK = "ob:{exchange}:{symbol}:{timeframe}:{id}"
    ORDER_BLOCKS_ACTIVE = "ob:{exchange}:{symbol}:active"
    
    # Signals
    SIGNAL = "signal:{exchange}:{symbol}:{id}"
    ACTIVE_SIGNALS_HASH = "signals:{exchange}:{symbol}:active"
    
    # Orders
    ORDER = "order:{exchange}:{symbol}:{order_id}"
    ACTIVE_ORDERS = "orders:{exchange}:{symbol}:active"
    
    # Market state
    MARKET_STATE = "market:{exchange}:{symbol}:{timeframe}:state"

# Time-to-live (TTL) constants (in seconds)
class CacheTTL:
    MINUTE = 60
    HOUR = 60 * 60
    DAY = 24 * HOUR
    WEEK = 7 * DAY
    MONTH = 30 * DAY
    
    # Specific TTLs
    CANDLE_DATA = 1 * HOUR
    MARKET_STATE = 7 * DAY
    ORDER_DATA = 30 * DAY
    SIGNAL_DATA = 7 * DAY
    HEARTBEAT = 5 * MINUTE
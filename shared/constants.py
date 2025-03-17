# shared/constants.py

# Queue-related constants
class Exchanges:
    MARKET_DATA = "market_data"
    STRATEGY = "strategy"
    EXECUTION = "execution"
    SYSTEM = "system"

class Queues:
    CANDLES = "candles_data"
    SIGNALS = "strategy_signals"
    ORDERS = "execution_orders"
    SYSTEM_EVENTS = "system_events"
    TEST_DATA = "test_data"
    TEST_RESULTS = "test_results"

class RoutingKeys:
    # Market data
    CANDLE_NEW = "candle.new.{timeframe}"
    CANDLE_ALL = "candle.new.#"
    TRADE_NEW = "trade.new"
    
    # Strategy
    SIGNAL_NEW = "signal.new"
    ORDER_BLOCK_DETECTED = "orderblock.detected"
    
    # Execution
    ORDER_NEW = "order.new"
    ORDER_EXECUTED = "order.executed"
    ORDER_CANCELLED = "order.cancelled"
    ORDER_FAILED = "order.failed"
    
    # System
    SYSTEM_ALERT = "system.alert"
    SYSTEM_HEARTBEAT = "system.heartbeat"

# Cache-related constants
class CacheKeys:
    # Market data
    LATEST_CANDLE = "candle:{symbol}:{timeframe}:latest"
    CANDLE_HISTORY_SET = "candle:{symbol}:{timeframe}:history"
    CANDLE_DATA = "candle:{symbol}:{timeframe}:{timestamp}"
    
    # Order blocks
    ORDER_BLOCK = "ob:{symbol}:{timeframe}:{id}"
    ORDER_BLOCKS_ACTIVE = "ob:{symbol}:active"
    
    # Signals
    SIGNAL = "signal:{id}"
    ACTIVE_SIGNALS_HASH = "signals:active"
    
    # Orders
    ORDER = "order:{order_id}"
    ACTIVE_ORDERS = "orders:{symbol}:active"
    
    # Market state
    MARKET_STATE = "market:{symbol}:state"
    
    # System
    SYSTEM_STATUS = "system:status"
    LAST_HEARTBEAT = "system:{service}:last_heartbeat"

# Time-to-live (TTL) constants (in seconds)
class CacheTTL:
    MINUTE = 60
    HOUR = 60 * 60
    DAY = 24 * HOUR
    WEEK = 7 * DAY
    MONTH = 30 * DAY
    
    # Specific TTLs
    CANDLE_DATA = 7 * DAY
    MARKET_STATE = 1 * DAY
    ORDER_DATA = 30 * DAY
    SIGNAL_DATA = 7 * DAY
    HEARTBEAT = 5 * MINUTE
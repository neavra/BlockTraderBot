import logging

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackContext, CallbackQueryHandler

from data.alert import Alert
from order_manager import OrderManager
from position_manager import PositionManager

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class TelegramBot:
    """
    Telegram Bot for trading system monitoring.
    
    This bot sends alerts to users, provides information about positions and orders,
    and responds to user commands.
    """
    
    def __init__(self, token: str, chat_id: str):
        """
        Initialize the Telegram bot.
        
        Args:
            token: The Telegram bot token obtained from BotFather
            chat_id: The chat ID where messages will be sent
        """
        self.token = token
        self.chat_id = chat_id
        self.application = Application.builder().token(token).build()
        self.order_manager = OrderManager()
        self.position_manager = PositionManager()
        
        # Register command handlers
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("orders", self.orders_command))
        self.application.add_handler(CommandHandler("positions", self.positions_command))
        self.application.add_handler(CommandHandler("status", self.status_command))
        
        # Register callback query handler
        self.application.add_handler(CallbackQueryHandler(self.button_callback))
        
        logger.info("Telegram bot initialized")
    
    async def start_command(self, update: Update, context: CallbackContext) -> None:
        """Send a welcome message when the command /start is issued."""
        user = update.effective_user
        await update.message.reply_html(
            f"Hi {user.mention_html()}! I'm your Trading Bot Monitor. "
            f"Use /help to see what I can do."
        )
    
    async def help_command(self, update: Update, context: CallbackContext) -> None:
        """Send a message when the command /help is issued."""
        help_text = (
            "🤖 *Trading Bot Monitor Help* 🤖\n\n"
            "Available commands:\n"
            "/start - Start the bot\n"
            "/help - Show this help message\n"
            "/orders - View your active orders\n"
            "/positions - View your open positions\n"
            "/status - View system status\n"
        )
        await update.message.reply_markdown(help_text)
    
    async def orders_command(self, update: Update, context: CallbackContext) -> None:
        """Display active orders when the command /orders is issued."""
        orders = self.order_manager.get_all_orders()
        
        if not orders:
            await update.message.reply_text("No active orders found.")
            return
        
        response = "*Active Orders:*\n\n"
        for order in orders:
            response += (
                f"*ID:* `{order.id}`\n"
                f"*Symbol:* {order.symbol}\n"
                f"*Side:* {order.side.upper()}\n"
                f"*Type:* {order.type.upper()}\n"
                f"*Price:* ${order.price:,.2f}\n"
                f"*Size:* {order.size}\n"
                f"*Status:* {order.status.upper()}\n"
                f"*Time:* {order.timestamp}\n\n"
            )
        
        keyboard = [
            [
                InlineKeyboardButton("Refresh Orders", callback_data="refresh_orders"),
                InlineKeyboardButton("View Positions", callback_data="view_positions")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_markdown(response, reply_markup=reply_markup)
    
    async def positions_command(self, update: Update, context: CallbackContext) -> None:
        """Display open positions when the command /positions is issued."""
        positions = self.position_manager.get_all_positions()
        
        if not positions:
            await update.message.reply_text("No open positions found.")
            return
        
        response = "*Open Positions:*\n\n"
        for position in positions:
            pnl_emoji = "📈" if position.pnl >= 0 else "📉"
            side_emoji = "🟢" if position.side == "long" else "🔴"
            
            response += (
                f"*Symbol:* {position.symbol}\n"
                f"*Side:* {side_emoji} {position.side.upper()}\n"
                f"*Size:* {position.size}\n"
                f"*Entry Price:* ${position.entry_price:,.2f}\n"
                f"*Current Price:* ${position.current_price:,.2f}\n"
                f"*P&L:* {pnl_emoji} ${position.pnl:,.2f} ({position.pnl_percent:+.2f}%)\n"
                f"*Opened:* {position.timestamp}\n\n"
            )
        
        keyboard = [
            [
                InlineKeyboardButton("Refresh Positions", callback_data="refresh_positions"),
                InlineKeyboardButton("View Orders", callback_data="view_orders")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_markdown(response, reply_markup=reply_markup)
    
    async def status_command(self, update: Update, context: CallbackContext) -> None:
        """Display system status when the command /status is issued."""
        orders = self.order_manager.get_all_orders()
        positions = self.position_manager.get_all_positions()
        
        status_text = (
            "🖥️ *System Status* 🖥️\n\n"
            "📊 *Summary:*\n"
            f"• Active Orders: {len(orders)}\n"
            f"• Open Positions: {len(positions)}\n"
            f"• System Status: Operational ✅\n\n"
            "💰 *Positions Overview:*\n"
        )
        
        total_pnl = sum(position.pnl for position in positions)
        pnl_emoji = "📈" if total_pnl >= 0 else "📉"
        
        status_text += f"• Total P&L: {pnl_emoji} ${total_pnl:,.2f}\n\n"
        
        for position in positions:
            side_emoji = "🟢" if position.side == "long" else "🔴"
            status_text += f"• {position.symbol}: {side_emoji} {position.size} @ ${position.entry_price:,.2f}\n"
        
        await update.message.reply_markdown(status_text)
    
    async def button_callback(self, update: Update, context: CallbackContext) -> None:
        """Handle button callbacks from inline keyboards."""
        query = update.callback_query
        await query.answer()
        
        if query.data == "refresh_orders":
            orders = self.order_manager.get_all_orders()
            response = "*Active Orders:*\n\n"
            for order in orders:
                response += (
                    f"*ID:* `{order.id}`\n"
                    f"*Symbol:* {order.symbol}\n"
                    f"*Side:* {order.side.upper()}\n"
                    f"*Type:* {order.type.upper()}\n"
                    f"*Price:* ${order.price:,.2f}\n"
                    f"*Size:* {order.size}\n"
                    f"*Status:* {order.status.upper()}\n"
                    f"*Time:* {order.timestamp}\n\n"
                )
            
            await query.edit_message_text(response, parse_mode="Markdown")
        
        elif query.data == "refresh_positions":
            positions = self.position_manager.get_all_positions()
            response = "*Open Positions:*\n\n"
            for position in positions:
                pnl_emoji = "📈" if position.pnl >= 0 else "📉"
                side_emoji = "🟢" if position.side == "long" else "🔴"
                
                response += (
                    f"*Symbol:* {position.symbol}\n"
                    f"*Side:* {side_emoji} {position.side.upper()}\n"
                    f"*Size:* {position.size}\n"
                    f"*Entry Price:* ${position.entry_price:,.2f}\n"
                    f"*Current Price:* ${position.current_price:,.2f}\n"
                    f"*P&L:* {pnl_emoji} ${position.pnl:,.2f} ({position.pnl_percent:+.2f}%)\n"
                    f"*Opened:* {position.timestamp}\n\n"
                )
            
            await query.edit_message_text(response, parse_mode="Markdown")
        
        elif query.data == "view_positions":
            # Don't call positions_command directly since we're in a callback context
            positions = self.position_manager.get_all_positions()
            
            if not positions:
                await query.edit_message_text("No open positions found.")
                return
            
            response = "*Open Positions:*\n\n"
            for position in positions:
                pnl_emoji = "📈" if position.pnl >= 0 else "📉"
                side_emoji = "🟢" if position.side == "long" else "🔴"
                
                response += (
                    f"*Symbol:* {position.symbol}\n"
                    f"*Side:* {side_emoji} {position.side.upper()}\n"
                    f"*Size:* {position.size}\n"
                    f"*Entry Price:* ${position.entry_price:,.2f}\n"
                    f"*Current Price:* ${position.current_price:,.2f}\n"
                    f"*P&L:* {pnl_emoji} ${position.pnl:,.2f} ({position.pnl_percent:+.2f}%)\n"
                    f"*Opened:* {position.timestamp}\n\n"
                )
            
            keyboard = [
                [
                    InlineKeyboardButton("Refresh Positions", callback_data="refresh_positions"),
                    InlineKeyboardButton("View Orders", callback_data="view_orders")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(response, parse_mode="Markdown", reply_markup=reply_markup)
        
        elif query.data == "view_orders":
            # Don't call orders_command directly since we're in a callback context
            orders = self.order_manager.get_all_orders()
            
            if not orders:
                await query.edit_message_text("No active orders found.")
                return
            
            response = "*Active Orders:*\n\n"
            for order in orders:
                response += (
                    f"*ID:* `{order.id}`\n"
                    f"*Symbol:* {order.symbol}\n"
                    f"*Side:* {order.side.upper()}\n"
                    f"*Type:* {order.type.upper()}\n"
                    f"*Price:* ${order.price:,.2f}\n"
                    f"*Size:* {order.size}\n"
                    f"*Status:* {order.status.upper()}\n"
                    f"*Time:* {order.timestamp}\n\n"
                )
            
            keyboard = [
                [
                    InlineKeyboardButton("Refresh Orders", callback_data="refresh_orders"),
                    InlineKeyboardButton("View Positions", callback_data="view_positions")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(response, parse_mode="Markdown", reply_markup=reply_markup)
    
    async def send_alert(self, alert: Alert) -> None:
        """
        Send an alert to the configured chat.
        
        Args:
            alert: The alert to send
        """
        message = (
            f"{alert.type.value}\n\n"
            f"*Symbol:* {alert.symbol}\n"
            f"*Time:* {alert.timestamp}\n"
            f"*Message:* {alert.message}\n"
        )
        
        # Add any additional details
        if alert.details:
            message += "\n*Details:*\n"
            for key, value in alert.details.items():
                message += f"• {key}: {value}\n"
        
        try:
            await self.application.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode="Markdown"
            )
            logger.info(f"Alert sent: {alert.type.name} for {alert.symbol}")
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
    
    async def fetch_positions(self) -> None:
        """Fetch and send current positions to the configured chat."""
        positions = self.position_manager.get_all_positions()
        
        if not positions:
            await self.application.bot.send_message(
                chat_id=self.chat_id,
                text="No open positions found."
            )
            return
        
        response = "*Current Positions:*\n\n"
        for position in positions:
            pnl_emoji = "📈" if position.pnl >= 0 else "📉"
            side_emoji = "🟢" if position.side == "long" else "🔴"
            
            response += (
                f"*Symbol:* {position.symbol}\n"
                f"*Side:* {side_emoji} {position.side.upper()}\n"
                f"*Size:* {position.size}\n"
                f"*Entry Price:* ${position.entry_price:,.2f}\n"
                f"*Current Price:* ${position.current_price:,.2f}\n"
                f"*P&L:* {pnl_emoji} ${position.pnl:,.2f} ({position.pnl_percent:+.2f}%)\n"
                f"*Opened:* {position.timestamp}\n\n"
            )
        
        await self.application.bot.send_message(
            chat_id=self.chat_id,
            text=response,
            parse_mode="Markdown"
        )
    
    async def fetch_orders(self) -> None:
        """Fetch and send current orders to the configured chat."""
        orders = self.order_manager.get_all_orders()
        
        if not orders:
            await self.application.bot.send_message(
                chat_id=self.chat_id,
                text="No active orders found."
            )
            return
        
        response = "*Current Orders:*\n\n"
        for order in orders:
            response += (
                f"*ID:* `{order.id}`\n"
                f"*Symbol:* {order.symbol}\n"
                f"*Side:* {order.side.upper()}\n"
                f"*Type:* {order.type.upper()}\n"
                f"*Price:* ${order.price:,.2f}\n"
                f"*Size:* {order.size}\n"
                f"*Status:* {order.status.upper()}\n"
                f"*Time:* {order.timestamp}\n\n"
            )
        
        await self.application.bot.send_message(
            chat_id=self.chat_id,
            text=response,
            parse_mode="Markdown"
        )
    
    def start(self) -> None:
        """Start the bot."""
        logger.info("Starting bot...")
        self.application.run_polling()
    
    async def stop(self) -> None:
        """Stop the bot."""
        logger.info("Stopping bot...")
        await self.application.stop()

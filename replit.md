# Telegram Bot "Academy" 

## Project Overview
This is an MVP Telegram bot for educational campaigns that supports three main scenarios:
- **Webinar Gift**: `/start <campaign>` - subscription check and gift distribution by campaign
- **Lottery Mailing**: `/lottery <campaign>` - mini-lottery with instant coupon distribution  
- **Interactive Predictions**: `/fortune <campaign>` - entertainment predictions with CTA to gifts

The bot integrates with Google Sheets for data storage and can operate in both polling and webhook modes.

## Current State
- Python 3.11 environment set up
- All dependencies installed from requirements.txt
- Project structure includes handlers, services, storage, keyboards, and utilities
- Configuration supports both polling and webhook modes
- Ready for environment configuration and deployment

## Architecture
```
app/
  main.py          # Entry point, mode selection (polling/webhook)
  bot.py           # Bot initialization and handler registration
  config.py        # Environment configuration
  handlers/        # Commands and scenarios (start, contacts, fun_interactive, etc.)
  services/        # Integrations (Google Sheets, coupons, stats, phone)
  storage/         # Local database for idempotency
  keyboards/       # Common keyboards
  utils/           # Helper utilities
```

## Key Features
- Phone number collection (via contact or text) with leads sheet recording
- Event and lead tracking in Google Sheets
- Coupon storage with idempotent distribution
- Admin commands (/ping, /report)
- Subscription checking for channel access
- FastAPI webhook endpoint for production deployment

## Environment Variables Required
- `TELEGRAM_BOT_TOKEN`: Bot token from BotFather
- `MODE`: "polling" (default) or "webhook"
- `WEBHOOK_URL`: Public URL for webhook mode
- `SECRET_TOKEN`: Security token for webhook verification
- `ADMIN_CHAT_ID`: Administrator chat ID(s) for notifications (comma-separated for fallback)
- `ALERTS_ENABLED`: Toggle system alerts on/off (`true`/`false`)
- `ALERTS_MENTION`: Optional mention (`@username`) prefixed to alerts
- `ALERTS_RATE_LIMIT`: Minimum interval between identical alerts (seconds)
- `ALERTS_BUNDLE_WINDOW`: Error aggregation window (seconds)
- `CHANNEL_USERNAME`: Channel for subscription verification
- `GOOGLE_SHEETS_ID`: Google Sheets table ID
- `GOOGLE_SERVICE_JSON_B64`: Base64 encoded service account JSON
- `PORT`: FastAPI port (default: 8000)

## Recent Changes
- Set up Python 3.11 environment
- Installed all project dependencies
- Ready for environment configuration
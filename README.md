# Telegram-бот "Academy"

MVP Telegram-бот для образовательной кампании. Поддерживает три сценария (вебинар-подарок, рассылка-лотерея и интерактив-предсказания), хранит события, лиды и купоны в Google Sheets и умеет работать в режимах polling и webhook.

## Возможности

- `/start <campaign>` — проверка подписки и выдача подарка по кампании.
- `/lottery <campaign>` — мини-лотерея с мгновенной выдачей купона.
- `/fortune <campaign>` — развлекательное предсказание с CTA на подарок.
- Приём телефонов (через контакт или текст) с записью в лист `leads` и уведомлением администратора.
- Учёт событий и лидов в листе `events`.
- Хранение купонов в листе `coupons` с идемпотентной выдачей.
- Админ-команды `/ping` и `/report`.

## Подготовка окружения

1. Создайте Google Service Account и выдайте доступ к нужной Google Sheets таблице.
2. Сохраните JSON ключ и закодируйте его в base64: `base64 -w0 key.json`.
3. Создайте `.env` файл на основе [.env.example](./.env.example).
4. Установите зависимости: `pip install -r requirements.txt`.

### Переменные окружения

| Переменная | Назначение |
|------------|------------|
| `TELEGRAM_BOT_TOKEN` | Токен Telegram-бота |
| `MODE` | `polling` (по умолчанию) или `webhook` |
| `WEBHOOK_URL` | Публичный URL для webhook (например, адрес Replit) |
| `SECRET_TOKEN` | Секрет для проверки заголовка `X-Telegram-Bot-Api-Secret-Token` |
| `ADMIN_CHAT_ID` | Чат ID администратора для уведомлений |
| `CHANNEL_USERNAME` | Канал для проверки подписки |
| `GOOGLE_SHEETS_ID` | ID таблицы Google Sheets |
| `GOOGLE_SERVICE_JSON_B64` | base64 от JSON ключа сервисного аккаунта |
| `PORT` | Порт запуска FastAPI (для webhook) |

## Запуск

### Режим polling (тестовый)

По умолчанию `MODE=polling`. При запуске бот удаляет webhook (`bot.delete_webhook(drop_pending_updates=True)`) и начинает опрос:

```bash
python -m app.main
```

### Режим webhook (боевой)

1. Установите `MODE=webhook`, задайте `WEBHOOK_URL` и `SECRET_TOKEN`.
2. Запустите: `python -m app.main`. При старте бот вызовет `setup_webhook()` (удаление старого webhook + установка нового с allowed_updates).
3. Проверьте `getWebhookInfo` в BotFather или через API.

### Аварийный откат

Для возврата к опросу: установите `MODE=polling`, запустите `python -m app.main`. Бот вызовет `deleteWebhook` и перейдёт в режим polling.

## Структура проекта

```
app/
  main.py          # точка входа, выбор режима
  bot.py           # инициализация бота и регистрация хендлеров
  config.py        # конфигурация из ENV
  handlers/        # команды и сценарии
  services/        # интеграции (Google Sheets, купоны, статистика, телефон)
  storage/         # локальная база для идемпотентности
  keyboards/       # общие клавиатуры
  utils/           # вспомогательные утилиты
```

## Проверка сценариев

1. `MODE=polling`: `/start test` → проверка подписки → выдача купона и запись в таблицы.
2. Повторная команда `get_gift` вернёт тот же код (идемпотентность). Отправка контакта создаёт запись в `leads` и уведомление администратору.
3. `MODE=webhook`: при корректном URL и секрете бот устанавливает webhook, обработка `/start` и других сценариев происходит через FastAPI.

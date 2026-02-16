import requests
import time
from datetime import datetime
import threading
import logging
from typing import Optional, Dict, Any

# Настройки
TELEGRAM_BOT_TOKEN = '8526007602:AAF2p-ngC0amxeo1UvPOOy8RqHVxW0dYGAg'  # Замените на свой токен
TELEGRAM_CHAT_ID = '5296533274'  # ID чата/пользователя

# Параметры корреляции
MIN_PRICE_DIFF_PERCENT = 0.3  # Минимальная разница в цене для сигнала (0.3%)
MIN_VOLUME_USD = 100000  # Минимальный объем на Binance (в USD)
CHECK_INTERVAL = 30  # ИНТЕРВАЛ ТЕПЕРЬ 30 СЕКУНД
VOLUME_COMPARISON = 1.5  # Объем на Binance должен быть в 1.5 раза больше чем на MEXC

# НАСТРОЙКА КОЛИЧЕСТВА ПРОВЕРОК
SYMBOLS_PER_CYCLE = 2000  # Сколько монет проверять за один цикл

# Кэш для статусов монет MEXC (чтобы не ддосить)
coin_status_cache = {}
CACHE_TTL = 300  # 5 минут

# Логирование - ОСТАВЛЯЕМ INFO, DEBUG включать не обязательно
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Хранилище отправленных сигналов (чтобы не спамить одним и тем же)
sent_signals = {}


def send_telegram_message(text: str) -> bool:
    """Отправка сообщения в Telegram"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': text,
        'parse_mode': 'HTML',
        'disable_web_page_preview': False
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        logging.error(f"Ошибка отправки Telegram: {e}")
        return False


def get_all_binance_usdt_symbols() -> list:
    """Получение всех USDT пар с Binance через публичный тикер"""
    url = "https://api.binance.com/api/v3/ticker/24hr"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        symbols = []
        for item in data:
            symbol = item.get('symbol', '')
            if symbol.endswith('USDT'):
                # Проверяем, что есть объем и цена
                if float(item.get('quoteVolume', 0)) > 0 and float(item.get('lastPrice', 0)) > 0:
                    symbols.append(symbol)

        logging.info(f"✅ Загружено {len(symbols)} USDT пар с Binance")
        return symbols
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки символов Binance: {e}")
        return []


def get_all_mexc_usdt_symbols() -> list:
    """Получение всех USDT пар с MEXC через публичный тикер"""
    url = "https://api.mexc.com/api/v3/ticker/24hr"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        symbols = []
        for item in data:
            symbol = item.get('symbol', '')
            if symbol.endswith('USDT'):
                # Проверяем, что есть объем и цена
                if float(item.get('quoteVolume', 0)) > 0 and float(item.get('lastPrice', 0)) > 0:
                    symbols.append(symbol)

        logging.info(f"✅ Загружено {len(symbols)} USDT пар с MEXC")
        return symbols
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки символов MEXC: {e}")
        return []


def get_binance_ticker(symbol: str) -> Optional[Dict[str, Any]]:
    """Получение 24-часового тикера с Binance для конкретной пары"""
    url = "https://api.binance.com/api/v3/ticker/24hr"
    params = {'symbol': symbol}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        return {
            'symbol': data['symbol'],
            'lastPrice': float(data['lastPrice']),
            'volume': float(data['quoteVolume']),  # объем в USDT за 24ч
            'priceChangePercent': float(data['priceChangePercent'])
        }
    except Exception as e:
        logging.debug(f"Ошибка получения тикера Binance для {symbol}: {e}")
        return None


def get_mexc_ticker(symbol: str) -> Optional[Dict[str, Any]]:
    """Получение 24-часового тикера с MEXC для конкретной пары"""
    url = "https://api.mexc.com/api/v3/ticker/24hr"
    params = {'symbol': symbol}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        # Проверяем, что это не массив (если запросили конкретный символ)
        if isinstance(data, list):
            if len(data) > 0:
                data = data[0]
            else:
                return None

        return {
            'symbol': data['symbol'],
            'lastPrice': float(data['lastPrice']),
            'volume': float(data['quoteVolume']),  # объем в USDT за 24ч
            'priceChangePercent': float(data['priceChangePercent'])
        }
    except Exception as e:
        logging.debug(f"Ошибка получения тикера MEXC для {symbol}: {e}")
        return None


def get_coin_status(currency: str) -> Optional[Dict[str, Any]]:
    """
    Получение статуса депозита/вывода монеты с MEXC через публичный API
    Использует кэширование
    """
    # Проверяем кэш
    now = time.time()
    if currency in coin_status_cache:
        cached_data, timestamp = coin_status_cache[currency]
        if now - timestamp < CACHE_TTL:
            return cached_data

    url = "https://www.mexc.com/api/platform/asset/currencyDetail"
    params = {'currency': currency.upper()}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get('code') != 200:
            logging.debug(f"MEXC currencyDetail {currency} ошибка: {data}")
            return None

        # Извлекаем нужные поля
        result = {
            'currency': currency.upper(),
            'depositStatus': data['data'].get('depositStatus', False),
            'withdrawStatus': data['data'].get('withdrawStatus', False),
            'name': data['data'].get('currencyFullName', currency)
        }

        # Сохраняем в кэш
        coin_status_cache[currency] = (result, now)
        return result
    except Exception as e:
        logging.debug(f"Ошибка получения статуса для {currency}: {e}")
        return None


def extract_base_currency(binance_symbol: str) -> str:
    """Извлекает базовую валюту из пары Binance"""
    if binance_symbol.endswith('USDT'):
        base = binance_symbol[:-4]
        # Обработка популярных префиксов
        if base.startswith('1000'):
            base = base[4:]
        elif base.startswith('100'):
            base = base[3:]
        return base
    return binance_symbol


def find_matching_mexc_symbol(base_currency: str, mexc_symbols: list) -> Optional[str]:
    """Поиск соответствующей пары на MEXC по базовой валюте"""
    base_upper = base_currency.upper()
    for sym in mexc_symbols:
        if sym.endswith('USDT'):
            mexc_base = sym[:-4]
            # Обработка префиксов
            if mexc_base.startswith('1000'):
                mexc_base = mexc_base[4:]
            elif mexc_base.startswith('100'):
                mexc_base = mexc_base[3:]
            
            if mexc_base == base_upper:
                return sym
    return None


def check_correlation_opportunity(binance_symbol: str, mexc_symbols: list) -> Optional[Dict[str, Any]]:
    """
    Проверяет возможность корреляции для пары binance_symbol с MEXC.
    Возвращает словарь с данными, если условия выполнены.
    """
    # 1. Получаем данные с Binance
    binance_ticker = get_binance_ticker(binance_symbol)
    if not binance_ticker:
        return None

    binance_price = binance_ticker['lastPrice']
    binance_volume = binance_ticker['volume']

    # Проверяем минимальный объем на Binance
    if binance_volume < MIN_VOLUME_USD:
        return None

    # 2. Извлекаем базовую валюту и ищем на MEXC
    base_currency = extract_base_currency(binance_symbol)
    mexc_symbol = find_matching_mexc_symbol(base_currency, mexc_symbols)
    
    if not mexc_symbol:
        return None

    # 3. Получаем данные с MEXC
    mexc_ticker = get_mexc_ticker(mexc_symbol)
    if not mexc_ticker:
        return None

    mexc_price = mexc_ticker['lastPrice']
    mexc_volume = mexc_ticker['volume']

    # 4. Проверяем что объем на Binance больше чем на MEXC
    if binance_volume <= mexc_volume * VOLUME_COMPARISON:
        return None

    # 5. Проверяем статус депозита/вывода на MEXC
    currency_status = get_coin_status(base_currency)
    if not currency_status:
        return None

    deposit_enabled = currency_status.get('depositStatus', False)
    withdraw_enabled = currency_status.get('withdrawStatus', False)

    if not deposit_enabled or not withdraw_enabled:
        return None

    # 6. Рассчитываем разницу в цене (в %)
    price_diff = (binance_price - mexc_price) / mexc_price * 100
    abs_price_diff = abs(price_diff)

    if abs_price_diff < MIN_PRICE_DIFF_PERCENT:
        return None

    # 7. Определяем направление на основе отношения цен
    if binance_price > mexc_price:
        direction = "📈 MEXC < BINANCE (MEXC может догнать)"
        action = "Покупка на MEXC, продажа на Binance"
        signal_type = "🟢 LONG MEXC"
    else:
        direction = "📉 MEXC > BINANCE (MEXC может упасть)"
        action = "Продажа на MEXC, покупка на Binance"
        signal_type = "🔴 SHORT MEXC"

    # Формируем ссылки
    binance_trade_url = f"https://www.binance.com/en/trade/{binance_symbol}?type=spot"
    mexc_trade_url = f"https://www.mexc.com/exchange/{mexc_symbol}"

    # Уникальный ключ для предотвращения повторов (на 24 часа)
    today = datetime.now().strftime('%Y-%m-%d')
    signal_key = f"{base_currency}_{today}_{abs_price_diff:.1f}"
    if signal_key in sent_signals:
        if time.time() - sent_signals[signal_key] < 86400:  # 24 часа
            return None

    result = {
        'symbol': base_currency,
        'binance_symbol': binance_symbol,
        'mexc_symbol': mexc_symbol,
        'binance_price': binance_price,
        'mexc_price': mexc_price,
        'price_diff': price_diff,
        'abs_price_diff': abs_price_diff,
        'direction': direction,
        'action': action,
        'signal_type': signal_type,
        'binance_volume': binance_volume,
        'mexc_volume': mexc_volume,
        'binance_url': binance_trade_url,
        'mexc_url': mexc_trade_url,
        'deposit': deposit_enabled,
        'withdraw': withdraw_enabled,
        'signal_key': signal_key,
        'volume_ratio': binance_volume / mexc_volume if mexc_volume > 0 else 0
    }
    return result


def format_correlation_message(data: Dict[str, Any]) -> str:
    """Форматирует сообщение для Telegram (HTML, моноширинный)"""
    # Рассчитываем цену для копирования (округляем до разумного количества знаков)
    if data['binance_price'] < 0.0001:
        price_precision = 8
    elif data['binance_price'] < 0.01:
        price_precision = 6
    else:
        price_precision = 4

    message = f"""
<code>{data['signal_type']} {data['symbol']} | Разница {data['abs_price_diff']:.2f}%</code>

<b>{data['direction']}</b>
<b>{data['action']}</b>

💰 <b>Цены (копируй):</b>
<code>BINANCE: {data['binance_price']:.{price_precision}f}</code>
<code>MEXC:    {data['mexc_price']:.{price_precision}f}</code>
<code>Разрыв:  {data['price_diff']:+.2f}%</code>

📊 <b>Объемы 24ч:</b>
<code>BINANCE: ${data['binance_volume']:,.0f}</code>
<code>MEXC:    ${data['mexc_volume']:,.0f}</code>
<code>Ratio:   {data['volume_ratio']:.2f}x</code>

🔗 <b>Ссылки:</b>
• <a href='{data['binance_url']}'>Binance {data['binance_symbol']}</a>
• <a href='{data['mexc_url']}'>MEXC {data['mexc_symbol']}</a>

💳 <b>MEXC:</b> <code>Депозит {'✅' if data['deposit'] else '❌'} | Вывод {'✅' if data['withdraw'] else '❌'}</code>
"""
    return message


def monitor():
    """Основной цикл мониторинга"""
    logging.info("🚀 Запуск мониторинга корреляции Binance / MEXC")
    logging.info(
        f"⚙️ Параметры: разница цен {MIN_PRICE_DIFF_PERCENT}%, интервал {CHECK_INTERVAL}с, монет за цикл {SYMBOLS_PER_CYCLE}")

    # Отправляем сообщение о запуске
    send_telegram_message(
        f"🟢 <b>Бот корреляции запущен</b>\nМониторинг Binance vs MEXC активен.\n"
        f"Проверяю {SYMBOLS_PER_CYCLE} монет каждые {CHECK_INTERVAL}с\n"
        f"Минимальная разница цен: {MIN_PRICE_DIFF_PERCENT}%")

    last_binance_load = 0
    last_mexc_load = 0
    binance_symbols = []
    mexc_symbols = []
    total_checks = 0
    opportunities_found = 0
    cycle_count = 0

    while True:
        try:
            cycle_count += 1
            now = time.time()

            # Обновляем список символов Binance раз в час
            if now - last_binance_load > 3600 or not binance_symbols:
                binance_symbols = get_all_binance_usdt_symbols()
                if not binance_symbols:
                    logging.error("❌ Не удалось загрузить символы Binance, жду 30 сек...")
                    time.sleep(30)
                    continue
                last_binance_load = now
                logging.info(f"📋 Всего монет на Binance: {len(binance_symbols)}")

            # Обновляем список символов MEXC раз в час
            if now - last_mexc_load > 3600 or not mexc_symbols:
                mexc_symbols = get_all_mexc_usdt_symbols()
                if not mexc_symbols:
                    logging.error("❌ Не удалось загрузить символы MEXC, жду 30 сек...")
                    time.sleep(30)
                    continue
                last_mexc_load = now
                logging.info(f"📋 Всего монет на MEXC: {len(mexc_symbols)}")

            # Проверяем SYMBOLS_PER_CYCLE символов за цикл
            symbols_to_check = binance_symbols[:SYMBOLS_PER_CYCLE]

            logging.info(f"🔄 Цикл #{cycle_count}: проверяю {len(symbols_to_check)} монет...")

            for i, sym in enumerate(symbols_to_check, 1):
                try:
                    total_checks += 1
                    opportunity = check_correlation_opportunity(sym, mexc_symbols)
                    if opportunity:
                        opportunities_found += 1
                        msg = format_correlation_message(opportunity)
                        if send_telegram_message(msg):
                            sent_signals[opportunity['signal_key']] = time.time()
                            logging.info(f"✅ СИГНАЛ #{opportunities_found}: {opportunity['symbol']} "
                                         f"разница {opportunity['abs_price_diff']:.2f}% ({i}/{len(symbols_to_check)})")
                        time.sleep(2)  # Пауза между отправками

                    # Прогресс каждые 50 монет
                    if i % 50 == 0:
                        logging.info(f"⏳ Прогресс: {i}/{len(symbols_to_check)} монет проверено")

                except Exception as e:
                    logging.error(f"❌ Ошибка при проверке {sym}: {e}")
                    continue

            # Ротация символов: перемещаем проверенные в конец
            if binance_symbols:
                binance_symbols = binance_symbols[SYMBOLS_PER_CYCLE:] + binance_symbols[:SYMBOLS_PER_CYCLE]

            # Статистика за цикл
            logging.info(f"📊 Цикл #{cycle_count} завершен. "
                         f"Проверено: {len(symbols_to_check)} монет, "
                         f"Всего проверок: {total_checks}, "
                         f"Найдено сигналов: {opportunities_found}")
            logging.info(f"💤 Сплю {CHECK_INTERVAL} сек...")
            time.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            logging.info("🛑 Бот остановлен пользователем")
            send_telegram_message("🔴 <b>Бот корреляции остановлен</b>")
            break
        except Exception as e:
            logging.error(f"💥 Критическая ошибка: {e}")
            time.sleep(30)


if __name__ == "__main__":
    monitor()



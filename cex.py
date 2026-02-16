import requests
import time
from datetime import datetime
import threading
import logging
from typing import Optional, Dict, Any

# Настройки
TELEGRAM_BOT_TOKEN = '8526007602:AAF2p-ngC0amxeo1UvPOOy8RqHVxW0dYGAg'  # Замените на свой токен
TELEGRAM_CHAT_ID = '5296533274'  # ID чата/пользователя

# Параметры арбитража
MIN_SPREAD_PERCENT = 0.5  # Минимальный спред для сигнала
MIN_VOLUME_USD = 30000  # Минимальный объем на DEX (в USD) для фильтрации
CHECK_INTERVAL = 30  # ИНТЕРВАЛ ТЕПЕРЬ 30 СЕКУНД
MEXC_VOLUME_MULTIPLIER = 0.3  # DEX объем должен быть > MEXC объем * множитель

# НАСТРОЙКА КОЛИЧЕСТВА ПРОВЕРОК
SYMBOLS_PER_CYCLE = 2000  # Сколько монет проверять за один цикл (было 50, стало 200)

# Список сетей для DexScreener (приоритетные)
PREFERRED_CHAINS = ['ethereum', 'bsc', 'polygon', 'arbitrum', 'optimism', 'avalanche', 'base', 'fantom']

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


def get_dexscreener_pair(query: str) -> Optional[Dict[str, Any]]:
    """
    Поиск пары на DexScreener по запросу (символ).
    Возвращает лучшую пару по ликвидности среди предпочтительных сетей.
    """
    url = "https://api.dexscreener.com/latest/dex/search"
    params = {'q': query}

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if not data.get('pairs'):
            return None

        # Фильтруем пары по приоритетным сетям
        valid_pairs = []
        for p in data['pairs']:
            chain = p.get('chainId')
            if chain in PREFERRED_CHAINS:
                try:
                    liquidity = float(p.get('liquidity', {}).get('usd', 0))

                    if liquidity >= MIN_VOLUME_USD:
                        valid_pairs.append({
                            'chain': chain,
                            'dex': p.get('dexId', 'unknown'),
                            'priceUsd': float(p.get('priceUsd', 0)),
                            'liquidityUsd': liquidity,
                            'volume24h': float(p.get('volume', {}).get('h24', 0)),
                            'url': p.get('url', ''),
                            'pairAddress': p.get('pairAddress'),
                            'baseToken': p.get('baseToken', {}).get('symbol'),
                            'baseAddress': p.get('baseToken', {}).get('address'),
                            'quoteToken': p.get('quoteToken', {}).get('symbol')
                        })
                except (ValueError, TypeError) as e:
                    continue

        if not valid_pairs:
            return None

        # Сортируем по ликвидности (убывание) и берём лучшую
        best_pair = max(valid_pairs, key=lambda x: x['liquidityUsd'])

        return best_pair
    except Exception as e:
        logging.debug(f"Ошибка DexScreener для {query}: {e}")
        return None


def extract_base_currency(mexc_symbol: str) -> str:
    """Извлекает базовую валюту из пары MEXC"""
    if mexc_symbol.endswith('USDT'):
        base = mexc_symbol[:-4]
        # Обработка популярных префиксов
        if base.startswith('1000'):
            base = base[4:]
        return base
    return mexc_symbol


def check_arbitrage_opportunity(mexc_symbol: str) -> Optional[Dict[str, Any]]:
    """
    Проверяет возможность арбитража для пары mexc_symbol.
    Возвращает словарь с данными, если условия выполнены.
    """
    # 1. Получаем данные с MEXC
    mexc_ticker = get_mexc_ticker(mexc_symbol)
    if not mexc_ticker:
        return None

    mexc_price = mexc_ticker['lastPrice']
    mexc_volume = mexc_ticker['volume']

    # 2. Проверяем статус депозита/вывода
    base_currency = extract_base_currency(mexc_symbol)
    currency_status = get_coin_status(base_currency)
    if not currency_status:
        return None

    deposit_enabled = currency_status.get('depositStatus', False)
    withdraw_enabled = currency_status.get('withdrawStatus', False)

    if not deposit_enabled or not withdraw_enabled:
        return None

    # 3. Ищем пару на DexScreener
    dex_pair = get_dexscreener_pair(base_currency)
    if not dex_pair:
        return None

    dex_price = dex_pair['priceUsd']
    dex_volume = dex_pair['volume24h']
    dex_liquidity = dex_pair['liquidityUsd']
    dex_url = dex_pair['url']

    # 4. Сравниваем объемы (DEX объем должен быть > MEXC объем * множитель)
    if dex_volume <= mexc_volume * MEXC_VOLUME_MULTIPLIER:
        return None

    # 5. Рассчитываем спред (в %)
    spread = (mexc_price - dex_price) / dex_price * 100
    abs_spread = abs(spread)

    if abs_spread < MIN_SPREAD_PERCENT:
        return None

    # 6. Определяем направление на основе отношения цен
    if dex_price > mexc_price:
        direction = "LONG (MEXC догонит DEX вверх)"
        action = "Покупка на MEXC"
        signal_type = "🟢 LONG"
    else:
        direction = "SHORT (MEXC упадет до DEX)"
        action = "Продажа на MEXC"
        signal_type = "🔴 SHORT"

    # Формируем ссылку на MEXC
    mexc_trade_url = f"https://www.mexc.com/exchange/{mexc_symbol}"

    # Уникальный ключ для предотвращения повторов (на 24 часа)
    today = datetime.now().strftime('%Y-%m-%d')
    signal_key = f"{base_currency}_{today}_{abs_spread:.1f}"
    if signal_key in sent_signals:
        # Проверяем, не устарел ли ключ
        if time.time() - sent_signals[signal_key] < 86400:  # 24 часа
            return None

    result = {
        'symbol': base_currency,
        'mexc_symbol': mexc_symbol,
        'dex_price': dex_price,
        'mexc_price': mexc_price,
        'spread': spread,
        'abs_spread': abs_spread,
        'direction': direction,
        'action': action,
        'signal_type': signal_type,
        'dex_volume': dex_volume,
        'mexc_volume': mexc_volume,
        'dex_liquidity': dex_liquidity,
        'dex_url': dex_url,
        'mexc_url': mexc_trade_url,
        'deposit': deposit_enabled,
        'withdraw': withdraw_enabled,
        'signal_key': signal_key,
        'chain': dex_pair['chain'],
        'dex_name': dex_pair['dex']
    }
    return result


def format_arbitrage_message(data: Dict[str, Any]) -> str:
    """Форматирует сообщение для Telegram (HTML, моноширинный)"""
    # Рассчитываем цену для копирования (округляем до разумного количества знаков)
    if data['dex_price'] < 0.0001:
        price_precision = 8
    elif data['dex_price'] < 0.01:
        price_precision = 6
    else:
        price_precision = 4

    message = f"""
<code>{data['signal_type']} {data['symbol']} | Спред {data['abs_spread']:.2f}%</code>

<b>{data['direction']}</b>
<b>{data['action']}</b>

💰 <b>Цены (копируй):</b>
<code>DEX:    {data['dex_price']:.{price_precision}f}</code>
<code>MEXC:   {data['mexc_price']:.{price_precision}f}</code>
<code>Разрыв: {data['spread']:+.2f}%</code>

📊 <b>Объемы 24ч:</b>
<code>DEX:    ${data['dex_volume']:,.0f}</code>
<code>MEXC:   ${data['mexc_volume']:,.0f}</code>
<code>Liq:    ${data['dex_liquidity']:,.0f}</code>

🔗 <b>Ссылки:</b>
• <a href='{data['dex_url']}'>DexScreener ({data['chain']}/{data['dex_name']})</a>
• <a href='{data['mexc_url']}'>MEXC {data['mexc_symbol']}</a>

💳 <b>MEXC:</b> <code>Депозит {'✅' if data['deposit'] else '❌'} | Вывод {'✅' if data['withdraw'] else '❌'}</code>
"""
    return message


def monitor():
    """Основной цикл мониторинга"""
    logging.info("🚀 Запуск мониторинга арбитража DexScreener / MEXC")
    logging.info(
        f"⚙️ Параметры: спред {MIN_SPREAD_PERCENT}%, интервал {CHECK_INTERVAL}с, монет за цикл {SYMBOLS_PER_CYCLE}")

    # Отправляем сообщение о запуске
    send_telegram_message(
        f"🟢 <b>Бот арбитража запущен</b>\nМониторинг DEX vs MEXC активен.\nПроверяю {SYMBOLS_PER_CYCLE} монет каждые {CHECK_INTERVAL}с")

    last_symbols_load = 0
    symbols = []
    total_checks = 0
    opportunities_found = 0
    cycle_count = 0

    while True:
        try:
            cycle_count += 1

            # Обновляем список символов раз в час
            now = time.time()
            if now - last_symbols_load > 3600 or not symbols:
                symbols = get_all_mexc_usdt_symbols()
                if not symbols:
                    logging.error("❌ Не удалось загрузить символы, жду 30 сек...")
                    time.sleep(30)
                    continue
                last_symbols_load = now
                total_checks = 0
                opportunities_found = 0
                logging.info(f"📋 Всего монет в базе: {len(symbols)}")

            # Проверяем SYMBOLS_PER_CYCLE символов за цикл
            symbols_to_check = symbols[:SYMBOLS_PER_CYCLE]

            logging.info(f"🔄 Цикл #{cycle_count}: проверяю {len(symbols_to_check)} монет...")

            for i, sym in enumerate(symbols_to_check, 1):
                try:
                    total_checks += 1
                    opportunity = check_arbitrage_opportunity(sym)
                    if opportunity:
                        opportunities_found += 1
                        msg = format_arbitrage_message(opportunity)
                        if send_telegram_message(msg):
                            sent_signals[opportunity['signal_key']] = time.time()
                            logging.info(f"✅ СИГНАЛ #{opportunities_found}: {opportunity['symbol']} "
                                         f"спред {opportunity['abs_spread']:.2f}% ({i}/{len(symbols_to_check)})")
                        time.sleep(2)  # Пауза между отправками

                    # Прогресс каждые 50 монет
                    if i % 50 == 0:
                        logging.info(f"⏳ Прогресс: {i}/{len(symbols_to_check)} монет проверено")

                except Exception as e:
                    logging.error(f"❌ Ошибка при проверке {sym}: {e}")
                    continue

            # Ротация символов: перемещаем проверенные в конец
            if symbols:
                symbols = symbols[SYMBOLS_PER_CYCLE:] + symbols[:SYMBOLS_PER_CYCLE]

            # Статистика за цикл
            logging.info(f"📊 Цикл #{cycle_count} завершен. "
                         f"Проверено: {len(symbols_to_check)} монет, "
                         f"Всего проверок: {total_checks}, "
                         f"Найдено сигналов: {opportunities_found}")
            logging.info(f"💤 Сплю {CHECK_INTERVAL} сек...")
            time.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            logging.info("🛑 Бот остановлен пользователем")
            send_telegram_message("🔴 <b>Бот арбитража остановлен</b>")
            break
        except Exception as e:
            logging.error(f"💥 Критическая ошибка: {e}")
            time.sleep(30)


if __name__ == "__main__":
    monitor()



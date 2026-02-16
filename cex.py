import requests
import time
from datetime import datetime
import logging
from typing import Optional, Dict, Any

# Настройки Telegram
TELEGRAM_BOT_TOKEN = '8526007602:AAF2p-ngC0amxeo1UvPOOy8RqHVxW0dYGAg'
TELEGRAM_CHAT_ID = '5296533274'

# Параметры сигнала
MIN_SPREAD_PERCENT = 1.5           # Минимальный спред для сигнала (%)
CHECK_INTERVAL = 30                 # Пауза между циклами (сек)
SYMBOLS_PER_CYCLE = 2000             # Сколько монет проверять за один цикл

# Сети для DexScreener (приоритетные)
PREFERRED_CHAINS = ['ethereum', 'bsc', 'polygon', 'arbitrum', 'optimism', 'avalanche', 'base', 'fantom']

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Кэш отправленных сигналов (чтобы не дублировать)
sent_signals = {}


def send_telegram_message(text: str) -> bool:
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
    url = "https://api.mexc.com/api/v3/ticker/24hr"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        symbols = []
        for item in data:
            symbol = item.get('symbol', '')
            if symbol.endswith('USDT'):
                if float(item.get('quoteVolume', 0)) > 0 and float(item.get('lastPrice', 0)) > 0:
                    symbols.append(symbol)

        logging.info(f"✅ Загружено {len(symbols)} USDT пар с MEXC")
        return symbols
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки символов MEXC: {e}")
        return []


def get_mexc_ticker(symbol: str) -> Optional[Dict[str, Any]]:
    url = "https://api.mexc.com/api/v3/ticker/24hr"
    params = {'symbol': symbol}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            data = data[0] if data else None
        if not data:
            return None

        return {
            'symbol': data['symbol'],
            'lastPrice': float(data['lastPrice']),
            'volume': float(data['quoteVolume']),
            'priceChangePercent': float(data['priceChangePercent'])
        }
    except Exception as e:
        logging.debug(f"Ошибка получения тикера MEXC для {symbol}: {e}")
        return None


def get_dexscreener_pair(query: str, expected_symbol: str) -> Optional[Dict[str, Any]]:
    """
    Поиск пары на DexScreener по запросу (query).
    Возвращает лучшую пару по ликвидности среди предпочтительных сетей,
    но только если символ базового токена (baseToken['symbol']) совпадает с expected_symbol
    (после приведения к верхнему регистру).
    """
    url = "https://api.dexscreener.com/latest/dex/search"
    params = {'q': query}

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if not data.get('pairs'):
            return None

        valid_pairs = []
        expected_upper = expected_symbol.upper()

        for p in data['pairs']:
            chain = p.get('chainId')
            if chain in PREFERRED_CHAINS:
                try:
                    base_token_symbol = p.get('baseToken', {}).get('symbol', '')
                    # Проверяем, что символ базового токена совпадает с ожидаемым (игнорируем регистр)
                    if base_token_symbol.upper() != expected_upper:
                        continue

                    liquidity = float(p.get('liquidity', {}).get('usd', 0))
                    if liquidity > 0:
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
                except (ValueError, TypeError):
                    continue

        if not valid_pairs:
            return None

        best_pair = max(valid_pairs, key=lambda x: x['liquidityUsd'])
        return best_pair
    except Exception as e:
        logging.debug(f"Ошибка DexScreener для {query}: {e}")
        return None


def extract_base_currency(mexc_symbol: str) -> str:
    if mexc_symbol.endswith('USDT'):
        base = mexc_symbol[:-4]
        # Убираем популярные префиксы, чтобы получить "чистое" название монеты
        if base.startswith('1000'):
            base = base[4:]
        return base
    return mexc_symbol


def check_arbitrage_opportunity(mexc_symbol: str) -> Optional[Dict[str, Any]]:
    # 1. Данные с MEXC
    mexc_ticker = get_mexc_ticker(mexc_symbol)
    if not mexc_ticker:
        return None
    mexc_price = mexc_ticker['lastPrice']
    mexc_volume = mexc_ticker['volume']

    # 2. Базовая валюта
    base_currency = extract_base_currency(mexc_symbol)

    # 3. Поиск на DexScreener с проверкой совпадения символа
    dex_pair = get_dexscreener_pair(base_currency, base_currency)
    if not dex_pair:
        return None

    dex_price = dex_pair['priceUsd']
    dex_volume = dex_pair['volume24h']

    # 4. Условие по объёму: DEX объём должен быть строго больше MEXC объёма
    if dex_volume <= mexc_volume:
        return None

    # 5. Расчёт спреда
    spread = (mexc_price - dex_price) / dex_price * 100
    abs_spread = abs(spread)
    if abs_spread < MIN_SPREAD_PERCENT:
        return None

    # 6. Направление
    if dex_price > mexc_price:
        direction = "LONG (MEXC догонит DEX вверх)"
        action = "Покупка на MEXC"
        signal_type = "🟢 LONG"
    else:
        direction = "SHORT (MEXC упадёт до DEX)"
        action = "Продажа на MEXC"
        signal_type = "🔴 SHORT"

    # 7. Предотвращение повторов
    today = datetime.now().strftime('%Y-%m-%d')
    signal_key = f"{base_currency}_{today}_{abs_spread:.1f}"
    if signal_key in sent_signals and time.time() - sent_signals[signal_key] < 86400:
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
        'dex_liquidity': dex_pair['liquidityUsd'],
        'dex_url': dex_pair['url'],
        'mexc_url': f"https://www.mexc.com/exchange/{mexc_symbol}",
        'signal_key': signal_key,
        'chain': dex_pair['chain'],
        'dex_name': dex_pair['dex']
    }
    return result


def format_arbitrage_message(data: Dict[str, Any]) -> str:
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

📊 <b>Объёмы 24ч:</b>
<code>DEX:    ${data['dex_volume']:,.0f}</code>
<code>MEXC:   ${data['mexc_volume']:,.0f}</code>
<code>Liq:    ${data['dex_liquidity']:,.0f}</code>

🔗 <b>Ссылки:</b>
• <a href='{data['dex_url']}'>DexScreener ({data['chain']}/{data['dex_name']})</a>
• <a href='{data['mexc_url']}'>MEXC {data['mexc_symbol']}</a>
"""
    return message


def monitor():
    logging.info("🚀 Запуск мониторинга ценовых расхождений DEX / MEXC")
    logging.info(f"⚙️ Параметры: спред от {MIN_SPREAD_PERCENT}%, условие: объём DEX > объём MEXC")

    send_telegram_message(
        f"🟢 <b>Бот перезапущен</b>\n"
        f"Ищем расхождения цены ≥ {MIN_SPREAD_PERCENT}%\n"
        f"Условие: объём DEX > объём MEXC\n"
        f"Проверяю {SYMBOLS_PER_CYCLE} монет каждые {CHECK_INTERVAL}с"
    )

    last_symbols_load = 0
    symbols = []
    total_checks = 0
    opportunities_found = 0
    cycle_count = 0

    while True:
        try:
            cycle_count += 1
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
                                         f"спред {opportunity['abs_spread']:.2f}%")
                        time.sleep(2)

                    if i % 50 == 0:
                        logging.info(f"⏳ Прогресс: {i}/{len(symbols_to_check)}")
                except Exception as e:
                    logging.error(f"Ошибка при проверке {sym}: {e}")
                    continue

            symbols = symbols[SYMBOLS_PER_CYCLE:] + symbols[:SYMBOLS_PER_CYCLE]

            logging.info(f"📊 Цикл #{cycle_count} завершён. Проверено: {len(symbols_to_check)}, "
                         f"всего проверок: {total_checks}, сигналов: {opportunities_found}")
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


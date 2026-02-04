"""
📈 Stock/Crypto Recommendation Discord Bot
한국 주식 및 코인 추천 디스코드 봇
"""

import discord
from discord.ext import commands, tasks
import os
from dotenv import load_dotenv
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import ta
import asyncio
import schedule
import threading

# Load environment variables
load_dotenv()
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
CHANNEL_ID_STR = os.getenv('CHANNEL_ID')

if not DISCORD_TOKEN:
    print("❌ 오류: DISCORD_TOKEN이 설정되지 않았습니다")
    print("   .env 파일을 확인하세요")
    exit(1)

if not CHANNEL_ID_STR:
    print("❌ 오류: CHANNEL_ID가 설정되지 않았습니다")
    print("   .env 파일을 확인하세요")
    exit(1)

try:
    CHANNEL_ID = int(CHANNEL_ID_STR)
except ValueError:
    print(f"❌ 오류: CHANNEL_ID는 숫자여야 합니다 (현재: {CHANNEL_ID_STR})")
    exit(1)

# ==================== Configuration ====================
SCAN_HOUR = 8               # Scan time (hour)
SCAN_MINUTE = 30            # Scan time (minute)
STOCK_TOP_N = 10            # Number of stock recommendations
COIN_TOP_N = 10             # Number of crypto recommendations
VOLUME_SURGE_RATIO = 1.5    # Volume surge threshold
RSI_OVERSOLD = 35           # RSI oversold threshold
RSI_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
EMA_SHORT = 5
EMA_LONG = 20
BB_PERIOD = 20
MFI_PERIOD = 14

# ==================== Discord Bot Setup ====================
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

# ==================== Technical Indicators ====================
def calculate_rsi(data, period=RSI_PERIOD):
    """Calculate RSI (Relative Strength Index)"""
    return ta.momentum.rsi(data, length=period)

def calculate_macd(data):
    """Calculate MACD"""
    macd = ta.trend.macd(data, window_fast=MACD_FAST, window_slow=MACD_SLOW, window_sign=MACD_SIGNAL)
    return macd

def calculate_ema(data, short=EMA_SHORT, long=EMA_LONG):
    """Calculate EMA (Exponential Moving Average)"""
    ema_short = ta.trend.ema_indicator(data, window=short)
    ema_long = ta.trend.ema_indicator(data, window=long)
    return ema_short, ema_long

def calculate_bollinger_bands(data, period=BB_PERIOD, std_dev=2):
    """Calculate Bollinger Bands"""
    bb = ta.volatility.bollinger_wband(data, window=period, window_dev=std_dev)
    return bb

def calculate_mfi(high, low, close, volume, period=MFI_PERIOD):
    """Calculate MFI (Money Flow Index)"""
    return ta.volume.money_flow_index(high, low, close, volume, window=period)

def analyze_stock(code, name, days=60):
    """
    Analyze a stock using technical indicators

    Returns:
        dict: Analysis results with signals
    """
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        df = fdr.DataReader(code, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

        if df is None or len(df) < 30:
            return None

        # Calculate indicators
        rsi = calculate_rsi(df['Close'].values)
        macd_line = ta.trend.macd(df['Close'], window_fast=MACD_FAST, window_slow=MACD_SLOW, window_sign=MACD_SIGNAL)
        ema_short, ema_long = calculate_ema(df['Close'])
        bb = calculate_bollinger_bands(df['Close'])
        mfi = calculate_mfi(df['High'], df['Low'], df['Close'], df['Volume'])

        # Current values
        current_close = df['Close'].iloc[-1]
        current_volume = df['Volume'].iloc[-1]
        avg_volume = df['Volume'].iloc[-30:-1].mean()
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 0

        # Price change
        prev_close = df['Close'].iloc[-2]
        price_change = ((current_close - prev_close) / prev_close * 100)

        # Signal detection
        signals = []

        # RSI oversold breakout
        if rsi.iloc[-2] <= RSI_OVERSOLD and rsi.iloc[-1] > RSI_OVERSOLD:
            signals.append("📊 RSI 과매도 탈출")
        elif rsi.iloc[-1] < RSI_OVERSOLD:
            signals.append("📊 RSI 과매도 상태")

        # MACD golden cross
        if pd.notna(macd_line.iloc[-2]) and pd.notna(macd_line.iloc[-1]):
            if macd_line.iloc[-2] < 0 and macd_line.iloc[-1] > 0:
                signals.append("✅ MACD 골든크로스")

        # EMA golden cross
        if pd.notna(ema_short.iloc[-1]) and pd.notna(ema_long.iloc[-1]):
            if ema_short.iloc[-2] <= ema_long.iloc[-2] and ema_short.iloc[-1] > ema_long.iloc[-1]:
                signals.append("📈 EMA 골든크로스")
            elif ema_short.iloc[-1] > ema_long.iloc[-1]:
                signals.append("📈 EMA 상승 추세")

        # Bollinger Bands bounce
        if pd.notna(bb.iloc[-1]):
            if bb.iloc[-2] < 0.2 and bb.iloc[-1] >= 0.2:
                signals.append("🎈 볼린저밴드 하단 반등")

        # Volume surge
        if volume_ratio >= VOLUME_SURGE_RATIO:
            signals.append(f"🔥 거래량 급등 ({volume_ratio:.1f}배)")

        # MFI signal
        if pd.notna(mfi.iloc[-1]):
            if mfi.iloc[-1] < 30:
                signals.append("💰 MFI 과매도 신호")
            elif mfi.iloc[-1] > 70:
                signals.append("💰 MFI 과매수 신호")

        return {
            'code': code,
            'name': name,
            'current_price': round(current_close, 2),
            'price_change': round(price_change, 2),
            'rsi': round(rsi.iloc[-1], 2) if pd.notna(rsi.iloc[-1]) else None,
            'volume_ratio': round(volume_ratio, 2),
            'signals': signals,
            'signal_count': len(signals)
        }

    except Exception as e:
        print(f"❌ Error analyzing {code}: {str(e)}")
        return None

# ==================== Stock Scanning ====================
def scan_korean_stocks(limit=STOCK_TOP_N):
    """Scan Korean stocks for recommendations"""
    print("📊 스캔 시작: 한국 주식")

    try:
        # Get all stocks
        krx = fdr.StockListing('KRX')

        # Filter by market cap (top 500)
        if 'Marcap' in krx.columns:
            krx = krx.nlargest(500, 'Marcap')

        print(f"✅ 분석할 종목 수: {len(krx)}")

        results = []
        for idx, (_, row) in enumerate(krx.iterrows()):
            if idx % 100 == 0:
                print(f"  진행: {idx}/{len(krx)}")

            code = row.get('Code')
            name = row.get('Name')

            if not code:
                continue

            analysis = analyze_stock(code, name)

            if analysis and analysis['signal_count'] > 0:
                results.append(analysis)

        # Sort by signal count
        results.sort(key=lambda x: x['signal_count'], reverse=True)

        return results[:limit]

    except Exception as e:
        print(f"❌ Error scanning stocks: {str(e)}")
        return []

# ==================== Crypto Scanning ====================
def scan_cryptocurrencies(limit=COIN_TOP_N):
    """Scan cryptocurrencies using Binance data"""
    print("📊 스캔 시작: 암호화폐")

    try:
        # Get top cryptos by volume
        symbols = [
            'BTC', 'ETH', 'BNB', 'XRP', 'ADA', 'SOL', 'DOGE', 'AVAX',
            'LINK', 'MATIC', 'ATOM', 'LTC', 'DASH', 'SHIB', 'UNI', 'ARB',
            'APT', 'OP', 'FET', 'JTO'
        ]

        results = []

        for symbol in symbols[:limit]:
            try:
                # Try to get crypto data
                code = f"{symbol}KRW"

                end_date = datetime.now()
                start_date = end_date - timedelta(days=60)

                # Using a simplified approach with available data
                # In production, use Binance API
                df = fdr.DataReader(code, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

                if df is None or len(df) < 30:
                    continue

                analysis = analyze_stock(code, symbol)
                if analysis and analysis['signal_count'] > 0:
                    results.append(analysis)

            except:
                # If crypto data not available, skip
                continue

        return results[:limit]

    except Exception as e:
        print(f"❌ Error scanning cryptos: {str(e)}")
        return []

# ==================== Discord Commands ====================
@bot.event
async def on_ready():
    print(f"✅ 봇 로그인 완료: {bot.user}")
    print(f"📢 추천 채널: {CHANNEL_ID}")
    # Start background scheduling
    start_schedule_thread()

@bot.command(name='스캔')
async def scan(ctx):
    """Scan all (stocks + cryptos)"""
    await ctx.send("⏳ 전체 스캔 중입니다. 잠시만 기다려주세요...")

    stocks = scan_korean_stocks()
    cryptos = scan_cryptocurrencies()

    embed = discord.Embed(
        title="📈 주식/코인 추천",
        description=f"스캔 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        color=discord.Color.green()
    )

    if stocks:
        stock_text = "\n".join([f"• **{s['name']}** ({s['code']})\n  가격: {s['current_price']}, 변화: {s['price_change']}%\n  신호: {', '.join(s['signals'][:2])}" for s in stocks[:5]])
        embed.add_field(name="🇰🇷 한국 주식 TOP 5", value=stock_text or "추천 종목 없음", inline=False)

    if cryptos:
        crypto_text = "\n".join([f"• **{c['name']}**\n  신호: {', '.join(c['signals'][:2])}" for c in cryptos[:5]])
        embed.add_field(name="💰 암호화폐 TOP 5", value=crypto_text or "추천 암호화폐 없음", inline=False)

    await ctx.send(embed=embed)

@bot.command(name='주식')
async def scan_stocks(ctx):
    """Scan Korean stocks only"""
    await ctx.send("⏳ 한국 주식 스캔 중입니다. 잠시만 기다려주세요...")

    stocks = scan_korean_stocks()

    embed = discord.Embed(
        title="📈 한국 주식 추천",
        description=f"스캔 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        color=discord.Color.blue()
    )

    if stocks:
        for i, stock in enumerate(stocks[:10], 1):
            value = f"**가격**: {stock['current_price']}\n"
            value += f"**변화**: {stock['price_change']}%\n"
            value += f"**RSI**: {stock['rsi']}\n"
            value += f"**신호**: {', '.join(stock['signals'])}"
            embed.add_field(name=f"{i}. {stock['name']} ({stock['code']})", value=value, inline=False)
    else:
        embed.description = "추천할 종목이 없습니다."

    await ctx.send(embed=embed)

@bot.command(name='코인')
async def scan_cryptos(ctx):
    """Scan cryptocurrencies only"""
    await ctx.send("⏳ 암호화폐 스캔 중입니다. 잠시만 기다려주세요...")

    cryptos = scan_cryptocurrencies()

    embed = discord.Embed(
        title="💰 암호화폐 추천",
        description=f"스캔 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        color=discord.Color.gold()
    )

    if cryptos:
        for i, crypto in enumerate(cryptos[:10], 1):
            value = f"**신호**: {', '.join(crypto['signals'])}\n"
            value += f"**RSI**: {crypto['rsi']}"
            embed.add_field(name=f"{i}. {crypto['name']}", value=value, inline=False)
    else:
        embed.description = "추천할 암호화폐가 없습니다."

    await ctx.send(embed=embed)

@bot.command(name='도움')
async def help_command(ctx):
    """Show help message"""
    embed = discord.Embed(
        title="📚 봇 명령어 도움말",
        description="주식/코인 추천 봇 사용법",
        color=discord.Color.purple()
    )

    embed.add_field(name="!스캔", value="주식 + 코인 전체 스캔", inline=False)
    embed.add_field(name="!주식", value="한국 주식만 스캔 (상위 10개)", inline=False)
    embed.add_field(name="!코인", value="코인만 스캔 (상위 10개)", inline=False)
    embed.add_field(name="!도움", value="이 도움말 표시", inline=False)

    embed.add_field(name="📊 기술적 지표", value=
        "• RSI 과매도 탈출\n"
        "• MACD 골든크로스\n"
        "• EMA 골든크로스\n"
        "• 볼린저밴드 하단 반등\n"
        "• 거래량 급등\n"
        "• MFI 신호", inline=False)

    embed.add_field(name="⚠️ 주의사항", value=
        "이 봇의 추천은 기술적 지표 기반 필터링일 뿐,\n"
        "투자 조언이 아닙니다. 모든 투자 결정은\n"
        "개인의 책임입니다.", inline=False)

    await ctx.send(embed=embed)

# ==================== Scheduled Tasks ====================
def scheduled_scan():
    """Scheduled daily scan"""
    async def send_scan():
        try:
            channel = bot.get_channel(CHANNEL_ID)
            if not channel:
                print("❌ 채널을 찾을 수 없습니다")
                return

            print(f"⏰ {datetime.now()}: 자동 스캔 시작")

            stocks = scan_korean_stocks()
            cryptos = scan_cryptocurrencies()

            embed = discord.Embed(
                title="📈 일일 주식/코인 추천 (자동 스캔)",
                description=f"스캔 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                color=discord.Color.green()
            )

            if stocks:
                stock_text = "\n".join([f"• **{s['name']}** ({s['code']})\n  신호: {', '.join(s['signals'][:3])}" for s in stocks[:5]])
                embed.add_field(name="🇰🇷 한국 주식 TOP 5", value=stock_text, inline=False)

            if cryptos:
                crypto_text = "\n".join([f"• **{c['name']}**\n  신호: {', '.join(c['signals'][:3])}" for c in cryptos[:5]])
                embed.add_field(name="💰 암호화폐 TOP 5", value=crypto_text, inline=False)

            embed.set_footer(text="⚠️ 투자 참고용이며, 투자 조언이 아닙니다.")

            await channel.send(embed=embed)
            print("✅ 자동 스캔 완료")

        except Exception as e:
            print(f"❌ 자동 스캔 오류: {str(e)}")

    asyncio.run_coroutine_threadsafe(send_scan(), bot.loop)

def schedule_daily_scan():
    """Schedule daily scan at specified time"""
    scan_time = f"{SCAN_HOUR:02d}:{SCAN_MINUTE:02d}"
    schedule.every().day.at(scan_time).do(scheduled_scan)
    print(f"⏰ 매일 {scan_time}에 자동 스캔 설정됨")

def start_schedule_thread():
    """Start background scheduling thread"""
    schedule_daily_scan()

    def scheduler_loop():
        while True:
            schedule.run_pending()
            asyncio.sleep(60)

    thread = threading.Thread(target=scheduler_loop, daemon=True)
    thread.start()

# ==================== Main ====================
if __name__ == "__main__":
    print("=" * 50)
    print("📈 주식/코인 추천 디스코드 봇 시작")
    print("=" * 50)
    bot.run(DISCORD_TOKEN)

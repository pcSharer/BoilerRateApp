import os
import time
import asyncio
import logging
from typing import Optional, List, Tuple
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import httpx
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iot_app")

app = FastAPI(title="IoT Temperature Monitor")

# Configuration Constants
FEED_ID = "424220"
API_KEY = os.environ.get("EMONCMS_API_KEY", "FakeApi")
BASE_URL = "http://emoncms.org/feed/data.json"
MELBOURNE_TZ = ZoneInfo("Australia/Melbourne")

# Thread-safe Global Cache for Real-Time data
# Stores a list of tuples: (timestamp_ms, temperature_c)
GLOBAL_CACHE: List[Tuple[int, float]] = []
CACHE_LOCK = asyncio.Lock()


async def fetch_emoncms_data(start_ms: int, end_ms: int) -> List[Tuple[int, float]]:
    """Fetches raw data from emoncms.org for a specific window."""
    params = {
        "id": FEED_ID,
        "start": str(int(start_ms)),
        "end": str(int(end_ms)),
        "interval": "5",
        "skipmissing": "1",
        "limitinterval": "1",
        "apikey": API_KEY
    }
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(BASE_URL, params=params, timeout=10.0)
            if response.status_code == 200:
                data = response.json()
                # Ensure structure matches expected list of pairs [[ts, temp], ...]
                if isinstance(data, list):
                    return [(int(item[0]), float(item[1])) for item in data if len(item) >= 2]
            logger.error(f"Failed API call: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"Error fetching from IoT Server: {e}")
    return []


async def update_realtime_cache():
    """Background loop that updates data efficiently every 2 minutes."""
    global GLOBAL_CACHE
    while True:
        try:
            now_ms = int(time.time() * 1000)
            async with CACHE_LOCK:
                if not GLOBAL_CACHE:
                    # Initial load: Latest 3 hours
                    start_ms = now_ms - (3 * 60 * 60 * 1000)
                    logger.info("Initializing cache with past 3 hours of data...")
                    GLOBAL_CACHE = await fetch_emoncms_data(start_ms, now_ms)
                else:
                    # Incremental update: Latest 2 samples (approx 2 minutes)
                    # We check our latest timestamp to avoid duplicate fetches
                    latest_cached_ts = GLOBAL_CACHE[-1][0] if GLOBAL_CACHE else now_ms
                    # Request slightly overlapping window to ensure no missed minute gaps
                    start_ms = max(latest_cached_ts - 60000, now_ms - (5 * 60 * 1000))
                    new_samples = await fetch_emoncms_data(start_ms, now_ms)
                    
                    # Merge and deduplicate
                    existing_ts = {ts for ts, _ in GLOBAL_CACHE}
                    for ts, val in new_samples:
                        if ts not in existing_ts:
                            GLOBAL_CACHE.append((ts, val))
                    
                    # Sort cache chronologically
                    GLOBAL_CACHE.sort(key=lambda x: x[0])
                    
                    # Enforce a sliding 6-hour window max limit to keep memory stable
                    six_hours_ago = now_ms - (6 * 60 * 60 * 1000)
                    GLOBAL_CACHE = [item for item in GLOBAL_CACHE if item[0] >= six_hours_ago]
                    
            logger.info(f"Cache updated successfully. Total entries: {len(GLOBAL_CACHE)}")
        except Exception as e:
            logger.error(f"Error inside background sync loop: {e}")
        
        await asyncio.sleep(120)  # Synchronize every 2 minutes


@app.on_event("startup")
async def startup_event():
    # Spawns background task instantly upon app launch
    asyncio.create_task(update_realtime_cache())


def process_and_massage_data(samples: List[Tuple[int, float]], target_duration_hours: int) -> Tuple[str, List[str]]:
    """Massages dataset by mapping, filtering, aligning into 10-minute records, and calculating rate of rise."""
    if not samples:
        return (datetime.now(MELBOURNE_TZ).strftime("%Y-%m-%d"), ["No data available from IoT server."])
    
    # Sort data chronologically to trace accurate rate calculations
    samples = sorted(samples, key=lambda x: x[0])
    
    # 1. Map raw samples into a fast dictionary lookup table by closest minute timestamp
    # Unix timestamp divided by 60000 yields unique minute index keys
    ts_dict = {round(ts / 60000): temp for ts, temp in samples}
    
    # Determine the most recent timestamp available in the payload
    latest_ts_ms = samples[-1][0]
    latest_dt = datetime.fromtimestamp(latest_ts_ms / 1000, tz=MELBOURNE_TZ)
    top_line_date = latest_dt.strftime("%Y-%m-%d")
    
    # Align rows precisely to a fixed 10-minute stepping hierarchy counting backwards
    lines = []
    total_lines_needed = max(40, (target_duration_hours * 6))  # 6 rows per hour
    max_rate = 30
    got_steam = 110
    rate_per_bar = 6
    bar_str = ""

    for i in range(total_lines_needed):
        # Subtract steps of 10 minutes from the absolute target anchor point
        row_dt = latest_dt - timedelta(minutes=i * 10)
        row_minute_idx = round(row_dt.timestamp() / 60)
        
        # Look up current row temperature
        row_temp = ts_dict.get(row_minute_idx)
        if row_temp is None:
            # Fallback lookup to find nearest neighbor minute within a small tolerance window
            for offset in [1, -1, 2, -2]:
                if (row_minute_idx + offset) in ts_dict:
                    row_temp = ts_dict[row_minute_idx + offset]
                    break
        
        # Calculate rate of rise over the 10 minutes ending at this row
        # 10 minutes prior index
        prior_minute_idx = row_minute_idx - 10
        prior_temp = ts_dict.get(prior_minute_idx)
        if prior_temp is None:
            for offset in [1, -1, 2, -2]:
                if (prior_minute_idx + offset) in ts_dict:
                    prior_temp = ts_dict[prior_minute_idx + offset]
                    break
                    
        # Structure the printable row contents
        time_str = row_dt.strftime("%H:%M")     # PatChange - removed :%S
        
        if row_temp is not None and prior_temp is not None:
            # (Change over 10 mins) * 6 gives change per hour
            rate_of_rise = (row_temp - prior_temp) * 6
            rate_str = f"{round(rate_of_rise)}"  # PatChange - was ..._rise):+d}, which prepended "+" to positive numbers
            temp_str = f"{round(row_temp)}"
        else:
            rate_str = "N/A"
            temp_str = f"{round(row_temp)}" if row_temp is not None else "N/A"
            
        # PatChange: Was lines.append(f"{time_str:<15}{rate_str:<20}{temp_str}")
        bar_string = ""
        bar_char = "&#x2588"
        if row_temp is not None:
            if row_temp > got_steam:
                bar_char = "&#x258c"
            if rate_of_rise > max_rate:
                bar_string = bar_char * round((rate_of_rise - max_rate) / 6)
            
        lines.append(f"{time_str:>5} {temp_str:>4}{rate_str:>6} " + bar_string)

    return top_line_date, lines


@app.get("/", response_class=HTMLResponse)
async def index_endpoint(request: Request, history: Optional[str] = Query(None)):
    is_historic = False
    target_dt = None
    
    # Check if optional ISO 8601 parameter is submitted
    if history:
        try:
            # Replace spaces if raw parameters drop symbol formatting in transmission
            clean_stamp = history.strip().replace(" ", "T")
            # Parse localized timestamp and anchor context natively to Melbourne
            naive_dt = datetime.fromisoformat(clean_stamp)
            target_dt = naive_dt.replace(tzinfo=MELBOURNE_TZ)
            is_historic = True
        except ValueError:
            return HTMLResponse(content="<h3>Invalid ISO 8601 Date/Time string. Please use YYYY-MM-DDTHH:MM:SS format.</h3>", status_code=400)

    if is_historic and target_dt:
        # --- HISTORIC MODE ---
        # Fetch 5 full hours preceding specified endpoint directly from server
        end_ms = int((target_dt.timestamp() +60) * 1000)     # PatChange: Without the "+60" seconds the displayed end time is a minute behind the requested end time
        start_ms = end_ms - (5 * 60 * 60 * 1000)
        
        logger.info(f"Historic mode triggered for end target: {target_dt}")
        raw_samples = await fetch_emoncms_data(start_ms, end_ms)
        top_date, formatted_lines = process_and_massage_data(raw_samples, target_duration_hours=5)
        mode_label = f"HISTORIC REPORT (5 Hours ending at {target_dt.strftime('%Y-%m-%d %H:%M:%S')})"
        auto_refresh_script = ""  # No refresh scripting injected for historical mode
    else:
        # --- REAL TIME MODE ---
        # Deliver massaged results safely cloned straight from the global cache memory space
        async with CACHE_LOCK:
            local_samples = list(GLOBAL_CACHE)
            
        top_date, formatted_lines = process_and_massage_data(local_samples, target_duration_hours=3)
        mode_label = "REAL-TIME LIVE STREAM"
        # Injects script to auto-refresh every 2 minutes (120000 ms)
        auto_refresh_script = "<script>setTimeout(() => { window.location.reload(); }, 120000);</script>"

    # Construct complete layout view string inside an explicit box wrapper
    box_content = f"   {top_date}\n" + "\n".join(formatted_lines)   # PatChange: Added 3 spaces in front of top_date

    # Note: Using alternative variable substitution to prevent markdown rendering glitches
    # PatChange: Below was max-width: 700px;
    # PatChange: Below was <div class="header-row">Time          Rate of Rise (°C/h) Temp (°C)</div>

    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>IoT Temperature Monitor</title>
        <!-- This meta tag forces mobile browsers to scale everything perfectly -->
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
        <style>
            body { 
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; 
                background-color: #f4f6f9; 
                color: #333; 
                margin: 0; 
                padding: 10px;
                box-sizing: border-box;
            }
            .container { 
                width: 100%; 
                max-width: 340px; 
                margin: 0 auto; 
                background: white; 
                padding: 15px; 
                border-radius: 8px; 
                box-shadow: 0 4px 15px rgba(0,0,0,0.05); 
                box-sizing: border-box;
            }
            h2 { color: #2c3e50; margin-top: 0; font-size: 1.4rem; border-bottom: 2px solid #eaeaea; padding-bottom: 10px; }
            .mode-badge { display: inline-block; padding: 4px 10px; border-radius: 4px; font-size: 12px; font-weight: bold; background: #3498db; color: white; margin-bottom: 15px; }
            .historic-badge { background: #e67e22; }
            
            /* Responsive text row utilizing space-between layout instead of rigid tabs */
            .header-row { 
                font-family: monospace; 
                font-weight: bold; 
                background: #eef2f5; 
                padding: 10px; 
                border-radius: 4px 4px 0 0; 
                display: flex;
                justify-content: space-between;
                border-bottom: 1px solid #ccc;
                font-size: 13px;
            }
            
            /* Scalable text box maximizing phone real estate */
            textarea { 
                width: 100%; 
                height: calc(100vh - 160px); /* Dynamically fills the phone height */
                font-family: monospace; 
                font-size: 14px; 
                line-height: 1.6; 
                padding: 10px; 
                border: 1px solid #ccc; 
                border-top: none; 
                border-radius: 0 0 4px 4px; 
                background-color: #fafafa; 
                resize: none; 
                box-sizing: border-box; 
            }
            .footer { margin-top: 10px; font-size: 11px; color: #7f8c8d; text-align: right; }
        </style>
        REPLACE_REFRESH_SCRIPT
    </head>
    <body>
        <div class="container">
            <h2>IoT Server Monitor</h2>
            <div class="mode-badge REPLACE_BADGE_CLASS">REPLACE_MODE_LABEL</div>
            
            <div class="header-row">
                <span>Time</span>
                <span>Temp (&deg;C)</span>
                <span>Rate of Rise (&deg;C/h)</span>
            </div>
            <textarea readonly>REPLACE_BOX_CONTENT</textarea>
            
            <div class="footer">
                REPLACE_FOOTER_TEXT | Feed: REPLACE_FEED_ID
            </div>
        </div>
    </body>
    </html>
    """

    # Safe text replacements to construct the final HTML response
    html_content = html_content.replace("REPLACE_REFRESH_SCRIPT", auto_refresh_script)
    html_content = html_content.replace("REPLACE_BADGE_CLASS", "historic-badge" if is_historic else "")
    html_content = html_content.replace("REPLACE_MODE_LABEL", mode_label)
    html_content = html_content.replace("REPLACE_BOX_CONTENT", box_content)
    html_content = html_content.replace("REPLACE_FOOTER_TEXT", "Static Snapshot Mode" if is_historic else "Auto-refreshing every 2 mins")
    html_content = html_content.replace("REPLACE_FEED_ID", FEED_ID)

    return HTMLResponse(content=html_content) 

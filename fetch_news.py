#!/usr/bin/env python3
"""
Daily News Fetcher
Fetches news from RSS feeds (36kr, TechCrunch, Techmeme), filters by category,
translates Chinese content, and stores results in Supabase.
"""

import os
import re
import json
import time
import logging
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional

import feedparser
import requests
from bs4 import BeautifulSoup

# ─── Configuration ───────────────────────────────────────────────────────────

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"


RSS_FEEDS = [
    {"name": "36kr", "url": "https://36kr.com/feed", "language": "zh"},
    {"name": "TechCrunch", "url": "https://techcrunch.com/feed/", "language": "en"},
    {"name": "Techmeme", "url": "https://www.techmeme.com/feed.xml", "language": "en"},
]

ALLOWED_CATEGORIES = ["New Product", "New Feature", "New VC Investment"]

EXCLUDED_TOPICS = [
    # Energy related
    "energy", "solar", "wind power", "nuclear", "oil", "gas pipeline",
    "battery storage", "renewable energy", "fossil fuel",
    # Hardware related
    "hardware", "chip fabrication", "semiconductor manufacturing",
    "processor launch", "GPU release", "motherboard", "chip design",
    "CPU", "RAM", "SSD", "hard drive", "display panel", "sensor",
    "wearable device", "smart watch", "VR headset", "AR glasses",
    "robot hardware", "drone hardware", "3D printer", "IoT device",
    "smartphone launch", "laptop launch", "tablet launch",
    # Finance related (except VC)
    "finance", "stock market", "banking", "interest rate", "federal reserve",
    "treasury", "bond market", "forex", "cryptocurrency price",
    "IPO", "earnings report", "quarterly results",
    # Non-AI developer tools and packages
    "npm package", "python package", "ruby gem", "crate release",
    "library release", "framework release", "SDK release",
    "programming language update", "compiler update", "runtime update",
    "git tool", "CI/CD", "devops tool", "testing framework",
    "code editor", "IDE plugin", "linter", "formatter",
    "database release", "web framework", "CSS framework",
]

# Hard-coded keyword blocklist — if ANY of these appear in the title (case-insensitive),
# the article is EXCLUDED before LLM categorization even runs.
# This is a safety net to catch hardware/chip articles that the LLM might misclassify.
HARD_EXCLUDE_TITLE_KEYWORDS = [
    # Chips / semiconductors
    "chip startup", "chip maker", "chipmaker", "chip company", "chip business",
    "chip design", "chip fab", "chip plant", "chip factory",
    "semiconductor", "wafer", "foundry", "TSMC", "ASML",
    "AI chip", "ai chip", "custom chip", "custom silicon",
    # GPUs / processors
    "GPU", "TPU", "NPU", "processor", "CPU",
    "Nvidia", "NVIDIA", "nvidia", "AMD", "Intel",
    # Robotics / hardware
    "robot", "robotics", "humanoid", "quadruped", "robotic",
    "eVTOL", "evtol", "EVTOL", "flying car", "flying taxi",
    "drone", "satellite", "spacecraft", "rocket",
    # Automotive
    "electric vehicle", "EV battery", "self-driving car", "autonomous vehicle",
    "Tesla", "Rivian", "Lucid Motors",
    # Energy
    "solar panel", "wind turbine", "nuclear reactor", "power plant",
    # Biotech / pharma
    "biotech", "pharmaceutical", "drug trial", "clinical trial",
]

# Exceptions: VC/investment topics should NOT be excluded
ALLOWED_FINANCE_KEYWORDS = [
    "venture capital", "vc", "funding round", "series a", "series b", "series c",
    "seed round", "raised", "investment round", "startup funding", "angel investor",
    "accelerator", "incubator",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── LLM Client (Gemini 2.5 Flash with Google Search) ───────────────────────

LLM_MAX_RETRIES = 3
LLM_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def call_llm(prompt: str, system: str = "", max_tokens: int = 2000, use_search: bool = True) -> str:
    """Call Gemini 2.5 Flash and return the response text.
    
    Retries up to LLM_MAX_RETRIES times with exponential backoff for transient
    errors (429, 500, 502, 503, 504).
    
    Args:
        use_search: If True, enable Google Search grounding. Disable for translation.
    """
    if not GEMINI_API_KEY:
        log.error("GEMINI_API_KEY is not set!")
        return ""

    # Combine system prompt and user prompt into a single message
    full_prompt = f"{system}\n\n{prompt}" if system else prompt

    request_body = {
        "contents": [{"parts": [{"text": full_prompt}]}],
        "generationConfig": {
            "maxOutputTokens": max_tokens,
            "temperature": 0.3,
        },
    }
    if use_search:
        request_body["tools"] = [{"google_search": {}}]

    def _do_request():
        resp = requests.post(
            f"{GEMINI_URL}?key={GEMINI_API_KEY}",
            headers={"Content-Type": "application/json"},
            json=request_body,
            timeout=60,
        )
        return resp

    def _extract_text(resp):
        data = resp.json()
        if "candidates" in data:
            parts = data["candidates"][0]["content"]["parts"]
            text_parts = [p["text"] for p in parts if "text" in p]
            return " ".join(text_parts).strip()
        log.error(f"Gemini returned no candidates: {resp.text[:200]}")
        return ""

    for attempt in range(LLM_MAX_RETRIES + 1):
        try:
            resp = _do_request()

            if resp.status_code == 200:
                return _extract_text(resp)

            if resp.status_code in LLM_RETRYABLE_STATUS_CODES:
                # Exponential backoff: 3s, 6s, 12s
                wait = 3 * (2 ** attempt)
                log.warning(f"Gemini {resp.status_code} error (attempt {attempt + 1}/{LLM_MAX_RETRIES + 1}), retrying in {wait}s...")
                time.sleep(wait)
                continue

            # Non-retryable error
            log.error(f"Gemini error {resp.status_code}: {resp.text[:200]}")
            return ""

        except requests.exceptions.Timeout:
            wait = 3 * (2 ** attempt)
            log.warning(f"Gemini request timed out (attempt {attempt + 1}/{LLM_MAX_RETRIES + 1}), retrying in {wait}s...")
            time.sleep(wait)
            continue
        except Exception as e:
            log.error(f"LLM call failed: {e}")
            return ""

    log.error(f"Gemini failed after {LLM_MAX_RETRIES + 1} attempts")
    return ""


# ─── RSS Fetching ────────────────────────────────────────────────────────────

def fetch_rss(feed_config: dict) -> list[dict]:
    """Fetch and parse an RSS feed, returning entries from the last 24 hours."""
    url = feed_config["url"]
    name = feed_config["name"]
    language = feed_config["language"]

    log.info(f"Fetching RSS feed: {name} ({url})")

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; NewsFetcher/1.0)"
        }
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
    except Exception as e:
        log.error(f"Failed to fetch {name}: {e}")
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    entries = []

    for entry in feed.entries:
        pub_date = parse_entry_date(entry)
        if pub_date is None:
            pub_date = datetime.now(timezone.utc)

        if pub_date < cutoff:
            continue

        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()
        summary = entry.get("summary", "") or entry.get("description", "")

        if summary:
            soup = BeautifulSoup(summary, "html.parser")
            summary = soup.get_text(separator=" ", strip=True)

        image = extract_image_from_entry(entry, summary)

        entries.append({
            "title": title,
            "url": link,
            "content": summary[:3000],
            "datetime": pub_date,
            "source": name,
            "language": language,
            "image": image,
        })

    log.info(f"  Found {len(entries)} entries from last 24h in {name}")
    return entries


def parse_entry_date(entry) -> Optional[datetime]:
    """Parse the publication date from a feed entry."""
    for attr in ["published_parsed", "updated_parsed"]:
        parsed = getattr(entry, attr, None)
        if parsed:
            try:
                dt = datetime(*parsed[:6], tzinfo=timezone.utc)
                return dt
            except Exception:
                pass

    for attr in ["published", "updated", "dc_date"]:
        raw = entry.get(attr)
        if raw:
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass

    return None


def extract_image_from_entry(entry, summary: str) -> Optional[str]:
    """Try to extract a main image URL from the feed entry."""
    media_content = entry.get("media_content", [])
    if media_content:
        for media in media_content:
            url = media.get("url", "")
            if url and any(ext in url.lower() for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"]):
                return url

    media_thumbnail = entry.get("media_thumbnail", [])
    if media_thumbnail:
        for thumb in media_thumbnail:
            url = thumb.get("url", "")
            if url:
                return url

    enclosures = entry.get("enclosures", [])
    for enc in enclosures:
        if enc.get("type", "").startswith("image/"):
            return enc.get("href", "") or enc.get("url", "")

    raw_summary = entry.get("summary", "") or entry.get("description", "")
    if raw_summary:
        soup = BeautifulSoup(raw_summary, "html.parser")
        img = soup.find("img")
        if img and img.get("src"):
            return img["src"]

    content_encoded = entry.get("content", [])
    if content_encoded:
        for c in content_encoded:
            val = c.get("value", "")
            if val:
                soup = BeautifulSoup(val, "html.parser")
                img = soup.find("img")
                if img and img.get("src"):
                    return img["src"]

    return None



# ─── Translation ─────────────────────────────────────────────────────────────

def _contains_chinese(text: str) -> bool:
    """Check if text contains Chinese characters."""
    return bool(re.search(r'[\u4e00-\u9fff]', text))


def _extract_json_from_text(text: str) -> dict | None:
    """Try multiple strategies to extract JSON from LLM response text.
    
    Handles common issues:
    - Direct JSON responses
    - JSON wrapped in markdown code blocks
    - Truncated JSON where the content value is cut off mid-string
    """
    if not text:
        return None

    # Clean up: remove markdown code block wrapper if present
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
        cleaned = re.sub(r'\s*```$', '', cleaned)
        cleaned = cleaned.strip()

    # Strategy 1: Direct JSON parse
    try:
        obj = json.loads(cleaned)
        if isinstance(obj, dict) and "title" in obj:
            return obj
    except json.JSONDecodeError:
        pass

    # Strategy 2: Extract title and content using regex (handles truncated JSON)
    # This works even when the JSON is cut off mid-content or mid-title
    
    # Try to get a complete title (with closing quote)
    title_match = re.search(r'"title"\s*:\s*"((?:[^"\\]|\\.)*)"', cleaned)
    if not title_match:
        # Title itself is truncated (no closing quote) — extract what we have
        title_match_partial = re.search(r'"title"\s*:\s*"((?:[^"\\]|\\.)*)', cleaned)
        if title_match_partial:
            title_val = title_match_partial.group(1)
            if title_val.endswith("\\"):
                title_val = title_val[:-1]
            return {"title": title_val, "content": ""}
        return None

    title_val = title_match.group(1)
    content_val = ""
    content_match = re.search(r'"content"\s*:\s*"((?:[^"\\]|\\.)*)', cleaned)
    if content_match:
        content_val = content_match.group(1)
        # Clean up: remove trailing incomplete escape sequences
        if content_val.endswith("\\"):
            content_val = content_val[:-1]
    return {"title": title_val, "content": content_val}


def translate_to_english(title: str, content: str) -> tuple[str, str]:
    """Translate Chinese title and content to English using LLM.
    
    Uses a two-phase approach:
    1. First tries combined title+content translation (fast, single call)
    2. Falls back to separate title-only translation if combined fails
    
    Truncates content to 800 chars to avoid exceeding token limits.
    """
    # Truncate content more aggressively to avoid token overflow
    truncated_content = content[:800]

    prompt = f"""Translate the following Chinese news title and content into English.
Return ONLY a JSON object with keys "title" and "content". No markdown, no code blocks, no explanation.

Title: {title}

Content: {truncated_content}"""

    system = "You are a professional translator. Translate Chinese to English accurately. Return a single JSON object only: {\"title\": \"...\", \"content\": \"...\"}. Keep it concise."

    # Attempt 1: Combined translation with higher token limit
    for attempt in range(2):
        result = call_llm(prompt, system, max_tokens=3000, use_search=False)

        if not result:
            log.warning(f"Translation attempt {attempt + 1} returned empty, retrying...")
            time.sleep(1)
            continue

        data = _extract_json_from_text(result)
        if data:
            translated_title = data.get("title", title)
            translated_content = data.get("content", content)

            if not _contains_chinese(translated_title):
                return translated_title, translated_content
            else:
                log.warning(f"Translation attempt {attempt + 1} still contains Chinese in title, retrying...")
                time.sleep(1)
                continue
        else:
            log.warning(f"Translation attempt {attempt + 1} failed to parse: {result[:150]}")
            time.sleep(1)
            continue

    # Attempt 2: Translate title only (much shorter, very reliable)
    log.info(f"    Falling back to title-only translation...")
    title_prompt = f"Translate this Chinese headline to English. Return ONLY the English translation, nothing else.\n\n{title}"
    title_result = call_llm(title_prompt, "", max_tokens=500, use_search=False)

    if title_result and not _contains_chinese(title_result.strip()):
        translated_title = title_result.strip().strip('"').strip()
        # For content, try a separate call
        if truncated_content.strip():
            content_prompt = f"Translate this Chinese text to English. Return ONLY the English translation, nothing else.\n\n{truncated_content}"
            content_result = call_llm(content_prompt, "", max_tokens=2000, use_search=False)
            if content_result and not _contains_chinese(content_result.strip()[:100]):
                return translated_title, content_result.strip()
        return translated_title, content  # Return original content if content translation fails

    log.error(f"All translation attempts failed for: {title[:50]}")
    return title, content


# ─── Categorization & Filtering ──────────────────────────────────────────────

CATEGORIZATION_CHUNK_SIZE = 15  # Max articles per batch LLM call


def _pre_filter_by_keywords(entries: list[dict]) -> list[dict]:
    """Hard-coded keyword pre-filter. Removes articles whose title matches
    any keyword in HARD_EXCLUDE_TITLE_KEYWORDS before LLM categorization.
    
    This is a safety net to catch hardware/chip/robot articles that the LLM
    might misclassify as VC Investment or New Product.
    """
    passed = []
    for entry in entries:
        title_lower = entry['title'].lower()
        excluded = False
        for keyword in HARD_EXCLUDE_TITLE_KEYWORDS:
            if keyword.lower() in title_lower:
                log.info(f"  PRE-FILTER EXCLUDED: \"{entry['title'][:80]}\" (matched: '{keyword}')")
                excluded = True
                break
        if not excluded:
            passed.append(entry)
    return passed


def categorize_and_filter(entries: list[dict]) -> list[dict]:
    """Use LLM to categorize all entries in batch calls with Google Search.
    
    First applies a hard-coded keyword pre-filter to remove obvious hardware/chip
    articles, then splits remaining articles into chunks of CATEGORIZATION_CHUNK_SIZE
    and sends each chunk to Gemini for batch categorization.
    """
    if not entries:
        return []

    # Step 1: Hard-coded keyword pre-filter
    log.info(f"  Running keyword pre-filter on {len(entries)} articles...")
    entries = _pre_filter_by_keywords(entries)
    log.info(f"  {len(entries)} articles passed pre-filter")

    if not entries:
        return []

    # Step 2: LLM categorization
    filtered = []
    total = len(entries)
    
    for chunk_start in range(0, total, CATEGORIZATION_CHUNK_SIZE):
        chunk = entries[chunk_start:chunk_start + CATEGORIZATION_CHUNK_SIZE]
        chunk_num = chunk_start // CATEGORIZATION_CHUNK_SIZE + 1
        total_chunks = (total + CATEGORIZATION_CHUNK_SIZE - 1) // CATEGORIZATION_CHUNK_SIZE
        log.info(f"  Categorizing chunk {chunk_num}/{total_chunks} ({len(chunk)} articles)...")
        
        chunk_results = _categorize_batch(chunk, chunk_start)
        filtered.extend(chunk_results)
        
        # Small delay between chunks to avoid rate limiting
        if chunk_start + CATEGORIZATION_CHUNK_SIZE < total:
            time.sleep(1)

    return filtered


def _categorize_batch(chunk: list[dict], global_offset: int) -> list[dict]:
    """Categorize a batch of articles in a single LLM call."""
    # Build the articles list for the prompt
    entries_text = ""
    for idx, entry in enumerate(chunk):
        entries_text += f"\n{idx + 1}. Title: {entry['title']}\n   URL: {entry.get('url', '')}\n   Source: {entry.get('source', '')}\n   Content: {entry['content'][:500]}\n"

    prompt = f"""You are a strict news categorization expert. Analyze ALL of the following articles and categorize each one.

IMPORTANT: Use Google Search to look up article URLs when the title/content is ambiguous. Understanding the full context is critical for accurate categorization.

CATEGORIES (choose exactly one per article):

1. "New Product" — The article announces a NEWLY LAUNCHED SOFTWARE product, app, website, GitHub project, software tool, platform, or AI model. The key criterion: something brand new is being released or launched for the first time.
   - ONLY software products count. Physical/hardware products do NOT count (for example chip, robots. see EXCLUDE list for more).
   - IMPORTANT: An article about a company's AI model being TRAINED on certain chips is NOT a new product launch.
   - An article about export controls, regulations, or geopolitical issues around tech is NOT a new product.
   - An article about a company raising money is NOT a new product (it's VC investment).

2. "New Feature" — The article announces a NEW FEATURE added to an EXISTING product/app/platform/service.
   - The product must already exist and is getting a new capability or update.
   - Example: "TikTok adds AI shopping feature", "Google Maps adds real-time translation"

3. "New VC Investment" — The article announces a new venture capital investment, funding round, or acquisition of a SOFTWARE/TECH startup.
   - Must involve investment money (seed, Series A/B/C/D, etc.) flowing into a tech/SOFTWARE company.
   - The company receiving investment MUST be a SOFTWARE, SaaS, platform, or AI software company.
   - EXCLUDE if the company receiving investment makes HARDWARE: chips, semiconductors, GPUs, processors, robots, drones, eVTOL, vehicles, batteries, sensors, or any physical product.
   - Example EXCLUDE: "AI chip startup raises $500M" — this is a HARDWARE company, not software. EXCLUDE it.
   - Example EXCLUDE: "Robotics company raises $100M" — this is a HARDWARE company. EXCLUDE it.
   - Example INCLUDE: "SaaS startup raises $50M" — this is a SOFTWARE company. Include it.
   - Employee stock sales or secondary market transactions do NOT count.

4. "EXCLUDE" — The article does NOT fit any of the above three categories. EXCLUDE if:
   - General tech news, analysis, or commentary (not a specific launch/feature/investment)
   - Medicine, healthcare, biotech, pharmaceutical, or life sciences industry
   - Political news, government policy, export controls, trade disputes
   - Energy, power, solar, nuclear, fossil fuels
   - Hardware of ANY kind: chips, semiconductors, GPUs, smartphones, laptops, wearables, VR/AR headsets, IoT, drones, robots, humanoid robots, quadruped robots, robotic dogs, eVTOL aircraft, flying cars, electric vehicles, autonomous vehicles, 3D printers, satellites, space hardware
   - ANY company that designs, manufactures, or sells chips, semiconductors, GPUs, processors, or custom silicon — even if they also use AI. "AI chip startup" is still a HARDWARE company. EXCLUDE.
   - VC investment into hardware companies (chip startups, robotics companies, drone makers, etc.) — EXCLUDE even though it's an investment
   - Robotics: ANY article about physical robots, robot launches, robot demonstrations, or robotic hardware
   - Aviation/aerospace: eVTOL, drones, aircraft, rockets, space launches
   - Automotive: electric cars, self-driving cars, EV batteries, charging infrastructure
   - Finance, banking, stock market, earnings reports (unless it's VC investment in a tech startup)
   - Legal disputes, lawsuits, regulatory actions
   - Personnel changes, hiring, layoffs
   - Geopolitical tensions or trade war news involving tech companies
   - Articles about training AI models on specific hardware (this is NOT a product launch)

When in doubt, ALWAYS choose EXCLUDE.

Articles:
{entries_text}

Respond with ONLY a JSON array of objects, each with "index" (1-based), "category", and "reason" (brief 1-sentence explanation).
Example: [{{"index": 1, "category": "New Product", "reason": "Announces launch of a new AI tool"}}]
No other text."""

    system = "You are a news categorization expert. Categorize tech news articles strictly. Use Google Search to verify article context when needed. Be strict: if an article does not clearly fit New Product, New Feature, or New VC Investment, mark it as EXCLUDE."

    result = call_llm(prompt, system, max_tokens=4000, use_search=True)

    if not result:
        log.error("Empty LLM response for batch categorization")
        return []

    # Parse the JSON array response
    categories = _parse_categorization_response(result, len(chunk))

    filtered = []
    for cat_info in categories:
        idx = cat_info.get("index", 0) - 1
        category = cat_info.get("category", "EXCLUDE")
        reason = cat_info.get("reason", "")

        if 0 <= idx < len(chunk):
            global_idx = global_offset + idx
            if category in ALLOWED_CATEGORIES:
                entry = chunk[idx].copy()
                entry["category"] = category
                filtered.append(entry)
                log.info(f"  [{global_idx+1}] {chunk[idx]['title'][:60]}... -> {category} ({reason})")
            else:
                log.info(f"  [{global_idx+1}] {chunk[idx]['title'][:60]}... -> EXCLUDED ({reason})")

    return filtered


def _parse_categorization_response(result: str, expected_count: int) -> list[dict]:
    """Parse the categorization JSON array from the LLM response."""
    # Try direct JSON parse
    try:
        cleaned = result.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
            cleaned = re.sub(r'\s*```$', '', cleaned)
        categories = json.loads(cleaned)
        if isinstance(categories, list):
            return categories
    except json.JSONDecodeError:
        pass

    # Try to find JSON array in the response
    match = re.search(r'\[\s*\{.*?\}\s*\]', result, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    # Try to find individual JSON objects
    categories = []
    for m in re.finditer(r'\{[^{}]*"index"\s*:\s*(\d+)[^{}]*"category"\s*:\s*"([^"]+)"[^{}]*\}', result):
        try:
            idx = int(m.group(1))
            cat = m.group(2)
            categories.append({"index": idx, "category": cat, "reason": ""})
        except (ValueError, IndexError):
            continue

    if categories:
        return categories

    log.warning(f"Failed to parse categorization response, excluding all")
    return []


# ─── LLM-based Deduplication ─────────────────────────────────────────────────

def deduplicate_articles(entries: list[dict]) -> list[dict]:
    """Use LLM to identify articles covering the same topic and keep only one.
    
    Sends all article titles to Gemini to find duplicates. When duplicates are
    found, keeps the article with the most detailed content.
    """
    if len(entries) <= 1:
        return entries

    # Build article list for the prompt
    articles_text = ""
    for idx, entry in enumerate(entries):
        articles_text += f"\n{idx + 1}. [{entry.get('source', '')}] {entry['title']}"

    prompt = f"""Analyze the following list of news articles and identify groups of articles that cover THE SAME topic or event.

Two articles are duplicates if they report on the same specific event, announcement, or story — even if from different sources or with slightly different wording.

Articles:
{articles_text}

Respond with ONLY a JSON array of duplicate groups. Each group is an array of article indices (1-based) that cover the same topic.
If there are no duplicates, respond with an empty array: []

Example response: [[1, 5], [3, 7, 9]]
This means articles 1&5 are about the same topic, and articles 3, 7, 9 are about the same topic.

Only include groups with 2+ articles. Do NOT include articles that have no duplicates.
Respond with ONLY the JSON array, no other text."""

    system = "You are a news deduplication expert. Identify articles that cover the exact same news event or announcement. Be precise: only group articles that are truly about the same specific story, not just the same general topic."

    result = call_llm(prompt, system, max_tokens=1000, use_search=False)

    if not result:
        log.warning("Empty LLM response for deduplication, keeping all articles")
        return entries

    # Parse duplicate groups
    try:
        cleaned = result.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
            cleaned = re.sub(r'\s*```$', '', cleaned)
        groups = json.loads(cleaned)
        if not isinstance(groups, list):
            groups = []
    except json.JSONDecodeError:
        # Try regex fallback
        match = re.search(r'\[\s*\[.*?\]\s*\]', result, re.DOTALL)
        if match:
            try:
                groups = json.loads(match.group(0))
            except json.JSONDecodeError:
                groups = []
        else:
            groups = []

    if not groups:
        log.info("  No duplicate articles found")
        return entries

    # For each duplicate group, keep the article with the longest content
    indices_to_remove = set()
    for group in groups:
        if not isinstance(group, list) or len(group) < 2:
            continue

        # Convert to 0-based indices and validate
        valid_indices = [i - 1 for i in group if isinstance(i, int) and 1 <= i <= len(entries)]
        if len(valid_indices) < 2:
            continue

        # Find the article with the longest content (most detailed)
        best_idx = max(valid_indices, key=lambda i: len(entries[i].get('content', '')))
        
        # Mark others for removal
        for idx in valid_indices:
            if idx != best_idx:
                indices_to_remove.add(idx)
                log.info(f"  Removing duplicate [{idx+1}] \"{entries[idx]['title'][:60]}...\" (keeping [{best_idx+1}] \"{entries[best_idx]['title'][:60]}...\")")

    # Return entries without the duplicates
    deduplicated = [e for i, e in enumerate(entries) if i not in indices_to_remove]
    log.info(f"  Removed {len(indices_to_remove)} duplicate(s), {len(deduplicated)} articles remaining")
    return deduplicated


# ─── Image Extraction from URL ───────────────────────────────────────────────

def try_extract_image_from_url(url: str) -> Optional[str]:
    """Try to extract the main image from the article URL via Open Graph tags."""
    if not url:
        return None

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; NewsFetcher/1.0)"
        }
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        og_image = soup.find("meta", property="og:image")
        if og_image and og_image.get("content"):
            return og_image["content"]

        tw_image = soup.find("meta", attrs={"name": "twitter:image"})
        if tw_image and tw_image.get("content"):
            return tw_image["content"]

        for img in soup.find_all("img"):
            src = img.get("src", "")
            width = img.get("width", "")
            if src and (not width or int(width or 0) > 200):
                if any(ext in src.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"]):
                    return src

    except Exception as e:
        log.debug(f"Failed to extract image from {url}: {e}")

    return None


# ─── Batch Summarization ────────────────────────────────────────────────────

SUMMARIZATION_CHUNK_SIZE = 10  # Max articles per batch summarization call


def summarize_articles(entries: list[dict]) -> list[dict]:
    """Use LLM to generate a concise 1-sentence summary for each article.
    
    Extracts only the first paragraph of each article and asks the LLM to
    distill it into exactly one sentence. Processes in chunks.
    """
    if not entries:
        return entries

    total = len(entries)
    log.info(f"  Summarizing {total} articles...")

    for chunk_start in range(0, total, SUMMARIZATION_CHUNK_SIZE):
        chunk = entries[chunk_start:chunk_start + SUMMARIZATION_CHUNK_SIZE]
        chunk_num = chunk_start // SUMMARIZATION_CHUNK_SIZE + 1
        total_chunks = (total + SUMMARIZATION_CHUNK_SIZE - 1) // SUMMARIZATION_CHUNK_SIZE
        log.info(f"  Summarizing chunk {chunk_num}/{total_chunks} ({len(chunk)} articles)...")

        _summarize_batch(chunk)

        if chunk_start + SUMMARIZATION_CHUNK_SIZE < total:
            time.sleep(1)

    return entries


def _summarize_batch(chunk: list[dict]) -> None:
    """Summarize a batch of articles in a single LLM call, updating entries in-place."""
    entries_text = ""
    for idx, entry in enumerate(chunk):
        # Extract ONLY the first paragraph
        content = entry['content']
        first_para = content.split('\n\n')[0].split('\n')[0].strip()
        # If first paragraph is too short (< 30 chars), take first 300 chars
        if len(first_para) < 30:
            first_para = content[:300]
        else:
            first_para = first_para[:300]
        entries_text += f"\n{idx + 1}. Title: {entry['title']}\n   First paragraph: {first_para}\n"

    prompt = f"""For each article below, write EXACTLY ONE sentence summarizing the key point.

RULES:
- Use ONLY the first paragraph provided. Do NOT add any information beyond what is in the first paragraph.
- Output must be EXACTLY 1 sentence per article. Not 2, not 3. ONE sentence.
- The sentence should capture the most important fact from the first paragraph.
- Keep it under 150 characters if possible.
- Do NOT start with "The article..." or "This article...". Start directly with the subject.

Articles:
{entries_text}

Respond with ONLY a JSON array of objects, each with "index" (1-based) and "summary" (exactly 1 sentence).
Example: [{{"index": 1, "summary": "Canva acquired animation startup Cavalry and ad-tech startup Mango AI to expand its creative suite."}}]
No other text, no markdown code blocks."""

    system = "You are a professional news editor. Write exactly ONE sentence per article — no more, no less. Be specific with names and numbers. Use ONLY information from the first paragraph provided. Return valid JSON only."

    result = call_llm(prompt, system, max_tokens=4000, use_search=False)

    if not result:
        log.warning("Empty LLM response for summarization, keeping raw content")
        return

    # Parse the JSON array response
    summaries = _parse_summarization_response(result, len(chunk))

    for item in summaries:
        idx = item.get("index", 0) - 1
        summary = item.get("summary", "")
        if 0 <= idx < len(chunk) and summary:
            chunk[idx]["content"] = summary
            log.info(f"    [{idx+1}] Summarized: {chunk[idx]['title'][:50]}...")
        else:
            log.warning(f"    [{idx+1}] Failed to summarize, keeping raw content")


def _parse_summarization_response(result: str, expected_count: int) -> list[dict]:
    """Parse the summarization JSON array from the LLM response."""
    # Clean up markdown code blocks
    cleaned = result.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
        cleaned = re.sub(r'\s*```$', '', cleaned)
        cleaned = cleaned.strip()

    # Try direct JSON parse
    try:
        summaries = json.loads(cleaned)
        if isinstance(summaries, list):
            return summaries
    except json.JSONDecodeError:
        pass

    # Try to find JSON array in the response
    match = re.search(r'\[\s*\{.*?\}\s*\]', result, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    # Regex fallback: extract individual index+summary pairs
    summaries = []
    for m in re.finditer(r'"index"\s*:\s*(\d+)\s*,\s*"summary"\s*:\s*"((?:[^"\\]|\\.)*)"', result):
        try:
            summaries.append({"index": int(m.group(1)), "summary": m.group(2)})
        except (ValueError, IndexError):
            continue

    if summaries:
        return summaries

    log.warning("Failed to parse summarization response")
    return []


# ─── Google Drive CSV Storage ─────────────────────────────────────────────────

NEWS_CSV = "news_raw.csv"
NEWS_HEADERS = [
    "url", "date_of_news", "datetime_of_news", "source", "category",
    "title", "news_content", "main_picture",
]


def store_entries(entries: list[dict]) -> int:
    """Store filtered entries as CSV on Google Drive. Returns count of new rows."""
    if not entries:
        return 0

    from drive_storage import append_csv

    # Use current SGT time as the crawl timestamp
    sgt = timezone(timedelta(hours=8))
    crawl_now = datetime.now(sgt)
    crawl_date = crawl_now.strftime("%Y-%m-%d")
    crawl_datetime = crawl_now.isoformat()

    rows = []
    for entry in entries:
        rows.append({
            "url": entry["url"],
            "date_of_news": crawl_date,
            "datetime_of_news": crawl_datetime,
            "source": entry["source"],
            "category": entry["category"],
            "title": entry["title"][:500],
            "news_content": entry["content"][:5000],
            "main_picture": entry.get("image") or "",
        })

    # append_csv deduplicates by first header field (url)
    inserted = append_csv(NEWS_CSV, rows, NEWS_HEADERS)
    return inserted


# ─── Main Pipeline ───────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Starting daily news fetch pipeline")
    log.info("=" * 60)

    # Validate required env vars
    if not GEMINI_API_KEY:
        log.error("GEMINI_API_KEY is not set!")
        return

    # Step 1: Fetch all RSS feeds
    all_entries = []
    for feed_config in RSS_FEEDS:
        entries = fetch_rss(feed_config)
        all_entries.extend(entries)

    log.info(f"\nTotal entries fetched: {len(all_entries)}")

    if not all_entries:
        log.info("No entries found. Exiting.")
        return

    # Step 2: Translate Chinese entries (36kr)
    for entry in all_entries:
        if entry["language"] == "zh":
            log.info(f"  Translating: {entry['title'][:50]}...")
            entry["title"], entry["content"] = translate_to_english(
                entry["title"], entry["content"]
            )
            time.sleep(0.5)

    # Step 3: Try to extract images for entries without one
    for entry in all_entries:
        if not entry.get("image"):
            img = try_extract_image_from_url(entry["url"])
            if img:
                entry["image"] = img

    # Step 4: Categorize and filter (batch processing)
    log.info("\nCategorizing and filtering entries (batch)...")
    filtered_entries = categorize_and_filter(all_entries)
    log.info(f"Entries after filtering: {len(filtered_entries)}")

    # Step 4.5: LLM-based deduplication
    log.info("\nDeduplicating articles...")
    filtered_entries = deduplicate_articles(filtered_entries)
    log.info(f"Entries after deduplication: {len(filtered_entries)}")

    cat_counts = {}
    for entry in filtered_entries:
        cat = entry.get("category", "Unknown")
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
    for cat, count in sorted(cat_counts.items()):
        log.info(f"  {cat}: {count}")

    # Step 5: Summarize articles (replace raw content with 3-sentence summaries)
    log.info("\nSummarizing articles...")
    summarize_articles(filtered_entries)

    # Step 6: Store in Google Drive CSV
    log.info("\nStoring entries in Google Drive...")
    inserted = store_entries(filtered_entries)
    log.info(f"\nPipeline complete. Inserted {inserted} new entries.")

    # Summary
    log.info("\n" + "=" * 60)
    log.info("SUMMARY")
    log.info(f"  Total fetched:    {len(all_entries)}")
    log.info(f"  After filtering:  {len(filtered_entries)}")
    log.info(f"  New inserted:     {inserted}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

"""
Generate an RSS XML feed from the Product Hunt top products data.
Called after fetch_producthunt_top.py to create a static feed.xml
that is deployed to GitHub Pages.
"""

import os
import sys
import json
import html
from datetime import datetime, timezone
from xml.etree.ElementTree import Element, SubElement, tostring, indent


def fetch_products_from_drive():
    """Fetch the current Product Hunt top products from Google Drive CSV."""
    from drive_storage import read_csv

    products = read_csv("product_hunt_top_product.csv")
    # Sort by rank and convert numeric fields
    for p in products:
        p["rank"] = int(p.get("rank", 0))
        p["votes_count"] = int(p.get("votes_count", 0))
        p["comments_count"] = int(p.get("comments_count", 0))
    products.sort(key=lambda x: x["rank"])
    return products


def generate_rss_xml(products, output_path="public/feed.xml"):
    """Generate RSS 2.0 XML feed from Product Hunt products."""
    print(f"Generating RSS feed with {len(products)} products...")

    now = datetime.now(timezone.utc)
    pub_date = now.strftime("%a, %d %b %Y %H:%M:%S +0000")

    rss = Element("rss", version="2.0")
    rss.set("xmlns:atom", "http://www.w3.org/2005/Atom")

    channel = SubElement(rss, "channel")

    # Channel metadata
    SubElement(channel, "title").text = "Product Hunt — Top Products Today"
    SubElement(channel, "link").text = "https://www.producthunt.com"
    SubElement(channel, "description").text = (
        "Daily top 15 products from Product Hunt, curated by Tech News Daily."
    )
    SubElement(channel, "language").text = "en-us"
    SubElement(channel, "lastBuildDate").text = pub_date
    SubElement(channel, "pubDate").text = pub_date
    SubElement(channel, "ttl").text = "1440"  # 24 hours in minutes

    # Self-referencing atom link (best practice for RSS feeds)
    atom_link = SubElement(channel, "atom:link")
    atom_link.set("href", "https://frankzhu773.github.io/tech-news-daily-data-fetch/feed.xml")
    atom_link.set("rel", "self")
    atom_link.set("type", "application/rss+xml")

    # Add each product as an item
    for product in products:
        item = SubElement(channel, "item")

        name = product.get("name", "Unknown Product")
        tagline = product.get("tagline", "")
        description = product.get("description", "")
        rank = product.get("rank", 0)
        votes = product.get("votes_count", 0)
        comments = product.get("comments_count", 0)
        topics = product.get("topics", "")
        thumbnail = product.get("thumbnail_url", "")
        website_url = product.get("website_url", "")
        ph_url = product.get("url", "")
        fetch_date = product.get("fetch_date", "")

        # Title: rank + name + tagline
        SubElement(item, "title").text = f"#{rank} {name} — {tagline}"

        # Link: prefer the product's own website, fall back to PH page
        link_url = website_url if website_url else ph_url
        SubElement(item, "link").text = link_url

        # Build rich HTML description
        desc_parts = []
        if thumbnail:
            desc_parts.append(
                f'<p><img src="{html.escape(thumbnail)}" alt="{html.escape(name)}" '
                f'style="max-width:300px;border-radius:8px;" /></p>'
            )
        if tagline:
            desc_parts.append(f"<p><strong>{html.escape(tagline)}</strong></p>")
        if description:
            desc_parts.append(f"<p>{html.escape(description)}</p>")

        meta_parts = []
        if votes:
            meta_parts.append(f"⬆ {votes} votes")
        if comments:
            meta_parts.append(f"💬 {comments} comments")
        if topics:
            meta_parts.append(f"🏷 {html.escape(topics)}")
        if meta_parts:
            desc_parts.append(f'<p style="color:#888;">{" · ".join(meta_parts)}</p>')

        if ph_url and link_url != ph_url:
            desc_parts.append(
                f'<p><a href="{html.escape(ph_url)}">View on Product Hunt</a></p>'
            )

        SubElement(item, "description").text = "\n".join(desc_parts)

        # GUID: use PH URL as unique identifier
        guid = SubElement(item, "guid", isPermaLink="true")
        guid.text = ph_url if ph_url else link_url

        # pubDate: use fetch_date if available, otherwise now
        if fetch_date:
            try:
                dt = datetime.strptime(fetch_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                SubElement(item, "pubDate").text = dt.strftime(
                    "%a, %d %b %Y %H:%M:%S +0000"
                )
            except ValueError:
                SubElement(item, "pubDate").text = pub_date
        else:
            SubElement(item, "pubDate").text = pub_date

        # Category tags from topics
        if topics:
            for topic in topics.split(", "):
                topic = topic.strip()
                if topic:
                    SubElement(item, "category").text = topic

    # Pretty-print and write
    indent(rss, space="  ")
    xml_bytes = tostring(rss, encoding="unicode", xml_declaration=False)
    xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_bytes

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(xml_content)

    print(f"  ✓ RSS feed written to {output_path}")
    return output_path


def generate_index_html(output_path="public/index.html"):
    """Generate a simple landing page for the GitHub Pages site."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Tech News Daily — Product Hunt RSS Feed</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 640px;
            margin: 80px auto;
            padding: 0 20px;
            color: #1a1a1a;
            line-height: 1.6;
        }}
        h1 {{ font-size: 1.5rem; margin-bottom: 0.5rem; }}
        .subtitle {{ color: #666; margin-bottom: 2rem; }}
        a {{ color: #e56228; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .feed-link {{
            display: inline-block;
            background: #e56228;
            color: white;
            padding: 10px 24px;
            border-radius: 8px;
            font-weight: 600;
            margin-top: 1rem;
        }}
        .feed-link:hover {{ background: #d04f1a; text-decoration: none; }}
        code {{
            background: #f4f4f4;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 0.9em;
        }}
        .updated {{ color: #999; font-size: 0.85rem; margin-top: 2rem; }}
    </style>
</head>
<body>
    <h1>🚀 Product Hunt — Top Products RSS</h1>
    <p class="subtitle">Daily top 15 products from Product Hunt, curated by Tech News Daily.</p>
    <p>Subscribe to the RSS feed to get daily updates on the hottest new products launching on Product Hunt.</p>
    <p>
        <a class="feed-link" href="feed.xml">📡 Subscribe to RSS Feed</a>
    </p>
    <p style="margin-top: 1.5rem;">
        Or copy this URL into your RSS reader:<br>
        <code>https://frankzhu773.github.io/tech-news-daily-data-fetch/feed.xml</code>
    </p>
    <p class="updated">Last updated: {now}</p>
</body>
</html>"""

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"  ✓ Index page written to {output_path}")
    return output_path


def main():
    print("=" * 60)
    print("Product Hunt RSS Feed Generator")
    print("=" * 60)

    # Fetch products from Google Drive CSV
    products = fetch_products_from_drive()
    print(f"  Fetched {len(products)} products from Google Drive")

    if not products:
        print("  ⚠ No products found, generating empty feed")

    # Generate RSS feed and index page in public/ directory
    generate_rss_xml(products, "public/feed.xml")
    generate_index_html("public/index.html")

    print("\n" + "=" * 60)
    print("✓ RSS feed and index page generated in public/")
    print("=" * 60)


if __name__ == "__main__":
    main()

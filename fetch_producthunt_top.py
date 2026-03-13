"""
Fetch top 15 Product Hunt products of the day and store as CSV on Google Drive.
Overwrites all previous data on each run.
"""

import os
import sys
import requests
import json
from datetime import datetime, timezone

# ── Configuration ──────────────────────────────────────────────
PH_API_KEY = os.environ.get("PH_API_KEY", "")
PH_API_SECRET = os.environ.get("PH_API_SECRET", "")

CSV_FILENAME = "product_hunt_top_product.xlsx"
CSV_HEADERS = [
    "rank", "name", "tagline", "description", "url", "website_url",
    "thumbnail_url", "votes_count", "comments_count", "topics",
    "featured_at", "fetch_date",
]


def get_ph_token():
    """Get OAuth token from Product Hunt API."""
    print("Authenticating with Product Hunt API...")
    resp = requests.post(
        "https://api.producthunt.com/v2/oauth/token",
        json={
            "client_id": PH_API_KEY,
            "client_secret": PH_API_SECRET,
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    print("  Token obtained")
    return token


def fetch_top_products(token, count=15):
    """Fetch top products sorted by ranking from Product Hunt API."""
    print(f"Fetching top {count} products...")

    query = """
    {
      posts(order: RANKING, first: %d) {
        edges {
          node {
            id
            name
            tagline
            description
            slug
            url
            website
            votesCount
            commentsCount
            createdAt
            featuredAt
            thumbnail {
              url
            }
            topics {
              edges {
                node {
                  name
                }
              }
            }
          }
        }
      }
    }
    """ % count

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    resp = requests.post(
        "https://api.producthunt.com/v2/api/graphql",
        json={"query": query},
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    if "errors" in data:
        print(f"  GraphQL errors: {json.dumps(data['errors'])}")
        sys.exit(1)

    posts = data["data"]["posts"]["edges"]
    print(f"  Fetched {len(posts)} products")

    results = []
    for i, edge in enumerate(posts, 1):
        p = edge["node"]
        topics = ", ".join(t["node"]["name"] for t in p["topics"]["edges"])
        thumb = p["thumbnail"]["url"] if p.get("thumbnail") else None

        # Product Hunt detail page URL
        slug = p.get("slug", "")
        ph_url = f"https://www.producthunt.com/posts/{slug}" if slug else p["url"]
        # External website URL
        website_url = p.get("website") or p["url"]

        results.append({
            "rank": i,
            "name": p["name"],
            "tagline": p["tagline"],
            "description": p.get("description", ""),
            "url": ph_url,
            "website_url": website_url,
            "thumbnail_url": thumb,
            "votes_count": p["votesCount"],
            "comments_count": p["commentsCount"],
            "topics": topics,
            "featured_at": p.get("featuredAt"),
            "fetch_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        })
        print(f"  {i}. {p['name']} — {p['votesCount']} votes, {p['commentsCount']} comments")

    return results


def main():
    print("=" * 60)
    print("Product Hunt Top Products Fetcher")
    print("=" * 60)

    # Validate env vars
    missing = []
    if not PH_API_KEY:
        missing.append("PH_API_KEY")
    if not PH_API_SECRET:
        missing.append("PH_API_SECRET")
    if missing:
        print(f"ERROR: Missing environment variables: {', '.join(missing)}")
        sys.exit(1)

    # Step 1: Authenticate
    token = get_ph_token()

    # Step 2: Fetch top 15 products
    products = fetch_top_products(token, count=15)

    # Step 3: Save to Google Drive (Latest + Cumulative)
    from drive_storage import save_latest_and_cumulative
    save_latest_and_cumulative(
        CSV_FILENAME, products, CSV_HEADERS, dedup_keys=["fetch_date", "url"]
    )

    print("\n" + "=" * 60)
    print(f"Done! {len(products)} products saved to {CSV_FILENAME} on Google Drive.")
    print("=" * 60)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
One-time OAuth setup script.

Run this locally to authorize your Google account and obtain a refresh token.
The refresh token is then stored as a GitHub secret for headless use in Actions.

Prerequisites:
  1. Download your OAuth client JSON from Google Cloud Console
     (APIs & Services > Credentials > OAuth 2.0 Client IDs > Download JSON)
  2. Save it as 'client_secret.json' in this directory

Usage:
    pip install google-auth-oauthlib
    python auth_setup.py
"""

import os
import sys
import json
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive"]
CLIENT_SECRET_FILE = os.path.join(os.path.dirname(__file__), "client_secret.json")


def main():
    print("=" * 60)
    print("Google Drive OAuth Setup")
    print("=" * 60)

    if not os.path.exists(CLIENT_SECRET_FILE):
        print(f"\nERROR: {CLIENT_SECRET_FILE} not found.")
        print("Download your OAuth client JSON from Google Cloud Console")
        print("and save it as 'client_secret.json' in this directory.")
        sys.exit(1)

    print("\nA browser will open to authorize access to Google Drive.")
    print("After authorizing, the refresh token will be printed below.\n")

    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)

    try:
        creds = flow.run_local_server(port=0, open_browser=True)
    except Exception:
        auth_url, _ = flow.authorization_url(prompt="consent")
        print(f"\nOpen this URL in your browser:\n\n{auth_url}\n")
        code = input("Enter the authorization code: ").strip()
        flow.fetch_token(code=code)
        creds = flow.credentials

    print("\n" + "=" * 60)
    print("SUCCESS! Set this as a GitHub secret:")
    print("  gh secret set GOOGLE_OAUTH_REFRESH_TOKEN --body '<token>'")
    print("=" * 60)
    print(f"\n{creds.refresh_token}\n")


if __name__ == "__main__":
    main()

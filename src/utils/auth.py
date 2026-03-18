"""
auth.py

Handles Spotify API authentication using the Client Credentials Flow.

- Loads credentials from a `.env` file located in the `config/` directory
- Requests a temporary access token (~1 hour lifespan) from the Spotify Accounts service.
- Provides utility functions to retrieve the token and build the Authorization header
  required for making Spotify Web API requests
"""
import os
import base64
from requests import post
from dotenv import load_dotenv
from pathlib import Path
from src.utils.logger_config import get_logger

# Set the path to the .env file in the config/ directory
dotenv_path = Path(__file__).resolve().parent.parent.parent / "config" / ".env"
load_dotenv(dotenv_path=dotenv_path)

logger = get_logger("Extract_Artist_Scraper")

def get_token():
    """
    Authenticates with Spotify and retrieves an access token using the 
    Client Credentials Flow.

    Returns
    str
        A valid access token string.

    Raises
    Exception
        If client credentials are missing or the authentication request fails.
    """
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

    if not client_id or not client_secret: 
        raise logger.error("Missing client_id or client_secret in environment variables.")
    
    auth_string = f"{client_id}:{client_secret}"
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    #url to send request to 
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    response = post(url, headers=headers, data=data)

    # helpful for debugging
    if response.status_code != 200:
        raise logger.error(f"Failed to authenticate. Status Code: {response.status_code}. Response Text: {response.text}")
    
    return response.json().get("access_token")

def get_auth_headers():
    """
    Returns an Authorization header dictionary for Spotify API requests.
    
    Returns:
        dict: Header with bearer token.
    """
    token = get_token()
    return {"Authorization": f"Bearer {token}"}


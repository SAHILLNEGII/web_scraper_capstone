import logging
import os
import datetime
import csv
import praw
import requests
import pandas as pd
import json
from newspaper import Article
from groq import Groq
from azure.storage.blob import BlobServiceClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Azure Storage Configuration ---
SOURCE_STORAGE_CONNECTION_STRING = os.environ.get("SOURCE_STORAGE_CONNECTION_STRING") # Source Storage Account Connection String
SOURCE_CONTAINER_NAME = os.environ.get("SOURCE_CONTAINER_NAME")       # Source Container Name
SINK_STORAGE_CONNECTION_STRING = os.environ.get("SINK_STORAGE_CONNECTION_STRING") # Sink Storage Account Connection String
SINK_CONTAINER_NAME =  os.environ.get("SINK_CONTAINER_NAME")       # Sink Container Name

# --- API Keys ---
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT")
NEWSDATA_API_KEY = os.environ.get("NEWSDATA_API_KEY")
GUARDIAN_API_KEY = os.environ.get("GUARDIAN_API_KEY")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

# --- Suspicious Keywords ---
suspicious_keywords = [
    'fraud', 'money laundering', 'scam', 'theft', 'embezzlement',
    'bribery', 'corruption', 'phishing', 'fraudulence', 'racket', 'impostor'
]

def download_csv_from_azure(blob_service_client, container_name, blob_name):
    """Downloads a CSV file from Azure Blob Storage."""
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        downloader = blob_client.download_blob()
        csv_content = downloader.readall().decode("utf-8")
        return csv_content
    except Exception as e:
        logging.error(f"Error downloading CSV from Azure: {e}")
        return None

def upload_file_to_azure(blob_service_client, container_name, file_path):
    """Uploads a file to Azure Blob Storage."""
    try:
        blob_name = os.path.basename(file_path)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        logging.info(f"File '{blob_name}' uploaded to Azure Blob Storage.")
        return True
    except Exception as e:
        logging.error(f"Error uploading file to Azure: {e}")
        return False

def scrape_data(individual_name):
    """
    Scrapes data from Reddit, Newsdata.io, and The Guardian for a given individual.
    """
    search_name = f'"{individual_name}"'
    scrape_timestamp = datetime.datetime.now().isoformat()
    combined_data = []

    # --- Reddit Scraping ---
    try:
        logging.info("Scraping Reddit...")
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        subreddit = reddit.subreddit("all")
        search_results = subreddit.search(search_name, sort="new", limit=50)

        for submission in search_results:
            content = ""
            if submission.is_self:
                content = submission.selftext
            else:
                try:
                    article = Article(submission.url)
                    article.download()
                    article.parse()
                    content = article.text
                    if not content.strip():
                        content = "N/A"
                except Exception as e:
                    content = "N/A"

            full_text = submission.title + " " + content
            found_keywords = [kw for kw in suspicious_keywords if kw.lower() in full_text.lower()]

            if found_keywords:
                data_row = {
                    "Platform": "Reddit",
                    "Source/Website": "reddit.com",
                    "URL": submission.url,
                    "Page Title": submission.title,
                    "Content/Excerpt": full_text,
                    "Matched Keywords": ", ".join(found_keywords),
                    "Publication Date": datetime.datetime.fromtimestamp(submission.created_utc).isoformat(),
                    "Scrape Timestamp": scrape_timestamp
                }
                combined_data.append(data_row)
        logging.info("Reddit scraping complete.")
    except Exception as e:
        logging.error(f"Error scraping Reddit: {e}")

    # --- Newsdata.io Scraping ---
    try:
        logging.info("Scraping Newsdata.io...")
        base_url_news = "https://newsdata.io/api/1/latest"
        page = None
        newsdata_limit = 5
        page_count = 0

        while True:
            params = {
                "apikey": NEWSDATA_API_KEY,
                "q": search_name
            }
            if page is not None:
                params["page"] = page

            response = requests.get(base_url_news, params=params)
            if response.status_code != 200:
                logging.error(f"Error: Received status code {response.status_code} from Newsdata.io API.")
                break

            data = response.json()
            if data.get("status") != "success":
                logging.error(f"Error: API did not return success. Response: {data}")
                break

            results = data.get("results", [])
            if not results:
                break

            for article in results:
                article_title = article.get("title", "")
                article_url = article.get("link", "")
                content = article.get("content", "")
                if not content or not content.strip():
                    try:
                        art = Article(article_url)
                        art.download()
                        art.parse()
                        content = art.text
                        if not content.strip():
                            content = "N/A"
                    except Exception as e:
                        content = "N/A"

                full_text = article_title + " " + content
                found_keywords = [kw for kw in suspicious_keywords if kw.lower() in full_text.lower()]
                if found_keywords:
                    data_row = {
                        "Platform": "Newsdata.io",
                        "Source/Website": article.get("source_url", "newsdata.io"),
                        "URL": article_url,
                        "Page Title": article_title,
                        "Content/Excerpt": full_text,
                        "Matched Keywords": ", ".join(found_keywords),
                        "Publication Date": article.get("pubDate", "N/A"),
                        "Scrape Timestamp": scrape_timestamp
                    }
                    combined_data.append(data_row)

            next_page = data.get("nextPage")
            if next_page:
                page = next_page
                page_count += 1
                if page_count >= newsdata_limit:
                    logging.info(f"Reached the limit of {newsdata_limit} pages for Newsdata.io.")
                    break
            else:
                break
        logging.info("Newsdata.io scraping complete.")
    except Exception as e:
        logging.error(f"Error scraping Newsdata.io: {e}")

    # --- The Guardian Scraping ---
    try:
        logging.info("Scraping The Guardian...")
        base_url_guardian = "https://content.guardianapis.com/search"
        page = 1
        guardian_limit = 3
        page_count = 0

        while True:
            params = {
                "q": search_name,
                "api-key": GUARDIAN_API_KEY,
                "show-fields": "bodyText",
                "page": page,
                "page-size": 50
            }

            response = requests.get(base_url_guardian, params=params)
            if response.status_code != 200:
                logging.error(f"Error: Received status code {response.status_code} from The Guardian API.")
                break

            data = response.json()
            if data.get("response", {}).get("status") != "ok":
                logging.error(f"Error in API response: {data}")
                break

            results = data["response"].get("results", [])
            if not results:
                break

            for result in results:
                title = result.get("webTitle", "")
                url = result.get("webUrl", "")
                pub_date = result.get("webPublicationDate", "N/A")
                fields = result.get("fields", {})
                content = fields.get("bodyText", "")
                full_text = title + " " + content
                found_keywords = [kw for kw in suspicious_keywords if kw.lower() in full_text.lower()]
                if found_keywords:
                    data_row = {
                        "Platform": "The Guardian",
                        "Source/Website": "theguardian.com",
                        "URL": url,
                        "Page Title": title,
                        "Content/Excerpt": full_text,
                        "Matched Keywords": ", ".join(found_keywords),
                        "Publication Date": pub_date,
                        "Scrape Timestamp": scrape_timestamp
                    }
                    combined_data.append(data_row)

            current_page = data["response"].get("currentPage", 1)
            total_pages = data["response"].get("pages", 1)
            if current_page >= total_pages:
                break
            else:
                page += 1
                page_count += 1
                if page_count >= guardian_limit:
                    logging.info(f"Reached the limit of {guardian_limit} pages for The Guardian.")
                    break
        logging.info("The Guardian scraping complete.")
    except Exception as e:
        logging.error(f"Error scraping The Guardian: {e}")

    return combined_data

def analyze_text_with_llama3(text, model="llama3-8b-8192"):
    """
    Uses Groq's API to analyze text using LLaMA.
    Returns a dictionary with keys: sentiment, emotion, key_themes, and confidence.
    Text is truncated to 4000 characters to fit within token limits.
    """
    client = Groq(api_key=GROQ_API_KEY)
    prompt = f"""
    Analyze the following text in detail. Return a JSON response with:
    1. Overall sentiment (positive, neutral, or negative)
    2. Dominant emotion (e.g., anger, joy, surprise)
    3. Key themes/topics (list up to 3)
    4. A confidence score (between 0 and 1) for the sentiment.

    Text: {text[:4000]}
    """
    try:
        response = client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model=model,
            temperature=0.3,
            response_format={"type": "json_object"}
        )
        result_str = response.choices[0].message.content
        try:
            result = json.loads(result_str)
        except json.JSONDecodeError as e:
            logging.error(f"JSONDecodeError: {e}")
            logging.error(f"Response content: {result_str}")  # Print the problematic response
            result = {"sentiment": None, "emotion": None, "key_themes": None, "confidence": None}

    except Exception as e:
        logging.error(f"Error analyzing text: {e}")
        result = {"sentiment": None, "emotion": None, "key_themes": None, "confidence": None}
    return result

def process_and_analyze_data(combined_data):
    """
    Performs sentiment analysis on combined data using LLaMA via Groq.
    """
    sentiments = []
    emotions = []
    key_themes_list = []
    confidences = []

    logging.info("Performing sentiment analysis on combined data...")

    df_combined = pd.DataFrame(combined_data)

    for idx, row in df_combined.iterrows():
        text = row["Content/Excerpt"]
        if not isinstance(text, str) or len(text.strip()) == 0:
            result = {"sentiment": None, "emotion": None, "key_themes": None, "confidence": None}
        else:
            result = analyze_text_with_llama3(text)

        sentiments.append(result.get("sentiment"))
        emotions.append(result.get("emotion"))
        key_themes_list.append(result.get("key_themes"))
        confidences.append(result.get("confidence"))

        if idx % 5 == 0:
            logging.info(f"Processed row {idx}")

    df_combined["Sentiment"] = sentiments
    df_combined["Emotion"] = emotions
    df_combined["Key Themes"] = key_themes_list
    df_combined["Confidence"] = confidences

    logging.info("Sentiment analysis complete.")
    return df_combined

def main(blob_name: str):
    """
    Main function to orchestrate the scraping, analysis, and saving process.
    This function will be triggered when a new blob is uploaded to the source container.
    """
    try:
        logging.info(f"Function triggered by blob: {blob_name}")

        # Initialize Azure Blob Storage clients
        source_blob_service_client = BlobServiceClient.from_connection_string(SOURCE_STORAGE_CONNECTION_STRING)
        sink_blob_service_client = BlobServiceClient.from_connection_string(SINK_STORAGE_CONNECTION_STRING)

        # 1. Download CSV from Azure Blob Storage
        logging.info(f"Downloading {blob_name} from source container...")
        csv_content = download_csv_from_azure(source_blob_service_client, SOURCE_CONTAINER_NAME, blob_name)
        if not csv_content:
            logging.error("Failed to download CSV content. Aborting.")
            return

        # 2. Read names from CSV content
        names_to_scrape = []
        csv_reader = csv.reader(csv_content.splitlines())
        next(csv_reader, None)  # Skip header
        for row in csv_reader:
            if row:
                names_to_scrape.append(row[0].strip())  # Assuming name is in the first column

        logging.info(f"Extracted {len(names_to_scrape)} names from CSV.")

        # 3. Scrape and combine data for each name
        all_combined_data = []
        for name in names_to_scrape:
            logging.info(f"Processing name: {name}")
            combined_data = scrape_data(name)
            all_combined_data.extend(combined_data)

        # 4. Perform sentiment analysis
        if all_combined_data:
            df_with_sentiment = process_and_analyze_data(all_combined_data)

            # 5. Save the combined scraped data with sentiment analysis to a CSV file
            output_csv = "combined_scraped_data_sentiment.csv"
            df_with_sentiment.to_csv(output_csv, index=False)

            # 6. Upload the CSV file to the sink container
            if upload_file_to_azure(sink_blob_service_client, SINK_CONTAINER_NAME, output_csv):
                logging.info(f"Successfully processed {blob_name} and uploaded the results.")
            else:
                logging.error(f"Failed to upload the results to the sink container for {blob_name}.")
        else:
            logging.warning("No data was scraped. Sentiment analysis and upload skipped.")

    except Exception as e:
        logging.exception(f"An error occurred in the main function: {e}")

# Example usage (for local testing - replace with Azure Function trigger):
if __name__ == "__main__":
   #Set environment variables
    os.environ["SOURCE_STORAGE_CONNECTION_STRING"] = ""
    os.environ["SOURCE_CONTAINER_NAME"] = "source"
    os.environ["SINK_STORAGE_CONNECTION_STRING"] = ""
    os.environ["SINK_CONTAINER_NAME"] = "sink"
    os.environ["REDDIT_CLIENT_ID"] = ""
    os.environ["REDDIT_CLIENT_SECRET"] = ""
    os.environ["REDDIT_USER_AGENT"] = "Web Scraper"
    os.environ["NEWSDATA_API_KEY"] = ""
    os.environ["GUARDIAN_API_KEY"] = ""
    os.environ["GROQ_API_KEY"] = ""
    #Replace with the actual blob name for local testing
    blob_name = "potential_sus.csv"
    main(blob_name)
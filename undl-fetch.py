import requests
import os
import time


dhlauth_ids = [
    432389,
    425704,
    436882,
    462923,
    476807,
    486475,
    531122,
    581751,
    601507,
    619394,
    638152,
    655149,
    683329,
    703892,
    722814,
    749122,
    759303,
    778125,
    795957,
    812913,
    831455,
    848250,
    863282,
    873025,
    884010,
    890847,
    898143,
    911460,
    919902,
    926970,
    934819,
    939018,
]

# record_ids = [
#     3903489,
# ]


url = "https://digitallibrary.un.org"


def fetch_meeting_records(dhlauth_id) -> list | None:
    endpoint = f"{url}/search?of=recjson&fct__1=Meeting%20Records&fct__2=General%20Assembly&p=%28DHLAUTH%29{dhlauth_id}"

    try:
        print("Fetching results for DHLAUTH ID:", dhlauth_id)
        response = requests.get(endpoint)
        if response.status_code == 200:
            print("Data fetched successfully: 200")
            response_json = response.json()
            if isinstance(response_json, list) and response_json:
                return response_json
            else:
                print("Unexpected JSON structure: json")
                return None
        else:
            print(f"Error fetching data: {response.status_code}")
            return None

    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None


def fetch_record_data(record_id) -> dict | None:
    endpoint = f"{url}/record/{record_id}?of=recjson"

    try:
        response = requests.get(endpoint)
        if response.status_code == 200:
            response_json = response.json()
            if isinstance(response_json, list) and response_json:
                return response_json[0]
            elif isinstance(response_json, dict):
                return response_json
            else:
                print("Unexpected JSON structure: json")
                return None
        else:
            print(f"Error fetching data: {response.status_code}")
            return None

    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None


def get_transcript_pdf_url(record_data: dict) -> str | None:
    files = record_data["files"]
    if isinstance(files, list):
        for file in files:
            if file.get("description") == "English":
                return file.get("url")
    elif isinstance(files, dict):
        if files.get("description") == "English":
            return files.get("url")
    else:
        print("Unexpected JSON structure: json[\"files\"]")
    print("No English transcript PDF found.")
    return None


def download_pdf(url, save_folder="./data"):
    # Ensure the destination folder exists
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    # Extract the filename from the URL or specify a custom one
    document_name = url.split("/")[-1]
    session_number = document_name.split("_")[1].zfill(2)
    meeting_number = document_name.split(".")[1].split("-")[0].zfill(2)
    file_extension = document_name.split(".")[-1]

    # Create a subdirectory for the session number
    subdir_path = os.path.join(save_folder, f"session_{session_number}")
    if not os.path.exists(subdir_path):
        os.makedirs(subdir_path)

    file_path = os.path.join(subdir_path, f"meeting_{session_number}_{meeting_number}.{file_extension}")

    try:
        # Send a GET request to the URL, stream=True allows handling large files efficiently
        with requests.get(url, stream=True) as r:
            r.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

            # Open the local file in write-binary mode and write the content in chunks
            with open(file_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192): # 8 KB chunks
                    if chunk: # Filter out keep-alive new chunks
                        f.write(chunk)
    except requests.exceptions.RequestException as e:
        print(f"Download failed: {e}")



if __name__ == "__main__":
    for dhlauth_id in dhlauth_ids:
        results = fetch_meeting_records(dhlauth_id)
        if not results:
            print("No results found for DHLAUTH ID: ", dhlauth_id)
            continue
        for result in results:
            pdf_url = get_transcript_pdf_url(result)
            if not pdf_url:
                print("  Transcript PDF URL not found for Record ID: ", result.get("recid"))
                continue
            print("  Downloading PDF for Record ID: ", result.get("recid"))
            download_pdf(pdf_url)
            time.sleep(10)  # brief pause between downloads
        print("Completed DHLAUTH ID: ", dhlauth_id, "\n")

    # for record_id in record_ids:
    #     record_data = fetch_record_data(record_id)
    #     if not record_data:
    #         print("No record data found: ", record_id)
    #         continue
    #     pdf_url = get_transcript_pdf_url(record_data)
    #     if not pdf_url:
    #         print("Transcript PDF URL not found: ", record_id)
    #         continue
    #     download_pdf(pdf_url)
    #     print("Downloaded: ", record_id, "\n")

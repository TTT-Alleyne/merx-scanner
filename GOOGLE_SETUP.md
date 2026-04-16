# Google Sheets Setup — One Time Only

Follow these steps ONCE to connect the scanner to Google Sheets.

## Step 1 — Create a Google Cloud Project

1. Go to: https://console.cloud.google.com/
2. Click "Select a project" → "New Project"
3. Name it: "AlleyneAdmAgent"
4. Click "Create"

## Step 2 — Enable APIs

1. Go to: APIs & Services → Enable APIs
2. Search and enable: "Google Sheets API"
3. Search and enable: "Google Drive API"

## Step 3 — Create Service Account

1. Go to: APIs & Services → Credentials
2. Click "Create Credentials" → "Service Account"
3. Name: "merx-scanner"
4. Click "Create and Continue" → "Done"
5. Click on the service account you just created
6. Go to "Keys" tab → "Add Key" → "Create new key" → JSON
7. A file downloads automatically — rename it to: google_credentials.json
8. Put it in your merx-scanner folder

## Step 4 — Share your Google Drive folder

1. Open Google Drive
2. Find or create folder: "AlleyneAdmAgent"
3. Right-click → Share
4. Open google_credentials.json and find "client_email" field
5. Share the folder with that email address (give Editor access)
6. Copy the folder ID from the URL: 
   https://drive.google.com/drive/folders/THIS_IS_YOUR_FOLDER_ID
7. Put that ID in your .env file as GOOGLE_FOLDER_ID

## Step 5 — Run the scanner!

That's it. The scanner will automatically create and update the sheet.

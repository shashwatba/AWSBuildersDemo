# AWSBuildersDemo

# Claude scraping setup notes: 

2. **Install Chrome and ChromeDriver (for Selenium):**
   - Download and install Google Chrome
   - Download ChromeDriver from https://chromedriver.chromium.org/
   - Add ChromeDriver to your PATH or place it in the same directory as the script

## Configuration

### Environment Variables

Set these environment variables or modify the script configuration:

```bash
# AWS Configuration (Required)
export AWS_ACCESS_KEY_ID="your_actual_aws_access_key"
export AWS_SECRET_ACCESS_KEY="your_actual_aws_secret_key"
export AWS_REGION="us-east-1"
export S3_BUCKET_NAME="your-iscc-pdfs-bucket"

# Bedrock Configuration (Required for LLM parsing)
export BEDROCK_REGION="us-east-1"
export BEDROCK_MODEL_ID="anthropic.claude-3-sonnet-20240229-v1:0"

# Scraping Service (Choose one)
export SCRAPING_SERVICE="selenium"  # Options: selenium, brightdata, tavily

# Optional API Keys (if using paid services)
export BRIGHTDATA_API_KEY="your_brightdata_api_key"
export TAVILY_API_KEY="your_tavily_api_key"
```


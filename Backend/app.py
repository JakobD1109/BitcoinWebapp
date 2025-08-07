from scrape_logic import main

def lambda_handler(event, context):
    try:
        main()
        return {
            "statusCode": 200,
            "body": "Scraping and insertion succeeded"
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Scraping failed: {str(e)}"
        }

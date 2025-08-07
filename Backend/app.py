from scrape_logic_lambda import main

def lambda_handler(event, context):
    try:
        main()
        return {
            "statusCode": 200,
            "body": "✅ Scraping and insertion succeeded"
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"❌ An error occurred: {str(e)}"
        }

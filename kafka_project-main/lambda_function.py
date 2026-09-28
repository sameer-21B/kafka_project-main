import db_ingest_script

def lambda_handler(event, context):
    print("Lambda triggered. Initializing db_ingest_script...")
    
    # Call the main function from your script
    result = db_ingest_script.main_function()
    print(result)
    
    return {
        'statusCode': 200,
        'body': f"Lambda executed successfully. Result: {result}"
    }
import json

def writeOpenAiRequest(filename: str, attributes: str, index: int):
    model_name = 'gpt-4o-mini'
    context = "Generate a self-description of this person. Stay concise and aim for each sentence to demonstrate a specific interest, value, or lifestyle detail of the person. Feel free to make things up but don't ramble."
    
    # Construct request dictionary
    request_data = {
        "custom_id": f"request-{index}",
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": model_name,
            "messages": [
                {"role": "system", "content": context},
                {"role": "user", "content": attributes}
            ],
            "max_tokens": 1000
        }
    }
    
    # Write JSON data to file
    with open(filename, "a") as file:
        json.dump(request_data, file)
        file.write("\n")  




    
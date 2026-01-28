import ask_llm
import requests
import json
import pprint


def check_llm_status():
    print('Check for current LLM running status')
    url = "http://192.168.1.26:1234/api/v0/models"
    # Check if the request was successful
    response = requests.get(url)
    if response.status_code == 200:
        # Parse and pretty-print the JSON response
        models = response.json()
        print(json.dumps(models, indent=2))
    
        # If you want to print just the model names/IDs
        print("\nAvailable models:")
        for model in models.get("data", []):
            print(f"- {model.get('id')}")
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
    return print('LLM Status check finished')


def get_industry_stocks(article):
    """
    Function that takes an article as input and returns industry information and stock recommendations
    
    Args:
        article (str): The article text to analyze
        
    Returns:
        dict: The complete response from the API
        str: Just the model's content response if successful
    """
    url = "http://192.168.1.26:1234/api/v0/chat/completions"
    
    # Set the headers
    headers = {
        "Content-Type": "application/json"
    }
    
    # Set the data
    data = {
        "model": "gemma-3-27b-it",
        "messages": [
            {"role": "system", "content": "Only Identify the industry and give 5 US stocks within that industry"},
            {"role": "user", "content": f"What industry is the article talking about, give 5 US stocks within the industry: {article}"}
        ],
        "temperature": 0.7,
        "max_tokens": -1,
        "stream": False
    }
    
    # Make the request
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    # Print the response
    print("Status:", response.status_code)
    response_data = response.json()
    print(response_data)
    
    # Extract just the model's response text if successful
    content = None
    if response.status_code == 200:
        if "choices" in response_data and len(response_data["choices"]) > 0:
            content = response_data["choices"][0]["message"]["content"]
            print("\nModel response:")
            print(content)
    
    return response_data, content
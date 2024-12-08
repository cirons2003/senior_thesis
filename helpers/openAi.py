import openai
from dotenv import load_dotenv
import os

#load_dotenv()

client = openai.OpenAI(api_key=os.getenv("OPEN_AI_KEY")); 

def generateDescription(attributes):
    context = "Based on the provided details about this person, generate a self-description paragraph in their voice. \
    Ensure the paragraph aligns logically with the input, \
    and avoids generic phrases. Focus on creating a concise but meaningful description that reflects the person's \
    Highlight their key interests, values, and defining traits."
    
    response = client.chat.completions.create(
        model='gpt-3.5-turbo',
        messages=[
            {"role": "system", "content": context},
            {"role": "user", "content": attributes}
        ],
        max_tokens=250
    )
    
    return response.choices[0].message.content.strip()




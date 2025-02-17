import openai
from dotenv import load_dotenv
import os

load_dotenv("/home/cirons2003/senior-thesis/senior_thesis/.env")


client = openai.OpenAI(api_key=os.getenv("OPEN_AI_KEY")); 
context = "Generate a self-description of this person. Stay concise and aim for each sentence to demonstrate a specific interest, value, or lifestyle detail of the person. Feel free to make things up but dont ramble."
print(context)

def generateDescription(attributes: str):

    response = client.chat.completions.create(
        model='gpt-4o-mini',
        messages=[
            {"role": "system", "content": context},
            {"role": "user", "content": attributes}
        ],
        max_tokens=250
    )
    
    return response.choices[0].message.content.strip()




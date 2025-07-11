import asyncio
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
from langchain_mcp_adapters.client import MultiServerMCPClient
from dotenv import load_dotenv
import os
import langchain

load_dotenv()

model = ChatOpenAI(model="gpt-4o", temperature=0)

async def run_query(user_query: str):
    print(f"▶ User Query: {user_query}")
    
    client = MultiServerMCPClient({
        "postgres": {
            "command": "python",
            "args": ["./servers/postgresql_server.py"],
            "transport": "stdio",
        }
    })
    
    agent_executor = create_react_agent(model, await client.get_tools())
    
    langchain.debug = True
    
    print("\nAgent is thinking...")
    result = await agent_executor.ainvoke({"messages": [("user", user_query)]})
    
    langchain.debug = False
    
    final_answer = result['messages'][-1].content
    print("\nFinal Answer:")
    print(final_answer)


if __name__ == "__main__":
    test_query = "Compare the customer satisfaction score (CSS) for the 'Sorento Signature' and 'Carnival Signature' trims."
    
    asyncio.run(run_query(test_query))
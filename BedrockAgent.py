import asyncio
import os
from dotenv import load_dotenv
from typing import Optional, Dict, Any
from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import InMemorySaver
from langchain_anthropic import ChatAnthropic
import boto3
import json

load_dotenv()
checkpointer = InMemorySaver()

class BedrockAgent:
    """
    LangGraph agent that connects to Bedrock MCP server for knowledge base retrieval
    """
    
    def __init__(self, server_script_path: str):
        """
        Initialize the agent with Anthropic LLM
        
        Args:
            server_script_path: Path to the BedrockMCPServer.py file
        """
        self.server_script_path = server_script_path
        
        # Initialize LLM - using Anthropic API
        self.llm = ChatAnthropic(
            model="claude-3-5-sonnet-20241022",  # or claude-3-opus-latest
            temperature=0,
            api_key=os.getenv("ANTHROPIC_API_KEY"),
        )
        
        # Initialize MCP client
        self.mcp_client = MultiServerMCPClient({
            "bedrockServer": {
                "command": "python",
                "args": [server_script_path],
                "transport": "stdio",
            }
        })
    
    async def setup_tools_and_prompt(self, system_prompt: str):
        """
        Set up the LangGraph agent with MCP tools from Bedrock server
        
        Args:
            system_prompt: System prompt for the agent
        """
        # Get tools from MCP server
        tools = await self.mcp_client.get_tools()
        
        print(f"Connected to MCP server with tools: {[tool.name for tool in tools]}")
        
        # Create LangGraph ReAct agent
        self.agent = create_react_agent(
            self.llm, 
            tools, 
            prompt=system_prompt, 
            checkpointer=checkpointer
        )
    
    async def chat(self, message: str, thread_id: str = "1") -> dict:
        """
        Send a message to the agent and get a response with streaming
        
        Args:
            message: User message
            thread_id: Conversation thread ID for memory
        """
        config = {"configurable": {"thread_id": thread_id}}
        
        try:
            print(f"🤖 Processing: {message}\n")
            
            # Stream events to show thinking
            final_response = None
            async for event in self.agent.astream_events(
                {"messages": [{"role": "user", "content": message}]},
                config,
                version="v2"
            ):
                if event["event"] == "on_llm_start":
                    print("🤔 Analyzing your question...")
                    
                elif event["event"] == "on_tool_start":
                    tool_name = event["name"]
                    print(f"🔍 Using tool: {tool_name}")
                    
                    if tool_name == "retrieve_documents":
                        print("   📚 Searching Bedrock Knowledge Base...")
                    elif tool_name == "get_knowledge_base_info":
                        print("   ℹ️ Getting knowledge base information...")
                        
                elif event["event"] == "on_tool_end":
                    print("   ✅ Analysis complete, synthesizing response...")
                    
                # Capture final response
                if event["event"] == "on_chain_end":
                    final_response = event.get("data", {}).get("output", {})
            
            print("\n")  # New line after streaming
            return final_response or {"messages": []}
            
        except Exception as e:
            print(f"❌ Error: {e}")
            raise
    
    def format_response(self, response: Dict[str, Any]) -> str:
        """Format the agent response for display"""
        final_message = response["messages"][-1]
        
        if hasattr(final_message, 'content'):
            content = final_message.content
            
            if isinstance(content, str):
                return content
            elif isinstance(content, list):
                text_parts = []
                for item in content:
                    if isinstance(item, dict) and 'text' in item:
                        text_parts.append(item['text'])
                    elif isinstance(item, str):
                        text_parts.append(item)
                return " ".join(text_parts)
            else:
                return str(content)
        else:
            return str(final_message)


async def main():
    """Interactive chat session with the agent"""
    
    # Initialize agent
    agent = BedrockAgent(
        server_script_path="./BedrockMCPServer.py"  # Update this path
    )
    
    # System prompt for the agent
    system_prompt = """You are an AI assistant with access to a Bedrock Knowledge Base containing various documents.

YOUR ROLE:
- Help users find and understand information from the knowledge base
- Use the retrieve_documents tool to search for relevant content
- Provide accurate, well-sourced answers based on the retrieved documents
- Synthesize information from multiple sources when appropriate

RESPONSE GUIDELINES:
- Always cite which documents your information comes from
- If you can't find relevant information in the knowledge base, say so clearly
- Be conversational but professional
- Focus on providing accurate information from the knowledge base
- When multiple documents contain relevant information, synthesize them coherently

When answering questions:
1. Use the retrieve_documents tool to find relevant information
2. Analyze the retrieved content carefully
3. Provide a clear, comprehensive answer based on the sources
4. Always mention which documents you're referencing"""
    
    await agent.setup_tools_and_prompt(system_prompt)
    
    print("🚀 Bedrock Knowledge Base Agent is ready!")
    print("Type 'quit' or 'exit' to end the conversation\n")
    
    while True:
        user_input = input("\nYou: ").strip()
        
        if user_input.lower() in ['quit', 'exit', 'q']:
            print("👋 Goodbye!")
            break
        elif not user_input:
            continue
        
        try:
            response = await agent.chat(user_input)
            formatted_response = agent.format_response(response)
            print(f"\nAgent: {formatted_response}")
        except Exception as e:
            print(f"\n❌ Error: {e}")
            print("Please try again or check your configuration.")


if __name__ == "__main__":
    asyncio.run(main())
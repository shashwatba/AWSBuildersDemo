import logging
import os
from mcp.server.fastmcp import FastMCP
from typing import Any, Dict, List, Optional
import boto3
from dotenv import load_dotenv
import json
import asyncio

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("bedrock_server_debug.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize FastMCP
mcp = FastMCP("BedrockKnowledgeBase")
load_dotenv()

class BedrockKnowledgeBaseRAG:
    """RAG system using AWS Bedrock Knowledge Base"""
    
    def __init__(self):
        self.bedrock_agent_runtime = None
        self.knowledge_base_id = os.getenv("BEDROCK_KNOWLEDGE_BASE_ID")
        self.aws_region = os.getenv("AWS_REGION", "us-east-1")
        self.initialized = False
        
    async def initialize(self):
        """Initialize the Bedrock client"""
        if self.initialized:
            return
            
        logger.info("🚀 Initializing Bedrock Knowledge Base RAG system...")
        
        try:
        # Initialize Bedrock Agent Runtime client
            self.bedrock_agent_runtime = boto3.client(
            'bedrock-agent-runtime',
            region_name=self.aws_region
        )
        
            if not self.knowledge_base_id:
                raise ValueError("BEDROCK_KNOWLEDGE_BASE_ID not set in environment variables")
        
            self.initialized = True
            logger.info(f"✅ Bedrock RAG system ready with Knowledge Base ID: {self.knowledge_base_id}")
        
            
            if not self.knowledge_base_id:
                raise ValueError("BEDROCK_KNOWLEDGE_BASE_ID not set in environment variables")
            
            self.initialized = True
            logger.info(f"✅ Bedrock RAG system ready with Knowledge Base ID: {self.knowledge_base_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Bedrock client: {e}")
            raise
    


    async def search(self, query: str, limit: int = 5):
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None,
            lambda: self.bedrock_agent_runtime.retrieve(
                knowledgeBaseId=self.knowledge_base_id,
                retrievalQuery={"text": query},
                retrievalConfiguration={
                    "vectorSearchConfiguration": {
                        "numberOfResults": limit,
                        "overrideSearchType": "HYBRID"
                    }
                }
            )
        )

        # Optional: print or log the response for debug
        print("Bedrock retrieve response:", response)

        results = []
        for result in response.get("retrievalResults", []):
            content = result.get("content", "")
            source = result.get("location", {}).get("s3Location", {}).get("uri", "")
            results.append({"content": content, "source": source})

        return results

            
        # except Exception as e:
        #     logger.error(f"Error searching Bedrock Knowledge Base: {e}")
        #     raise

# Initialize RAG system
rag_system = BedrockKnowledgeBaseRAG()

# MCP TOOLS

@mcp.tool()
async def retrieve_documents(query: str, limit: int = 3) -> Dict[str, Any]:
    """
    Search through documents in AWS Bedrock Knowledge Base.
    
    Args:
        query: Search query for finding relevant documents
        limit: Number of results to return (1-10, default 3)
    
    Returns:
        Dictionary with search results containing relevant document excerpts
    """
    try:
        # Ensure RAG system is initialized
        if not rag_system.initialized:
            await rag_system.initialize()
        
        # Perform search
        results = await rag_system.search(query, min(limit, 10))
        
        return {
            "query": query,
            "results": [
                {
                    "title": r['title'],
                    "content": r['text'][:800] + "..." if len(r['text']) > 800 else r['text'],
                    "source": r['source'],
                    "url": r['url'],
                    "relevance_score": round(r['similarity'] * 100, 1),
                    "metadata": r.get('metadata', {})
                }
                for r in results
            ],
            "total_results": len(results),
            "source": "AWS Bedrock Knowledge Base"
        }
    except Exception as e:
        logger.error(f"Error in retrieve_documents: {e}")
        return {
            "error": f"Search failed: {str(e)}",
            "query": query,
            "results": []
        }

@mcp.tool()
async def get_knowledge_base_info() -> Dict[str, Any]:
    """
    Get information about the connected Bedrock Knowledge Base.
    
    Returns:
        Dictionary with knowledge base configuration and status
    """
    try:
        if not rag_system.initialized:
            await rag_system.initialize()
        
        return {
            "knowledge_base_id": rag_system.knowledge_base_id,
            "aws_region": rag_system.aws_region,
            "status": "connected" if rag_system.initialized else "disconnected",
            "service": "AWS Bedrock Knowledge Base"
        }
    except Exception as e:
        logger.error(f"Error getting knowledge base info: {e}")
        return {
            "error": str(e),
            "status": "error"
        }

# Initialize on startup
async def startup():
    """Initialize RAG system when server starts"""
    try:
        await rag_system.initialize()
        logger.info("🎯 Bedrock Knowledge Base MCP Server ready!")
    except Exception as e:
        logger.error(f"Startup error: {e}")

if __name__ == "__main__":
    # Run the server
    mcp.run(transport='stdio')
    
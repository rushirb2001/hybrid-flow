#!/usr/bin/env python3
"""Verification script to check NEXT/PREV relationships in Neo4j."""

import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment variables
load_dotenv()

# Connect to Neo4j
neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
neo4j_user = os.getenv("NEO4J_USER", "neo4j")
neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

# Check total NEXT relationships
with driver.session() as session:
    result = session.run("MATCH ()-[r:NEXT]->() RETURN count(r) as total")
    next_count = result.single()["total"]
    print(f"Total NEXT relationships: {next_count}")

    result = session.run("MATCH ()-[r:PREV]->() RETURN count(r) as total")
    prev_count = result.single()["total"]
    print(f"Total PREV relationships: {prev_count}")

    # Check a specific example
    result = session.run("""
        MATCH (p1:Paragraph)-[:NEXT]->(p2:Paragraph)
        WHERE p1.chunk_id STARTS WITH 'bailey:ch01'
        RETURN p1.chunk_id as para1, p1.number as num1,
               p2.chunk_id as para2, p2.number as num2
        ORDER BY num1
        LIMIT 5
    """)

    print("\nExample sequential links in bailey:ch01:")
    for record in result:
        print(f"  {record['para1']} (#{record['num1']}) -> {record['para2']} (#{record['num2']})")

    # Verify bidirectionality
    result = session.run("""
        MATCH (p1:Paragraph)-[:NEXT]->(p2:Paragraph)
        WHERE NOT (p2)-[:PREV]->(p1)
        RETURN count(*) as broken_links
    """)
    broken = result.single()["broken_links"]
    print(f"\nBroken bidirectional links: {broken}")

    if broken == 0:
        print("✓ All NEXT/PREV relationships are properly bidirectional!")
    else:
        print("✗ Some relationships are missing their reverse link")

driver.close()

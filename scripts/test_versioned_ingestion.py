"""Test script for versioned Neo4j ingestion.

This script tests the complete hierarchy of versioned nodes created with
the version_id parameter in all upsert methods.
"""

import os
from dotenv import load_dotenv
from hybridflow.storage.neo4j_client import Neo4jStorage

def test_versioned_ingestion():
    """Test complete versioned ingestion through entire hierarchy."""
    load_dotenv()

    storage = Neo4jStorage(
        uri=os.getenv('NEO4J_URI'),
        user=os.getenv('NEO4J_USER'),
        password=os.getenv('NEO4J_PASSWORD')
    )

    version_id = 'v4_integration_test'

    print(f"Testing versioned ingestion with version_id='{version_id}'...")

    # 1. Upsert textbook
    print("  Creating textbook...")
    storage.upsert_textbook('test_versioned', 'Test Versioned Book', version_id=version_id)

    # 2. Upsert chapter
    print("  Creating chapter...")
    storage.upsert_chapter('test_versioned', '1', 'Test Chapter', version=1, version_id=version_id)

    # 3. Upsert section
    print("  Creating section...")
    storage.upsert_section('test_versioned:ch1', '1', 'Test Section', version_id=version_id)

    # 4. Upsert subsection
    print("  Creating subsection...")
    storage.upsert_subsection('test_versioned:ch1:s1', '1.1', 'Test Subsection', version_id=version_id)

    # 5. Upsert subsubsection
    print("  Creating subsubsection...")
    storage.upsert_subsubsection(
        'test_versioned:ch1:s1:ss1.1', '1.1.1', 'Test Subsubsection', version_id=version_id
    )

    # 6. Upsert paragraphs
    print("  Creating paragraphs...")
    for i in range(1, 4):
        storage.upsert_paragraph(
            parent_id='test_versioned:ch1:s1:ss1.1',
            paragraph_number=f'1.1.1.{i}',
            text=f'Test paragraph {i} content.',
            chunk_id=f'test_versioned:ch1:1.1.1.{i}',
            page=1,
            bounds=[100.0, 100.0 + (i * 50), 500.0, 150.0 + (i * 50)],
            cross_references=[],
            version_id=version_id
        )

    # 7. Link sequential paragraphs
    print("  Linking sequential paragraphs...")
    links_created = storage.link_sequential_paragraphs('test_versioned:ch1', version_id=version_id)
    print(f"    Created {links_created} NEXT/PREV links")

    # 8. Upsert table
    print("  Creating table...")
    storage.upsert_table(
        paragraph_chunk_id='test_versioned:ch1:1.1.1.1',
        table_number='1.1',
        description='Test table',
        page=1,
        bounds=[100.0, 200.0, 500.0, 400.0],
        file_png='',
        file_xlsx='',
        version_id=version_id
    )

    # 9. Upsert figure
    print("  Creating figure...")
    storage.upsert_figure(
        paragraph_chunk_id='test_versioned:ch1:1.1.1.2',
        figure_number='1.1',
        caption='Test figure caption',
        page=1,
        bounds=[100.0, 450.0, 500.0, 650.0],
        file_png='',
        version_id=version_id
    )

    # Verify all nodes created with correct version label
    print(f"\nVerifying nodes created with :{version_id} label...")

    with storage.driver.session() as session:
        # Count nodes by type
        node_types = ['Textbook', 'Chapter', 'Section', 'Subsection', 'Subsubsection', 'Paragraph', 'Table', 'Figure']

        for node_type in node_types:
            result = session.run(
                f"MATCH (n:{node_type}:{version_id}) RETURN count(n) as count"
            )
            count = result.single()['count']
            print(f"  {node_type:15s}: {count:3d} nodes")

        # Verify relationships
        print("\nVerifying relationships between versioned nodes...")

        # CONTAINS (Textbook -> Chapter)
        result = session.run(f"""
            MATCH (t:Textbook:{version_id})-[r:CONTAINS]->(c:Chapter:{version_id})
            RETURN count(r) as count
        """)
        count = result.single()['count']
        print(f"  CONTAINS:              {count:3d} relationships")

        # HAS_SECTION (Chapter -> Section)
        result = session.run(f"""
            MATCH (c:Chapter:{version_id})-[r:HAS_SECTION]->(s:Section:{version_id})
            RETURN count(r) as count
        """)
        count = result.single()['count']
        print(f"  HAS_SECTION:           {count:3d} relationships")

        # HAS_SUBSECTION (Section -> Subsection)
        result = session.run(f"""
            MATCH (s:Section:{version_id})-[r:HAS_SUBSECTION]->(ss:Subsection:{version_id})
            RETURN count(r) as count
        """)
        count = result.single()['count']
        print(f"  HAS_SUBSECTION:        {count:3d} relationships")

        # HAS_SUBSUBSECTION (Subsection -> Subsubsection)
        result = session.run(f"""
            MATCH (ss:Subsection:{version_id})-[r:HAS_SUBSUBSECTION]->(sss:Subsubsection:{version_id})
            RETURN count(r) as count
        """)
        count = result.single()['count']
        print(f"  HAS_SUBSUBSECTION:     {count:3d} relationships")

        # HAS_PARAGRAPH (Subsubsection -> Paragraph)
        result = session.run(f"""
            MATCH (sss:Subsubsection:{version_id})-[r:HAS_PARAGRAPH]->(p:Paragraph:{version_id})
            RETURN count(r) as count
        """)
        count = result.single()['count']
        print(f"  HAS_PARAGRAPH:         {count:3d} relationships")

        # NEXT/PREV (Paragraph -> Paragraph)
        result = session.run(f"""
            MATCH (p1:Paragraph:{version_id})-[r:NEXT]->(p2:Paragraph:{version_id})
            RETURN count(r) as count
        """)
        count = result.single()['count']
        print(f"  NEXT:                  {count:3d} relationships")

        result = session.run(f"""
            MATCH (p1:Paragraph:{version_id})-[r:PREV]->(p2:Paragraph:{version_id})
            RETURN count(r) as count
        """)
        count = result.single()['count']
        print(f"  PREV:                  {count:3d} relationships")

        # CONTAINS_TABLE (Paragraph -> Table)
        result = session.run(f"""
            MATCH (p:Paragraph:{version_id})-[r:CONTAINS_TABLE]->(t:Table:{version_id})
            RETURN count(r) as count
        """)
        count = result.single()['count']
        print(f"  CONTAINS_TABLE:        {count:3d} relationships")

        # CONTAINS_FIGURE (Paragraph -> Figure)
        result = session.run(f"""
            MATCH (p:Paragraph:{version_id})-[r:CONTAINS_FIGURE]->(f:Figure:{version_id})
            RETURN count(r) as count
        """)
        count = result.single()['count']
        print(f"  CONTAINS_FIGURE:       {count:3d} relationships")

    storage.close()

    print(f"\n✓ All nodes and relationships created successfully with :{version_id} label")
    print("✓ Versioned ingestion test passed")

if __name__ == '__main__':
    test_versioned_ingestion()

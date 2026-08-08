# Transform Job Summary

## Job Flow

### Essentially...

```
Embeddings are derived artifacts, not part of the canonical data.
Embeddings should be considered a serving layer.
```
```
Bronze
    ↓
Silver: normalized. goes into DW or LH
    ↓
Gold (optional)
    ↓ : make a semantic document 
        → Elasticsearch document index
        → graph nodes and edges
        → daily/weekly analytical aggregates
        → embeddings
        // as els document like {id: xxx, text_embedings:...}
Indexing pipeline: meta data filter + bm25 + vector?
    ├── build search documents
    ├── generate embeddings
    ├── create keyword fields
    ├── create metadata
    └── push into Elasticsearch/OpenSearch
```

- Searchable object types: Event, Story, Entity

### TF method

**Transformation order**

1. Parse and deduplicate GKG into canonical documents.
2. Explode GKG repeated values into annotation occurrences.
3. Deduplicate Events by GlobalEventID.
4. Normalize EventMentions at their original mention grain.
5. Resolve each Mention to its canonical document.
6. Attach the event foreign key.
7. Record unmatched-document and unmatched-event quality flags.

Crucially, never explode themes, people, organizations and locations and then join them directly to Mentions. **That would produce a cartesian multiplication**!! 

such as: `mention × themes × people × organizations × locations`

Instead, Mentions and annotations independently reference the same document_id.

### How downstream products consume it

####  Search mart/index

Build one search document per silver_document, denormalizing:

- Document metadata
- Distinct themes, people, organizations and locations
- Connected event classifications
- Source and publication date
- Mention/event confidence summaries

Store location relation explicitly:
- document_mentions_location
- event_action_location
- actor1_location
- actor2_location

That prevents “Kosovo was mentioned somewhere in the article” from being confused with “the event occurred in Kosovo.”

#### Mentions-grain mart
Start from silver_event_mention, then join one-to-one/many-to-one with:
- silver_event
- silver_document

Keep document annotations as arrays, existence flags or separate bridges. Do not physically repeat every annotation occurrence on every mention unless that particular mart explicitly needs mention-by-annotation grain.

#### GraphDB projection

The silver model maps directly to:

```
(Document)-[:MENTIONS_EVENT]->(Event)
(Document)-[:MENTIONS_PERSON]->(Person)
(Document)-[:MENTIONS_ORGANIZATION]->(Organization)
(Document)-[:MENTIONS_LOCATION]->(Location)
(Document)-[:HAS_THEME]->(Theme)
(Event)-[:OCCURRED_AT]->(Location)
(Event)-[:INVOLVES_ACTOR]->(Actor)
```

Offsets, occurrence counts and confidence become edge properties or edge weights.

#### Important limitation

GKG does not contain the original article body; it contains metadata computed from the document. Therefore, using only Events, Mentions and GKG, kw= is structured metadata search—not arbitrary historical full-text search. You can search GKG themes, names, organizations, locations, title metadata and event classifications, but a truly arbitrary keyword search requires separately obtaining/indexing article text or another text-oriented GDELT stream.

### Summary

Preserve Events and Documents as independent entities, make EventMentions the event-to-document bridge, explode GKG into document-level annotation occurrences, and only denormalize them when producing the search, mention-analysis and graph marts.

## Dev

### Structure

example structure for adding tf tasks:

src/main/java/com/dyrmgraph/transform/
├── Tf1.java
├── Tf2.java
├── Tf3.java
└── Tf4.java

All built into one jar

### Execution

KPO launches a container pod with command:

spark-submit \
  --class com.dyrmgraph.transform.{GDELTTables,GDELTLookups} \
  /opt/transform/transform.jar \
  s3a://my-bucket/input \
  s3a://my-bucket/output

### Ops

- Write .java code
- Test and build .jar
- Build dockerized spark app in which the .jar is baked
- container integration test
- scratch & build -> artifact registry (release)
- used by KPO

### Notes

- `mvn test`: run test
- `mvn clean verify` run test & build
- Dockerfile: build it with -f specified from the proj root context
- Configure KPO from the Airflow TF dag accordingly

MATCH ()-[r]->() WHERE NOT r:hasActor|ns0__hasActor|hasPlace|ns0__hasPlace|sameAs|owl__sameAs DETACH DELETE r;

MATCH (o) WHERE o:Resource AND NOT o:Event|Person|Location|Organisation|ContextualEvent|SourceEvent|NonEntity|Article|NewsSource AND NOT o.uri STARTS WITH 'http://dbpedia.org/resource/' AND NOT any(label IN labels(o) WHERE label STARTS WITH "ns0__" OR label STARTS WITH "ns1__") DETACH DELETE o;

MATCH (n)
WHERE any(label IN labels(n) WHERE label STARTS WITH 'ns3__')
WITH n, [label IN labels(n) WHERE label STARTS WITH 'ns3__' | substring(label, 5)] AS event_categories
SET n.event_categories = event_categories
RETURN n LIMIT 10;

CALL db.labels()
YIELD label WHERE (NOT label STARTS WITH 'ns1__' AND NOT label STARTS WITH 'ns0__') AND label CONTAINS "__"
WITH collect(label) AS labels
MATCH (n)
WHERE any(label IN labels(n) WHERE NOT label in labels) OR n:Resource
WITH collect(n) AS _nodes, labels
CALL apoc.create.removeLabels(_nodes, labels)
YIELD node
RETURN node, labels(node) AS labels
LIMIT 10;

CALL apoc.refactor.rename.label('ns0__Event', 'Event');
CALL apoc.refactor.rename.label('ns1__PERSON', 'Person');
CALL apoc.refactor.rename.label('ns1__LOCATION', 'Location');
CALL apoc.refactor.rename.label('ns1__ORGANIZATION', 'Organisation');
CALL apoc.refactor.rename.label('ns1__contextualEvent', 'ContextualEvent');
CALL apoc.refactor.rename.label('ns1__sourceEvent', 'SourceEvent');
CALL apoc.refactor.rename.label('ns1__NONENTITY', 'NonEntity');
CALL apoc.refactor.rename.type('ns0__hasActor', 'hasActor');
CALL apoc.refactor.rename.type('ns0__hasPlace', 'hasPlace');
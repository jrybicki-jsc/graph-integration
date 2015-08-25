b2share graph model
===================
This EUDAT subproject deals with data objects stored in EUDAT B2SHARE service.
The graph model used to store information about objects from B2SHARE is:

```
Person-:CREATED->DataObject-:DESCRIBED_BY->MetaData
DataObject-:HAS_TAG->Tag
```

### Queries example

Most popular keywords:
```
MATCH (k:Keyword)<-[:HAS_TAG]-(o)
RETURN k, count(o) as popularity
ORDER BY popularity DESC
```

Most "productive" authors:
```
MATCH (p:Person)-[]->(do:Data)
RETURN p.email AS author, count(do) AS objects
ORDER BY objects DESC
```

People working on data objects with same tags:
```
MATCH (p1:Person)-[]->(do:Data)-[]->(k:Keyword),
      (p2:Person)-[]->(do2:Data)-[]->(k)
WHERE p1.email<>p2.email
return p1, do, do2, k, p2 limit 90
```

Close friends of Peter:
```
MATCH (Peter:Person {name: 'peter.wittenburg@mpi.nl'}), (b:Person),
      p=shortestPath((Peter)-[*..5]-(b))
RETURN p
```
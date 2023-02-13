# CBRS

## launch database

```bash
docker-compose up -d
```

## launch server

This app need java 17 and maven installed

```bash
cd app_java/myapp
mvn clean compile exec:java
```

## Connect to neo4j browser

Go on http://0.0.0.0:7474/browser/ and select without authentification

## USeful queries

### Count all nodes

```cypher
MATCH (n) RETURN count(n)
```

### Count all genres

```cypher
MATCH (n:Genre) RETURN count(n)
```

### Get movie with id 1

```cypher
MATCH (n:Movie) WHERE n.id = 1 RETURN n
```

### Make an export with apoc in graphml

```cypher
CALL apoc.export.graphml.all("hello", {})
```
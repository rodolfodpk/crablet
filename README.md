# Crablet

So far, just an experiment

## Running

1. On terminal, run:
    ```bash
        docker-compose up 
    ``` 

2. Run the main method of  [`CrabletTest1.kt`](src/test/kotlin/crablet/example/CrabletTest1.kt)

 ```text
Vertx Pool started
Append operation
--> eventsToAppend: [{"type":"AccountOpened","id":10}, {"type":"AmountDeposited","amount":100}]
--> appendCondition: AppendCondition(query=StreamQuery(identifiers=[DomainIdentifier(name=StateName(value=Account), id=StateId(value=51834a25-31b4-461d-8cfe-cc75a842bbcb))], eventTypes=[EventName(value=AccountOpened), EventName(value=AmountDeposited)]), maximumEventSequence=SequenceNumber(value=0)) 

New sequence id ---> SequenceNumber(value=12)
New state ---> [{"type":"AccountOpened","id":10},{"type":"AmountDeposited","amount":100}]
 ```

## References

* https://www.youtube.com/watch?v=GzrZworHpIk
* https://www.youtube.com/watch?v=DhhxKoOpJe0
* https://github.com/crabzilla/crabzilla
* https://github.com/imrafaelmerino/json-values
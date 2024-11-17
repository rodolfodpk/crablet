# Crablet

So far, just an experiment

## Running

1. On terminal, run:
    ```bash
        docker-compose up 
    ``` 

2. Run the main method of  [`CrabletTest1.kt`](./src/main/kotlin/CrabletTest1.kt)

 ```bash
 Vertx Pool started
 SequenceNumber(value=12)
 Future{unresolved}
 Event: 11  {
 "type" : "AccountOpened",
 "id" : 10
 } = [{"type":"AccountOpened","id":10}]
 Event: 12  {
 "type" : "AmountDeposited",
 "amount" : 10
 } = [{"type":"AccountOpened","id":10},{"type":"AmountDeposited","amount":10}]
 End of stream    
 ```

## References

* https://www.youtube.com/watch?v=GzrZworHpIk
* https://www.youtube.com/watch?v=DhhxKoOpJe0
* https://github.com/crabzilla/crabzilla
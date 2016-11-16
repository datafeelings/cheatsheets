### Select all and return
`match(n) return (n)`

### Create a single node 
...with the label "Flight", and attributes number, airline, capacity  
`create(n:Flight {number: "12", airline: 'Delta', capacity: "160"})`

### Create a single edge
... with the label "Arrives" for such nodes a1 and b1 that a1 has a label Flight and number 23, and b1 has the label "Airport" and attribute 'DTW', and the edge is directed from a to b  
`match (a1: Flight {number: 23}),(b1:Airport {label: 'DTW'}) create (a1) -[r1:Arrives]-> (b1)`

### DELETE ALL, EVERYTHING
`MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r`

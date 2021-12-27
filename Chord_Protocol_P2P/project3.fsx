#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit"
#r "nuget: Akka.Remote" 
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic
open System.Security.Cryptography
open System.Text

let system = ActorSystem.Create("FSharpChord")
let actorRef name =
 select ("akka://" + system.Name + "/user/"  + name) system

let randomNumbers count =
    let rnd = System.Random()
    List.init count (fun _ -> rnd.Next ())

let calculateHash (inputString: string) =
    Encoding.UTF8.GetBytes(inputString)
    |> (new SHA256Managed()).ComputeHash

type ParentMessage = 
    | SendObject of (string)
    | Initialize of (int)
    | TableAppend of (string)
    | HopsTaken of (int)
    | Parameters of (int)
    | SendFObject of (string)
    | RetrieveObject of (string * int)
    | RetrieveFObject of (int)
    | Finish of (string)
    | SendObjectCompleted of (string)
    | TableAppendCompleted of (string)

let numberOfNodes = fsi.CommandLineArgs.[1] |> int
let numberOfRequests = fsi.CommandLineArgs.[2] |> int
let mutable startNode = ""
let mutable totalTableOps = 0
let mutable tableOpsCompleted = 0
let nodePrefix = "Node"
let mutable numNodes = numberOfNodes
let mutable requests = numNodes * numberOfRequests
let mutable hops = 0
let mutable totalDataObjects = int (numberOfNodes/2)
let mutable requestsCompleted = 0
let mutable nodeQueries = numberOfRequests
let nodeRef = new List<string>()
let objPrefix = "racris" 
let mutable numOfObjectsGiven = 0

let childActor (mailbox:Actor<_>) =

    let nodeObjects = new List<string>()
    let nodeTable = new List<string>()
    let rec loop () = actor {

        let! message = mailbox.Receive()
        let nodeName = mailbox.Self.Path.Name

        match message with

        | Initialize(requests) ->

            let randreq = randomNumbers requests

            for i in randreq do

                let r = (i + 43) % (totalDataObjects + 10)
                let findObject = "Object" + r.ToString()
                actorRef nodeRef.[0] <! RetrieveObject(findObject, 0)

        | SendFObject(object) ->

            nodeObjects.Add(object)
            actorRef "parent" <! SendObjectCompleted("")

        | RetrieveFObject(currentCount) ->
        
            actorRef "parent" <! HopsTaken(currentCount+1)
            actorRef "parent" <! Finish("")
            
        | SendObject(object) -> 
      
            let hashResult = calculateHash object
            let mutable stringHash = ""

            for i in hashResult do 
                stringHash <- stringHash + i.ToString("x2") 

            if nodeName > stringHash then 
                actorRef nodeName <! SendFObject(object)
            
            else
                let mutable check = true 

                if nodeTable.Count = 0 then 
                    actorRef nodeRef.[0] <! SendFObject(object)

                else 
                    let mutable index = nodeTable.Count - 1 
                    let mutable passed = false 

                    while check && index >= 0 do

                        let indexOfNode = nodeTable.[index]

                        if indexOfNode < stringHash then 
                            actorRef indexOfNode <! SendObject(object)
                            check <- false 
                            passed <- true 
                        
                        else 
                            index <- index - 1 

                    if passed = false then 
                        actorRef nodeTable.[0] <! SendFObject(object)

        | RetrieveObject(object, currentCount) ->

            let hashResult = calculateHash object
            let mutable stringHash = ""

            for i in hashResult do 
                stringHash <- stringHash + i.ToString("x2") 

            if nodeName > stringHash then 
                actorRef nodeName <! RetrieveFObject(currentCount+1)
            
            else
                let mutable check = true 

                if nodeTable.Count = 0 then 
                    actorRef nodeRef.[0] <! RetrieveFObject(currentCount)

                else 
                    let mutable index = nodeTable.Count - 1 
                    let mutable passed = false 

                    while check && index >= 0 do

                        let indexOfNode = nodeTable.[index]

                        if indexOfNode < stringHash then 
                            actorRef indexOfNode <! RetrieveObject(object, currentCount + 1)
                            check <- false 
                            passed <- true 
                        
                        else 
                            index <- index - 1 

                    if passed = false then 
                        actorRef nodeTable.[0] <! RetrieveFObject(currentCount + 1)

        
        | TableAppend(nodeName) ->
           
            nodeTable.Add(nodeName)
            actorRef "parent" <! TableAppendCompleted("")

        | _ -> ()

        return! loop()
    }
    loop()
             
let parentActor (mailbox:Actor<_>) =
 
    let rec loop () = actor {
        
        let mutable numNde = 0
        let! message = mailbox.Receive()
        match message with 

        | HopsTaken(count) ->
            hops <- hops + count 
        
        | Parameters(numberOfNodes) -> 
            
            numNde <- numberOfNodes

            for i in 1 .. numberOfNodes do
                
                let nodename = nodePrefix + i.ToString()
                let hashResult = calculateHash nodename
                let mutable stringHash = ""

                for j in hashResult do 
                    stringHash <- stringHash + j.ToString("x2")  

                spawn system stringHash childActor |> ignore
                nodeRef.Add(stringHash)
                
            nodeRef.Sort()
            startNode <- nodeRef.[0]

            for i in 0 .. nodeRef.Count-1 do 

                let mutable j = 1 
                
                while (i+j) < nodeRef.Count do 
                    
                    actorRef nodeRef.[i] <! TableAppend(nodeRef.[i+j])
                    j <- j * 2 

            for i in 0 .. nodeRef.Count-1 do 

                let mutable j = 1
                
                while (i+j) < nodeRef.Count do 
                    
                    totalTableOps <- totalTableOps + 1 
                    j <- j * 2 
      
        | SendObjectCompleted(n) ->

            numOfObjectsGiven <- numOfObjectsGiven + 1 

            if numOfObjectsGiven = totalDataObjects then 

                for item in nodeRef do 
                    actorRef item <! Initialize(nodeQueries)

        | TableAppendCompleted(n) ->

            tableOpsCompleted <- tableOpsCompleted + 1 

            if tableOpsCompleted = totalTableOps then
                            
                for i in 1 .. totalDataObjects do  
                    let objPrefix = "Object"
                    let object = objPrefix + i.ToString()
                  
                    let hashResult = calculateHash object
                    let mutable hashString = ""

                    for x in hashResult do 
                        hashString <- hashString + x.ToString("x2")  

                    actorRef nodeRef.[0] <! SendObject(object)    

        | Finish(finished) ->

            requestsCompleted <- requestsCompleted + 1 

            if requestsCompleted = requests then 
                mailbox.Context.System.Terminate() |> ignore
                
        | _ -> ()
       
        return! loop()
    }
    loop()

let parent = spawn system "parent" parentActor

parent <! Parameters(numberOfNodes)
system.WhenTerminated.Wait()

printfn $"Total hops : {hops}"
printfn $"Total query requests : {numNodes * numberOfRequests}"
let mutable hopsPerQuery = (hops |> float) / (numNodes * numberOfRequests |> float)
printfn $"Average Hops per query : {hopsPerQuery}"
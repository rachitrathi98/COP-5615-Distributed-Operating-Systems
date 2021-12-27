open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            stdout-loglevel : DEBUG
            loglevel : ERROR
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }")
 
let system = ActorSystem.Create("nodeConfig", configuration)
let nodeName = "Node"
let stopWatch = Diagnostics.Stopwatch()


let maxRumour = 10              //Maximum times rumour can be heard by an actor
let periodicTimer = 50  
let ratioDiff = 0.0000000001    // Stopping condition for termination is 10^-10
let mutable rounds = 3          //Rounds Actor has to undergi before the stopping condition

type SupervisorMsg = 
    | Start of int
    | PushSumEnd of double[]
    | Insert of int
    | Delete of int
    | PushSumStart of int
    | QueueSender of bool
    | PushQueueSender of bool

type NodeMsg =
    | Rumor of int
    | Sending of int
    | Push of double[]
    | PushSender of bool

let NodeActorRef name =
    select ("akka://" + system.Name + "/user/" + nodeName + name) system

let ActorRef name =
    select ("akka://" + system.Name + "/user/"  + name) system

//Creates an array for the respective neighbors
let neighborArray (arr:string array) nodeIndex =
    for i in 0 .. arr.Length-1 do
            if (i+1) = nodeIndex then
                Array.set arr i ""
            else
                Array.set arr i ((i+1).ToString())

//For 3D Grid and Imperfect 3D Grid six neighbors are taken based on the nearest perfect root of order of 3
let Neighbor3DArray (arr:string array) nodeIndex (imperfect3D:bool)=
    let cuberoot (f:float<'m^3>) : float<'m> = Math.Pow(float f, 1.0/3.0) |> LanguagePrimitives.FloatWithMeasure
    let k = cuberoot(arr.Length |> double) |> int
    let level = nodeIndex / (k * k)
    let upperLimit = (level + 1) * k * k
    let lowerLimit = level * k * k

    if (nodeIndex - k) >= lowerLimit then
        Array.set arr 0 ((nodeIndex - k).ToString())

    if (nodeIndex + k) < upperLimit then
        Array.set arr 1 ((nodeIndex + k).ToString())

    if (((nodeIndex - 1) % k) <> (k - 1)) && (nodeIndex - 1) >= 0 then
        Array.set arr 2 ((nodeIndex - 1).ToString())

    if ((nodeIndex + 1) % k) <> 0 then
        Array.set arr 3 ((nodeIndex + 1).ToString())

    if nodeIndex + (k * k) < arr.Length then
        Array.set arr 4 ((nodeIndex + (k * k)).ToString())

    if nodeIndex - (k * k) >= 0 then
        Array.set arr 5 ((nodeIndex - (k * k)).ToString())
   
    if imperfect3D then                                         //Change in condition for imperfect 3D where one random other neighbor is selected form the list of actors
        let rnd = Random()
        let mutable rndnodeIdx = Random().Next(arr.Length)+1
        while Array.Exists(arr, fun element -> element = rndnodeIdx.ToString() ) do
            rndnodeIdx <- rnd.Next(arr.Length)+1
        Array.set arr 6 (rndnodeIdx.ToString())

//For Line topoly the follwing neighbor array is created
let lineNeighborArray (arr:string array) nodeIndex =
    if (nodeIndex-1) > 0 then
        Array.set arr 0 ((nodeIndex-1).ToString())
    if (nodeIndex+1) <= arr.Length then
        Array.set arr 1 ((nodeIndex+1).ToString())

//Main Supervisor logic
let supervisor = 
    spawn system "supervisor"
        <| fun supervisorMailbox ->
            let mutable totalNumNodes = 0
            let mutable propCount = 0
            let mutable nodeSet = Set.empty 
            let stopWatch = System.Diagnostics.Stopwatch()
            let mutable orgNodeSet = Set.empty

            let rec supervisorLoop () =
                actor {
                    let! (msg: SupervisorMsg) = supervisorMailbox.Receive()
                    match msg with
                    | Start msg ->
                        totalNumNodes <- msg
                        for i in 1 .. totalNumNodes do
                            orgNodeSet <- orgNodeSet.Add(i)

                        stopWatch.Start()
                        let maxRcvTimes = maxRumour
                        NodeActorRef "1" <! Rumor maxRcvTimes

                    | PushSumEnd msg ->   
                        propCount <- propCount + 1
                        if propCount = totalNumNodes then
                            printfn ""
                            stopWatch.Stop()                        
                            printfn " Time taken: %u, Ratio:%.10f" stopWatch.ElapsedMilliseconds msg.[0]
                            Environment.Exit 1

                    | Insert msg ->
                        nodeSet <- nodeSet.Add(msg)
                        propCount <- propCount + 1
                        if propCount = totalNumNodes then
                            printfn ""
                            stopWatch.Stop()
                            printfn " Time taken: %u" stopWatch.ElapsedMilliseconds
                            Environment.Exit 1
                            
                    | Delete msg ->
                        nodeSet <- nodeSet.Remove(msg)

                    | QueueSender msg ->         
                        let rnd = Random()
                     
                        if msg then
                            if not nodeSet.IsEmpty then
                                for nodeIndex in nodeSet do
                                    NodeActorRef (nodeIndex.ToString()) <! Sending maxRumour

                    | PushSumStart msg ->                        
                        totalNumNodes <- msg
                        for i in 1 .. totalNumNodes do
                            orgNodeSet <- orgNodeSet.Add(i)
                        stopWatch.Start()
                        
                        for i in 1 .. totalNumNodes do
                            let nodename = i.ToString()
                            NodeActorRef nodename <! Push [|i|>double; 1.0|]

                    | PushQueueSender msg ->

                        for nodeIndex in 1 .. totalNumNodes do
                            let nodename = nodeIndex.ToString()
                            NodeActorRef nodename <! PushSender true

                    return! supervisorLoop ()
                }
            supervisorLoop ()


let nodes counter topology numNodes (nSet:Set<string>) (nodeMailbox:Actor<NodeMsg>) =
    let nodeName = nodeMailbox.Self.Path.Name
    let nodeIndex = nodeName.Substring(4) |> int
    
    // variables for push-sum
    let mutable (s:double) = 0.0
    let mutable (w:double) = 0.0
    let mutable lastRatio:double = 10000.0
    let mutable terminationCounter = 0
    let mutable counter = 0
    
    let rec loop  () = actor {
        let! (msg: NodeMsg) = nodeMailbox.Receive()
        match msg with
        | Rumor msg->
            if counter >= 0 then
                let maxCount = msg
                if counter < maxCount && counter >= 0 then
                    if counter = 0 then
                        // register as "active" node
                        ActorRef "supervisor" <! Insert nodeIndex
                    counter <- counter + 1
                else if counter = maxCount then
                    // unregister, not active now             
                    ActorRef "supervisor" <! Delete nodeIndex
                    counter <- -1
                else
                    printfn "\n[%s] Something wrong, counter: %d" nodeName counter

        | Sending msg ->                                //Send a rumour to a neighbor
            
            let maxCount = msg
            let rnd = Random()
            let tmp = (nSet |> Array.ofSeq)
            let name = tmp.[rnd.Next(tmp.Length)]
            if counter > 0 && counter < maxCount then
                NodeActorRef name <! Rumor maxCount

        
        | Push msg ->                                   // Adds the recieved values of s and w before sending to next
            if terminationCounter <> rounds then

                let rcvS = msg.[0]
                let rcvW = msg.[1]
                s <- s + rcvS
                w <- w + rcvW

        | PushSender msg ->
            if terminationCounter <> rounds then

                let ratio = s / w

                if (abs (ratio-lastRatio)) <= ratioDiff then      //Checks for ratio convergence
                    terminationCounter <- terminationCounter + 1

                    if terminationCounter = rounds then
                        ActorRef "supervisor" <! PushSumEnd [|ratio; s; w|]

                    else
                        lastRatio <- ratio

                        let rnd = Random()
                        let tmp = (nSet |> Array.ofSeq)
                        let name = tmp.[rnd.Next(tmp.Length)]
                        NodeActorRef name <! Push [|(0.5*s); (0.5*w)|]    //Send half of the values to one of the neighbor
                        nodeMailbox.Self.Tell (Push [|0.5*s; 0.5*w;|])

                        s <- 0.0
                        w <- 0.0
                else
                    terminationCounter <- 0
                    lastRatio <- ratio

                    let rnd = Random()
                    let tmp = (nSet |> Array.ofSeq)
                    let name = tmp.[rnd.Next(tmp.Length)]
                    NodeActorRef name <! Push [|(0.5*s); (0.5*w)|]
                    nodeMailbox.Self.Tell (Push [|0.5*s; 0.5*w;|])

                    s <- 0.0
                    w <- 0.0
            
        return! loop ()
    }
    loop ()

//Function used to spawn nodes using varied topologies
let nodeSpawn numNodes topology = 
    let cube x = x * x * x
    let cuberoot (f:float<'m^3>) : float<'m> = System.Math.Pow(float f, 1.0/3.0) |> LanguagePrimitives.FloatWithMeasure
    let mutable newNumNodes = numNodes
    if topology = "3D" || topology = "imp3D" then
        newNumNodes <- cube ((ceil(cuberoot(numNodes |> double))) |> int)

    for i in 1 .. newNumNodes do
        let nodename = nodeName + i.ToString()
        let newNodeArray = Array.create newNumNodes ""    
        match topology with
            | "full" ->
                neighborArray newNodeArray i 
            | "3D" ->
                let imperfect3D = false
                Neighbor3DArray newNodeArray i imperfect3D
            | "imp3D" ->
                let imperfect3D = true
                Neighbor3DArray newNodeArray i imperfect3D
            | "line" ->
                lineNeighborArray newNodeArray i
            | _ ->
                printfn "\n Wrong topology input!"
                Environment.Exit 1

        let nSet = (Array.filter ((<>) "") newNodeArray) |> Set.ofArray
        spawn system nodename (nodes 0 topology newNumNodes nSet) |> ignore
    newNumNodes

[<EntryPoint>]  //For matching the algorithm to be used. Can be Gossip or Push-Sum
let main argv =
        stopWatch.Start()
        let mutable numNodes = argv.[0] |> int
        let topology = argv.[1]
        let algorithm = argv.[2]
      
        let inbox = Inbox.Create(system)
        let mutable time = stopWatch.ElapsedMilliseconds
        numNodes <- nodeSpawn numNodes topology
       
        match algorithm with
            | "gossip" -> 
                (supervisor <! Start numNodes) 

                time <- stopWatch.ElapsedMilliseconds
                while true do
                    if (stopWatch.ElapsedMilliseconds) - time  >= (periodicTimer |> int64) then 
                        time <- stopWatch.ElapsedMilliseconds
                        inbox.Send(supervisor, QueueSender true)

            | "push-sum" -> 
                (supervisor <! PushSumStart numNodes)
                if argv.Length = 4 then
                    rounds <- argv.[3] |> int
                
                time <- stopWatch.ElapsedMilliseconds
                while true do
                    if (stopWatch.ElapsedMilliseconds) - time  >= (periodicTimer |> int64) then 
                        time <- stopWatch.ElapsedMilliseconds
                        (supervisor <! PushQueueSender true)
            | _ ->
                printfn "\n Wrong input!\n"
                Environment.Exit 1

        0 
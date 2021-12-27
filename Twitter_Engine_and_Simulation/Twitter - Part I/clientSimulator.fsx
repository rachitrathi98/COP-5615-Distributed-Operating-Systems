open System.Threading

#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#load "Functions.fsx"
open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Functions.functions

let configuration =
    ConfigurationFactory.ParseString
        (@"akka {
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            }
            remote {
                helios.tcp {
                    port = 60143
                    hostname = localhost
                }
            }
        }")

let system =
    ActorSystem.Create("RemoteFSharp", configuration)

let serverActor =
    system.ActorSelection("akka.tcp://RemoteFSharp@localhost:9001/user/server")

let mutable tweetCount : Map<string,int> = Map.empty 
let args = fsi.CommandLineArgs |> Array.tail
let mutable intUsers =  args.[0] |> int

let createRndWord() = 
    let chars = Array.concat([[|'#'|];[|'a' .. 'z'|];[|'A' .. 'Z'|];[|'0' .. '9'|];])
    let rndCharCount = Random().Next(5, 100)
    let sz = Array.length chars in
    let result = String(Array.init rndCharCount (fun _ -> chars.[Random().Next sz]))
    result

let findTweets (keys:string list, DB:Map<string, string list>) =
    let mutable tweets = []
    for key in keys do if (DB.ContainsKey(key)) then tweets <- List.append tweets DB.[key]
    tweets

let zipFProb(constant:float, user:int, allUsers:float) = 
    let users = constant / (user |> float)
    let totalUsers = users * allUsers
    let result = round(totalUsers)
    result

let zipfConstant(allUsers:int) = 
    let arrSum = [| for i in 1.0 .. allUsers |> float -> 1.0/i|] |> Array.sum
    let result = 1.0 / arrSum
    result
    
let Simulator (clientId: string)(serverRef: ActorSelection)(numSubsriber:float)(frequency:int)(mailbox: Actor<_>) = 
    let intSubscriber:int = numSubsriber |> int
    if intSubscriber > 0 then
        let userId = clientId.[8..] |> int
        let mutable followerList = List.Empty
        for i in [1..intSubscriber] do
            followerList <- followerList @ ["username" + (((userId + i) % intUsers) + 1).ToString()]
            printfn "%s is subscribed by %A" clientId followerList
            let simulateFollowers : SimulateFollowers = {
                UserId = clientId;
                FollowersList = followerList;
            }
        
            serverRef <! simulateFollowers

    else 
        printfn "%s is not subscribed by anyone. " clientId

    let getRandomUser = " @username" + ((Random().Next(intUsers)) + 1).ToString()

    let simulateStartTweet : SimulateStartTweet = { StartTweet = "startTweet"; Tweet = ""; ClientId = ""; }

    mailbox.Self <! simulateStartTweet

    let rec loop () =
        actor {
            let! message = mailbox.Receive()
            match box message with 
            | :? SimulateStartTweet as input->
                let command = input.StartTweet
                let reTweetUser = input.ClientId

                let tweet = input.Tweet
                if command = "startTweet" then
                    if frequency = 0 then
                        let sendToServer : SendToServer = { UserId = clientId; Tweet = "Tweet: " + createRndWord() + clientId.ToString(); }
                        serverActor <! sendToServer
                        Thread.Sleep(5)

                    elif frequency = 1 then
                        let sendToServer2 : SendToServer = { UserId = clientId; Tweet = ("Mention: " + createRndWord() + getRandomUser) + " " + clientId.ToString()}
                        serverActor <!sendToServer2
                        Thread.Sleep(10)
                        let simulateStartTweet : SimulateStartTweet = {StartTweet = "startTweet"; Tweet = ""; ClientId = ""; }
                        mailbox.Self <! simulateStartTweet
                        

                else 
                    printfn "Retweeted by %A from user %A" clientId reTweetUser
                    let reTweet : SendRetweet = {
                        ReTweetUser = reTweetUser;
                        Tweet = "RETWEET: "+ tweet + clientId.ToString();
                        ClientId = clientId;
                    }
                    serverActor <! reTweet
                    if frequency = 0 then 
                        Thread.Sleep 5
                    else if frequency = 1 then 
                        Thread.Sleep 10
                    else 
                        Thread.Sleep 15
                    let simulateStartTweet : SimulateStartTweet = {StartTweet = "startTweet"; Tweet = ""; ClientId = "";}
                    mailbox.Self <! simulateStartTweet
            
                    
            | _-> printfn "Invalid"
           
            return! loop ()
        }

    loop ()

let client (userId: string) (serverRef: ActorSelection) (numSubscriber:float)(frequency:int) (mailbox: Actor<_>) =   
    let generateRandNumber =
        Random().Next(0,1000)


    let sendUserId = userId
    let simulatorId = sendUserId + "simulator"
    let simulatoRef =
        spawn system simulatorId (Simulator sendUserId serverRef numSubscriber frequency)

    let rec loop () =
        actor {
            let! msg = mailbox.Receive()
            match box msg with 
            | :? SendTweet as input ->
                printfn "Tweet received by %A tweet: %A from %A" sendUserId input.Tweet input.SenderUser
                if tweetCount.ContainsKey(sendUserId) then
                    let count = tweetCount.Item(sendUserId)
                    tweetCount <- tweetCount.Add(sendUserId, count+1)
                else
                    tweetCount <- tweetCount.Add(sendUserId, 1)

                let randNo = generateRandNumber 
                if(randNo > 500) then
                    let retweet : SimulateStartTweet = {
                        StartTweet = "retweet";
                        Tweet = input.Tweet
                        ClientId = input.SenderUser;
                    }
                    simulatoRef <! retweet
 
            | _-> printfn "Failed"
            return! loop ()
        }

    loop ()

let SuperVisor =
    spawn system "SuperVisor"
    <| fun mailbox ->

        let floatUsers = intUsers |> float
        let constant = zipfConstant(intUsers)
        let high = round(ceil(floatUsers*0.1)) |> int
        let low = intUsers - high

        let register : Register = {
            UserId = intUsers
        }
        serverActor <! register
        printfn "%A clients registered" intUsers
        Thread.Sleep 1000
        System.Diagnostics.Stopwatch().Start()
        
        
        [ 1 .. intUsers ]
        |> List.map (fun id ->
            let userId = ("username" + id.ToString())
            let subscriberCount = zipFProb(constant,id, floatUsers)


            let frequency =
                if id <= high then 0
                else if id >= low then 2
                else 1


            let tempRef =
                spawn system (userId) (client userId serverActor subscriberCount frequency)

            tempRef) |> ignore

        let rec loop () =
            actor {
                let! message = mailbox.Receive()
                let sender = mailbox.Sender()
                match box message with
                | :? (int) as command ->
                    serverActor <! ("Sending bak to Server")
                    printfn "Simulation completed"
                    system.Terminate() |> ignore
                | _ -> return! loop ()
            }

        loop ()

Console.ReadLine()
system.Terminate() |> ignore
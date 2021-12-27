open System.Threading

#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#load "Functions.fsx"

open System
open Akka.Actor
open Akka.FSharp
open Functions.functions


let config =
    Configuration.parse
        @"akka {
            actor.provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            remote.helios.tcp {
                hostname = localhost
                port = 9001
            }
        }"

let watch = Diagnostics.Stopwatch()
let mutable flr: Map<string, list<string>> = Map.empty
let mutable flg: Map<string, list<string>> = Map.empty
let mutable userInit: Set<string> = Set.empty
let mutable requests: int = 0
let mutable tweetsSim: int = 0
let system = System.create "RemoteFSharp" config

let getActorRef (userId: string) =
    let userActorRef = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:60143/user/"+ userId)
    userActorRef

watch.Start()

let mutable supervisoRef: IActorRef = null

let comm =
    spawn system "server"
    <| fun mailbox ->
        let rec loop () =
            actor {

                if (watch.ElapsedMilliseconds > 100000L) then
                    supervisoRef <! 1
                    printfn "Requests Processed: %A" requests
                    printfn "Number of Tweets Sent: %A\n\n" tweetsSim
                    Thread.Sleep(1000000)
                    system.Terminate() |> ignore

                let! msg = mailbox.Receive()
                requests <- requests + 1

                match box msg with
                | :? (string) as cmd ->

                    printfn "Requests Processed are: %i" requests
                    printfn "Total Tweets are %i" tweetsSim
                    mailbox.Sender() <! "DONE"
                    system.Terminate() |> ignore

                | :? SimulateFollowers as ip ->
                    let userId = ip.UserId
                    let flList = ip.FollowersList

                    for li in flList do

                        if ((userInit.Contains(li))
                            && userInit.Contains(userId)) then
                            let check = flr.TryFind(userId)

                            match check with

                            | None -> printfn "UserId not check"

                            | Some currList ->
                                let mutable temp = currList
                                temp <- temp @ [ li ]
                                flr <- flr.Add(userId, temp)
                                ()

                            let checkNew = flg.TryFind(li)

                            match checkNew with
                            | Some currList ->
                                let mutable temp1 = currList
                                temp1 <- temp1 @ [ userId ]
                                flg <- flg.Add(li, temp1)

                            | None -> printfn "List is unchecked"

                | :? Register as ip ->

                    for users in [ 1 .. ip.UserId ] do

                        let user = "username" + users.ToString()
                        userInit <- userInit.Add(user)
                        flr <- flr.Add(user, list.Empty)
                        flg <- flg.Add(user, list.Empty)

                    supervisoRef <- mailbox.Sender()

                | :? SendToServer as ip ->

                    let userId = ip.UserId
                    let tweet = ip.Tweet
                    let check = flr.TryFind(userId)

                    match check with

                    | None -> printfn "Error!"

                    | Some x ->
                        for u in x do
                            let userActRef = getActorRef u
                            let sendTweet: SendTweet = { Tweet = tweet; SenderUser = userId }
                            tweetsSim <- tweetsSim + 1
                            userActRef <! sendTweet

                | :? SendRetweet as ip ->

                    let tweet = ip.Tweet
                    let senderUser = ip.ClientId

                    let check = flr.TryFind(senderUser)

                    match check with
                    | None -> printfn "Invalid"
                    | Some flList ->
                        for u in flList do
                            let userActRef = getActorRef u

                            let senderTweet: SendTweet =
                                { Tweet = tweet
                                  SenderUser = senderUser }

                            tweetsSim <- tweetsSim + 1
                            userActRef <! senderTweet

                | _ -> printfn "Invalid Command"

                return! loop ()

            }

        loop ()

Console.ReadLine() |> ignore
system.Terminate() |> ignore

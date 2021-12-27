#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#load "Functions.fsx"

open System
open System.Threading
open Functions.functions
open Akka.Actor
open Akka.Configuration
open Akka.FSharp

let configuration = ConfigurationFactory.ParseString(@"akka {
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                
            }
            remote {
                helios.tcp {
                    port = 0
                    hostname = localhost
                }
            }
        }")


let system = ActorSystem.Create("RemoteFSharp", configuration)
let echoClient = 
    system.ActorSelection
        ("akka.tcp://RemoteFSharp@localhost:9000/user/EchoServer")
let mutable tweetIdSet : Set<int> = Set.empty
let rand = Random()
let mutable randomNo = rand.Next(1, Int32.MaxValue)
let mutable flag = true
let mutable tweetFlag = true
let mutable followFlag = true
let mutable reTweetFlag = true

while flag do
    printfn 
        "1.Register User\n2.Login User\n3.Logout User\n4.Send Tweet\n5.Follow/Subscribe User\n6.Subscribed Tweets\n7.Query Tweets\n8.Live Users\n9.Exit"
    printfn "Enter your input choice: "
    let input = System.Console.ReadLine()
    match input with
    | "1" -> // Register
             
        printfn "Enter username that will be used for registration: "
        let username = System.Console.ReadLine()
        
        let registerUser : RegisterUser = 
            {Username = username;
             Status = false}
        
        let task : Async<obj> = echoClient <? registerUser
        let response : obj = Async.RunSynchronously(task)
        printfn "%s\n" (string(response))
    | "2" -> // Login
             
        printfn "Enter username for login: "
        let username = System.Console.ReadLine()
        
        let loginUser : LoginUser = 
            {Username = username;
             Status = true}
        
        let task : Async<obj> = echoClient <? loginUser
        let response : obj = Async.RunSynchronously(task)
        printfn "%s\n" (string(response))
    | "3" -> // Logout
             
        printfn "Enter username for logout: "
        let username = System.Console.ReadLine()
        let logoutUser : LogoutUser = {Username = username}
        let task : Async<obj> = echoClient <? logoutUser
        let response : obj = Async.RunSynchronously(task)
        printfn "%s\n" (string(response))
    | "4" -> // Tweet
             
        tweetFlag <- true
        printfn "Enter username: "
        let username = System.Console.ReadLine()
        while tweetFlag do
            printfn "Do you want to post a tweet?: (Y/N) "
            let input = Console.ReadLine()
            match input with
            | "Y" -> 
                printfn "Enter tweet: "
                while (not(tweetIdSet.Contains(randomNo))) do
                    tweetIdSet <- tweetIdSet.Add(randomNo)
                let randomTweetId = randomNo |> string
                randomNo <- rand.Next(1, Int32.MaxValue)
                let tweet = System.Console.ReadLine()
                
                let tweetInfo : TweetInfo = 
                    {Username = username;
                     TweetId = randomTweetId;
                     Tweet = tweet}
                
                let task : Async<obj> = echoClient <? tweetInfo
                let response : obj = Async.RunSynchronously(task)
                printfn "%s" (string(response))
            | "N" -> tweetFlag <- false
            | _ -> printfn "Please enter a valid input"
    | "5" -> // Follow/Subscribe Users
             
        followFlag <- true
        printfn "Enter username: "
        let input1 = System.Console.ReadLine()
        printfn "Enter follower username: "
        let input2 = System.Console.ReadLine()
        
        let follow : Follow = 
            {WantsToFollow = input1;
             IsFollowedBy = input2}
        
        let task : Async<obj> = echoClient <? follow
        let response : obj = Async.RunSynchronously(task)
        printfn "%s\n" (string(response))
        while followFlag do
            printfn "Do you want to follow more users? (Y/N)"
            let input3 = Console.ReadLine()
            match input3 with
            | "Y" -> 
                printfn "Enter username you want to follow: "
                let input4 = System.Console.ReadLine()
                
                let follow : Follow = 
                    {WantsToFollow = input1;
                     IsFollowedBy = input4}
                
                let task : Async<obj> = echoClient <? follow
                let response : obj = Async.RunSynchronously(task)
                printfn "%s\n" (string(response))
            | "N" -> followFlag <- false
            | _ -> printfn "Invalid Input"
            ()
    | "6" -> // Get Subscribed tweets
             
        printfn "Enter username: "
        let input = System.Console.ReadLine()
        let getSubscribedTweetsfromFollowing : GetSubscribedTweetsfromFollowing = 
            {Username = input}
        printfn "%s, Tweets subscribed: " input
        let task : Async<obj> = echoClient <? getSubscribedTweetsfromFollowing
        let response : obj = Async.RunSynchronously(task)
        printfn "%s" (string(response))
        reTweetFlag <- true
        while reTweetFlag do
            printfn "Do you want to retweet? (Y/n)"
            let wantToRetweet = System.Console.ReadLine()
            match wantToRetweet with
            | "Y" -> 
                printfn "Enter username: "
                let loggedInUser = System.Console.ReadLine()
                printfn "Enter Retweet message username: "
                let username = System.Console.ReadLine()
                let serverToClient : ServerToClient = 
                    {SendingToServer = username}
                let task : Async<list<string>> = echoClient <? serverToClient
                let response : list<string> = Async.RunSynchronously(task)
                let mutable counter = 1
                for tweet in response do
                    printfn "Tweet ID %i : %s" counter tweet
                    counter <- counter + 1
                printfn "Enter Tweet ID: "
                let tweetNo = System.Console.ReadLine() |> int
                
                let retweet : ReTweetInfo = 
                    {TweetId = tweetNo - 1;
                     Username = loggedInUser;
                     ReTweetUsername = username}
                
                let task : Async<obj> = echoClient <? retweet
                let response : obj = Async.RunSynchronously(task)
                printf "%s" (string(response))
            | "n" -> reTweetFlag <- false
            | _ -> printfn "Invalid Operation"
    | "7" -> // HashTag-Mention Query
             
        printfn 
            "Search Query: 1.HashTag OR MentionTag\n2.Hashtag AND Mention Tags Both"
        let input = System.Console.ReadLine()
        match input with
        | "1" -> 
            printfn "Enter HashTag / MentionTag: "
            let word = System.Console.ReadLine()
            let wordToSearch : QueryBy = {WordToSearch = word}
            let task : Async<obj> = echoClient <? wordToSearch
            let response : obj = Async.RunSynchronously(task)
            printfn "%s\n" (string(response))
        | "2" -> 
            printf "Enter Hastag: "
            let hashTag = System.Console.ReadLine()
            printf "Enter MentionTag: "
            let mentionTag = System.Console.ReadLine()
            
            let tags : HashTagAndMentionTag = 
                {HashTag = hashTag;
                 MentionTag = mentionTag}
            
            let task : Async<obj> = echoClient <? tags
            let response : obj = Async.RunSynchronously(task)
            printfn "%s\n" (string(response))
        | _ -> printfn "Invalid Input"
    | "8" -> //Get Tweets of All users
             
        printfn "Get tweets of all live users:\n"
        let getAllTweets : GetAllTweets = {LiveTweets = true}
        let task : Async<obj> = echoClient <? getAllTweets
        let response : obj = Async.RunSynchronously(task)
        printf "%s\n" (string(response))
    | "9" -> // Termination
             
        printfn "Exiting..."
        let terminate : TerminateInteractive = {Input = 2}
        let task : Async<obj> = echoClient <? terminate
        let response : obj = Async.RunSynchronously(task)
        if (string(response)) = "Done" then 
            Thread.Sleep(3000)
            system.Terminate() |> ignore
    | _ -> printfn "Invalid Option"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#load "Functions.fsx"
#load "Variables.fsx"

open System
open System.Data
open Akka.FSharp
open Functions.functions
open Variables.Variables

let registerUser = new DataTable("registerUser")

registerUser.Columns.Add("username", typeof<string>)
registerUser.Columns.Add("status", typeof<bool>)
registerUser.PrimaryKey <- [|registerUser.Columns.["username"]|]

let tweetTable = new DataTable("tweetTable")

tweetTable.Columns.Add("tweetId", typeof<string>)
tweetTable.Columns.Add("username", typeof<string>)
tweetTable.Columns.Add("tweet", typeof<string>)
tweetTable.PrimaryKey <- [|tweetTable.Columns.["tweetId"]|]

let mutable totalUsers : int = 0
let mutable totalTweets : int = 0
let mutable loggedInUsers : int = 0
let mutable loggedOutUsers : int = 0
let mutable totalReTweets : int = 0
let config = Configuration.parse @"akka {
            
            actor.provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            
            remote.helios.tcp {
                hostname = localhost
                port = 9000
            }
            
        }"
let splitLine = (fun (line : string) -> Seq.toList(line.Split " "))
let system = System.create "RemoteFSharp" config

let extractString(username : string, dataRowVal : DataTable) = 
    let expression : string = sprintf "username = '%s'" username
    let result = dataRowVal.Select(expression)
    let mutable temp : List<string> = List.empty
    result 
    |> Seq.iter(fun x -> temp <- temp @ [(String.Join(" ", x.ItemArray))])
    let splitLine1 = (fun (line : string) -> Seq.toList(line.Split ' '))
    let mutable str = ""
    for strin in temp do
        str <- str + strin
    let extractedString = splitLine1 str
    let usernamVal = extractedString.[0]
    for i in [0..result.Length - 1] do
        result.[i].Delete()
    dataRowVal.AcceptChanges()

let echoServer = 
    spawn system "EchoServer" <| fun mailbox -> 
        let rec loop() = 
            actor {
                let! message = mailbox.Receive()
                let sender = mailbox.Sender()
                let context = mailbox.Context.Self
                match box message with
                | :? TerminateInteractive as input -> 
                    let command = input.Input
                    sender <! "Done"
                    system.Terminate() |> ignore
                | :? RegisterUser as input -> 
                    let username = input.Username
                    let status = input.Status
                    if not(allUsers.Contains(username)) then 
                        let newRow = registerUser.NewRow()
                        newRow.SetField("username", username)
                        newRow.SetField("status", status)
                        totalUsers <- totalUsers + 1
                        loggedOutUsers <- loggedOutUsers + 1
                        allUsers <- allUsers.Add(username)
                        followingMap <- followingMap.Add(username, List.empty)
                        followersMap <- followersMap.Add(username, List.empty)
                        registerUser.Rows.Add(newRow)
                        printfn 
                            "Total Registered Users: %i\nTotal Online Users: %i\nTotal Offline  Users: %i\nTotal Tweets: %i\nTotal ReTweets: %i\n" 
                            totalUsers loggedInUsers loggedOutUsers totalTweets 
                            totalReTweets
                        printfn "User %s: Registered" username
                        printfn "Registered users: %A\n" allUsers
                        sender 
                        <! sprintf 
                               "%s has been successfully registered. Welcome!" 
                               username
                    else 
                        sender 
                        <! sprintf "%s is already registered. Please login" 
                               username
                | :? LoginUser as input -> 
                    let username = input.Username
                    let status = input.Status
                    if allUsers.Contains(username) then 
                        if not(setOfLoggedInUsers.Contains(username)) then 
                            setOfLoggedInUsers <- setOfLoggedInUsers.Add
                                                      (username)
                            extractString(username, registerUser)
                            let newRow = registerUser.NewRow()
                            newRow.SetField("username", username)
                            newRow.SetField("status", status)
                            registerUser.Rows.Add(newRow)
                            loggedInUsers <- loggedInUsers + 1
                            loggedOutUsers <- loggedOutUsers - 1
                            printfn 
                                "Total Registered Users: %i\nTotal Online Users: %i\nTotal Offline  Users: %i\nTotal Tweets: %i\nTotal ReTweets: %i\n" 
                                totalUsers loggedInUsers loggedOutUsers 
                                totalTweets totalReTweets
                            printfn "User %s: Logged in" username
                            printfn "Logged in users: %A\n" setOfLoggedInUsers
                            sender 
                            <! sprintf 
                                   "%s has been successfully logged in. Let's Tweet!" 
                                   username
                        else 
                            sender 
                            <! sprintf "%s is already logged in. Please Check" 
                                   username
                    else 
                        sender 
                        <! sprintf "%s is not registered. Please Register" 
                               username
                | :? LogoutUser as input -> 
                    let username = input.Username
                    let status = false
                    if allUsers.Contains(username) then 
                        if setOfLoggedInUsers.Contains(username) then 
                            extractString(username, registerUser)
                            let newRow = registerUser.NewRow()
                            newRow.SetField("username", username)
                            newRow.SetField("status", status)
                            registerUser.Rows.Add(newRow)
                            loggedInUsers <- loggedInUsers - 1
                            loggedOutUsers <- loggedOutUsers + 1
                            setOfLoggedInUsers <- setOfLoggedInUsers.Remove
                                                      (username)
                            printfn 
                                "Total Registered Users: %i\nTotal Online Users: %i\nTotal Offline  Users: %i\nTotal Tweets: %i\nTotal ReTweets: %i\n" 
                                totalUsers loggedInUsers loggedOutUsers 
                                totalTweets totalReTweets
                            printfn "User %s: Logged in" username
                            printfn "Logged in users: %A\n" setOfLoggedInUsers
                            sender <! sprintf "%s is logged out" username
                        else 
                            sender 
                            <! sprintf "%s is not logged in. Please Login" 
                                   username
                    else 
                        sender 
                        <! sprintf "%s is not registered. Please Register" 
                               username
                | :? TweetInfo as input -> 
                    let tweetId = input.TweetId
                    let username = input.Username
                    let tweet = input.Tweet
                    if allUsers.Contains(username) then 
                        if setOfLoggedInUsers.Contains(username) then 
                            if mapUserNametoTweets.ContainsKey(username) then 
                                let mutable temp = 
                                    mapUserNametoTweets.Item(username)
                                temp <- temp @ [tweet]
                                mapUserNametoTweets <- mapUserNametoTweets.Add
                                                           (username, temp)
                            else 
                                mapUserNametoTweets <- mapUserNametoTweets.Add
                                                           (username, list.Empty)
                                let mutable temp = 
                                    mapUserNametoTweets.Item(username)
                                temp <- temp @ [tweet]
                                mapUserNametoTweets <- mapUserNametoTweets.Add
                                                           (username, temp)
                            let res = splitLine tweet
                            for value in res do
                                if value.Contains("#") then 
                                    if mapUserToHashTags.ContainsKey
                                           (value.[1..value.Length]) then 
                                        mapUserToHashTags <- mapUserToHashTags.Add
                                                                 (value.[1..value.Length], 
                                                                  
                                                                  mapUserToHashTags.[value.[1..value.Length]]
                                                                      .Add(tweet))
                                    else 
                                        mapUserToHashTags <- mapUserToHashTags.Add
                                                                 (value.[1..value.Length], 
                                                                  Set.empty)
                                        mapUserToHashTags <- mapUserToHashTags.Add
                                                                 (value.[1..value.Length], 
                                                                  
                                                                  mapUserToHashTags.[value.[1..value.Length]]
                                                                      .Add(tweet))
                                if value.Contains("@") then 
                                    if mapUserToMentionTags.ContainsKey
                                           (value.[1..value.Length]) then 
                                        mapUserToMentionTags <- mapUserToMentionTags.Add
                                                                    (value.[1..value.Length], 
                                                                     
                                                                     mapUserToMentionTags.[value.[1..value.Length]]
                                                                         .Add(tweet))
                                    else 
                                        mapUserToMentionTags <- mapUserToMentionTags.Add
                                                                    (value.[1..value.Length], 
                                                                     Set.empty)
                                        mapUserToMentionTags <- mapUserToMentionTags.Add
                                                                    (value.[1..value.Length], 
                                                                     
                                                                     mapUserToMentionTags.[value.[1..value.Length]]
                                                                         .Add(tweet))
                            mapTweetIdToUserName <- mapTweetIdToUserName.Add
                                                        (tweetId, username) //NOT USED
                            let newRow = tweetTable.NewRow()
                            newRow.SetField("tweetId", tweetId)
                            newRow.SetField("username", username)
                            newRow.SetField("tweet", tweet)
                            tweetTable.Rows.Add(newRow)
                            totalTweets <- totalTweets + 1
                            printfn 
                                "Total Registered Users: %i\nTotal Online Users: %i\nTotal Offline Users: %i\nTotal Tweets: %i\nTotal ReTweets: %i\n" 
                                totalUsers loggedInUsers loggedOutUsers 
                                totalTweets totalReTweets
                            printfn "User to Tweets: %A" mapUserNametoTweets
                            printfn "User %s Tweeted:  \"%s\" " username tweet
                            sender 
                            <! sprintf "%s tweeted:  \"%s\" " username tweet
                        else 
                            sender 
                            <! sprintf "%s is not logged in. Please Login" 
                                   username
                    else 
                        sender 
                        <! sprintf "%s is not registered. Please Register" 
                               username
                | :? ReTweetInfo as input -> 
                    let tweetID = input.TweetId
                    let username = input.Username
                    let reTweetusername = input.ReTweetUsername
                    let mutable retweet : string = "RE: " + llist.[tweetID]
                    if allUsers.Contains(username) then 
                        if allUsers.Contains(reTweetusername) then 
                            if setOfLoggedInUsers.Contains(username) then 
                                printfn "%s Retweeted:\n%s: \"%s\" \n" username 
                                    reTweetusername llist.[tweetID]
                                sender 
                                <! sprintf "%s Retweeted:\n%s: \"%s\" \n" 
                                       username reTweetusername llist.[tweetID]
                                if mapUserNametoTweets.ContainsKey(username) then 
                                    let mutable temp = 
                                        mapUserNametoTweets.Item(username)
                                    temp <- temp @ [retweet]
                                    mapUserNametoTweets <- mapUserNametoTweets.Add
                                                               (username, temp)
                                else 
                                    mapUserNametoTweets <- mapUserNametoTweets.Add
                                                               (username, 
                                                                list.Empty)
                                    let mutable temp = 
                                        mapUserNametoTweets.Item(username)
                                    temp <- temp @ [retweet]
                                    mapUserNametoTweets <- mapUserNametoTweets.Add
                                                               (username, temp)
                                totalReTweets <- totalReTweets + 1
                                printfn 
                                    "Total Registered Users: %i\nTotal Online Users: %i\nTotal Offline  Users: %i\nTotal Tweets: %i\nTotal ReTweets: %i\n" 
                                    totalUsers loggedInUsers loggedOutUsers 
                                    totalTweets totalReTweets
                                printfn "User to Tweets: %A" mapUserNametoTweets
                            else 
                                sender 
                                <! sprintf "%s please login to retweet" username
                        else 
                            sender 
                            <! sprintf "%s named user does not exist" 
                                   reTweetusername
                    else 
                        sender 
                        <! sprintf "%s is not registered. Please Register" 
                               username
                | :? Follow as input -> 
                    let wantsToFollow = input.WantsToFollow
                    let isFollowedBy = input.IsFollowedBy
                    if isFollowedBy = wantsToFollow then 
                        sender <! sprintf "User cannot follow their own self"
                    elif allUsers.Contains(wantsToFollow) then 
                        if allUsers.Contains(isFollowedBy) then 
                            if setOfLoggedInUsers.Contains(wantsToFollow) then 
                                let mutable temp = 
                                    followersMap.Item(isFollowedBy)
                                let mutable alreadyFollowing = false
                                for user in temp do
                                    if user = wantsToFollow then 
                                        alreadyFollowing <- true
                                let mutable temp1 = 
                                    followingMap.Item(wantsToFollow)
                                if alreadyFollowing then 
                                    sender 
                                    <! sprintf "%s already follows %s" 
                                           wantsToFollow isFollowedBy
                                else 
                                    temp <- temp @ [wantsToFollow]
                                    followersMap <- followersMap.Add
                                                        (isFollowedBy, temp)
                                    temp1 <- temp1 @ [isFollowedBy]
                                    followingMap <- followingMap.Add
                                                        (wantsToFollow, temp1)
                                    printfn "User %s started following User %s" 
                                        wantsToFollow isFollowedBy
                                    sender 
                                    <! sprintf "%s started following %s" 
                                           wantsToFollow isFollowedBy
                                    printfn "Following Map: %A" followingMap
                                    printfn "Followers Map: %A" followersMap
                            else 
                                sender 
                                <! sprintf "%s please login to follow %s" 
                                       wantsToFollow isFollowedBy
                        else 
                            sender 
                            <! sprintf "%s named user does not exist" 
                                   isFollowedBy
                    else 
                        sender 
                        <! sprintf "%s is not registered. Please Register" 
                               wantsToFollow
                | :? QueryBy as input -> 
                    let word = input.WordToSearch
                    let firstChar = word.[0]
                    let subWord = word.[1..word.Length]
                    if firstChar = '#' then 
                        if mapUserToHashTags.ContainsKey(subWord) then 
                            let setStr = mapUserToHashTags.Item(subWord)
                            let mutable finalStr = ""
                            for str in setStr do
                                finalStr <- finalStr + str + "\n"
                            printfn "HashTag to Tweets: %A" mapUserToHashTags
                            sender 
                            <! sprintf "Tweets with HashTag %s are as \n%s" word 
                                   finalStr
                    if firstChar = '@' then 
                        if mapUserToMentionTags.ContainsKey(subWord) then 
                            let setStr = mapUserToMentionTags.Item(subWord)
                            let mutable finalStr = ""
                            for str in setStr do
                                finalStr <- finalStr + str + "\n"
                            printfn "Mention to Tweets: %A" mapUserToMentionTags
                            sender 
                            <! sprintf "Tweets with Mention Tags %s are :\n%s" 
                                   word finalStr
                | :? GetAllTweets -> 
                    let mutable finalStr = ""
                    for KeyValue(key, value) in mapUserNametoTweets do
                        let mutable sendStr = ""
                        for tweet in [0..value.Length - 1] do
                            sendStr <- sendStr 
                                       + sprintf "%s Tweeted: \"%s\"\n" key 
                                             value.[tweet]
                        finalStr <- finalStr + sendStr
                    sender <! sprintf "%s\n" finalStr
                | :? GetSubscribedTweetsfromFollowing as input -> 
                    let username = input.Username
                    if allUsers.Contains(username) then 
                        if setOfLoggedInUsers.Contains(username) then 
                            let mutable followers : List<string> = List.empty
                            if followingMap.ContainsKey(username) then 
                                let tempList = followingMap.Item(username)
                                if (tempList.Length = 0) then 
                                    sender 
                                    <! sprintf 
                                           "%s must follow first to get subscribed tweets" 
                                           username
                                else 
                                    followers <- followers 
                                                 @ followingMap.Item(username)
                                    let mutable finalStr = ""
                                    for follower in [0..followers.Length - 1] do
                                        let mutable sendStr = ""
                                        if (mapUserNametoTweets.ContainsKey
                                                (followers.[follower])) then 
                                            let mutable temp = 
                                                mapUserNametoTweets.Item
                                                    (followers.[follower])
                                            for tweet in [0..temp.Length - 1] do
                                                sendStr <- sendStr 
                                                           + sprintf 
                                                                 "%s Tweeted: \"%s\"\n" 
                                                                 followers.[follower] 
                                                                 temp.[tweet]
                                        finalStr <- finalStr + sendStr
                                    sender <! finalStr
                        else 
                            sender 
                            <! sprintf "%s is not logged in. Please Login" 
                                   username
                    else 
                        sender 
                        <! sprintf "%s is not registered. Please Register" 
                               username
                | :? HashTagAndMentionTag as input -> 
                    let hashTag = input.HashTag
                    let mentionTag = input.MentionTag
                    let newHastag = hashTag.[1..hashTag.Length - 1]
                    let newMentionTag = mentionTag.[1..mentionTag.Length - 1]
                    let tweetStr = mapUserToHashTags.Item(newHastag)
                    let mutable finalStr = ""
                    for str in tweetStr do
                        finalStr <- finalStr + str + "\n"
                    sender 
                    <! sprintf "Tweets with HashTag %s and MentionTag : %s\n%s" 
                           hashTag mentionTag finalStr
                | :? ServerToClient as input -> 
                    let username = input.SendingToServer
                    if allUsers.Contains(username) then 
                        if setOfLoggedInUsers.Contains(username) then 
                            llist <- List.Empty
                            if mapUserNametoTweets.ContainsKey(username) then 
                                llist <- llist 
                                         @ mapUserNametoTweets.Item(username)
                                sender <! llist
                            else 
                                sender 
                                <! sprintf "No tweets available to retweet"
                        else 
                            sender 
                            <! sprintf "%s is not logged in. Please Login" 
                                   username
                    else 
                        sender 
                        <! sprintf "%s is not registered. Please Register" 
                               username
                | _ -> failwith "Unknown message"
                return! loop()
            }
        loop()

Console.ReadLine() |> ignore
system.Terminate() |> ignore
open System

module Variables = 

    let mutable mapTweetIdToUserName : Map<String, String> = Map.empty 
    let mutable mapUserToHashTags : Map<String, Set<string>> = Map.empty
    let mutable mapUserToMentionTags : Map<String, Set<string>> = Map.empty 
    let mutable followersMap : Map<String, list<string>> = Map.empty
    let mutable followingMap : Map<String, list<string>> = Map.empty 
    
    let mutable mapUserNametoTweets: Map<String, list<string>> = Map.empty
    
    let mutable allUsers : Set<string> = Set.empty
    let mutable setOfLoggedInUsers : Set<string> = Set.empty
    let mutable llist : list<string> =  List.empty

    

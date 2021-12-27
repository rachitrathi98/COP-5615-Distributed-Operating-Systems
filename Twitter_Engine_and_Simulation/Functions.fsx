open System

module functions = 

    type GetSubscribedTweetsfromFollowing = {
        Username : string;
    }

    type RegisterUser = {
        Username : string;
        Status : bool;
    }

    type LoginUser = {
        Username : string;
        Status : bool;
    }

    type LogoutUser = {
        Username : string;
    }

    type TweetInfo = {
        TweetId : string;
        Username : string;
        Tweet : string;
    }

    type Follow = {
        WantsToFollow : string;
        IsFollowedBy : string;
    }

    type QueryBy = {
        WordToSearch : string;
    }

    type HashTagAndMentionTag = {
        HashTag : string;
        MentionTag : string;
    }

    type ServerToClient = {
        SendingToServer : string
    }

    type ReTweetInfo = {
        TweetId : int;
        Username : string;
        ReTweetUsername: string;
    }

    type GetAllTweets = {
        LiveTweets : bool;
    }

    type TerminateInteractive = {
        Input : int;
    }


    type SendToServer = {
    UserId : string;
    Tweet : string;
    }

    type SendRetweetToServer = {
        UserId : string;
        Tweet : string;
        OgTweet : string;
    }
    type SimulateFollowers = {
        UserId : string;
        FollowersList : list<string>
    }

    type Register = {
        UserId : int;
    }


    type SimulateStartTweet = {
        StartTweet : string;
        Tweet : string;
        ClientId : string;
    }

    type SendTweet = {
        Tweet : string;
        SenderUser : string;
    }

    type SendRetweet = {
        Tweet : string;
        ClientId : string;
        ReTweetUser : string;
    }
    
    type SimulateStartRetweet = {
        ClientId : string;
        Tweet : string;
        OriginalTweeter : string;
    }

    type SimulateLogin = {
        UserId : int
    }
    

    
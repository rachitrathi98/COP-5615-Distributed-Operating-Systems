
#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Security.Cryptography
open System.Text

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                debug : {
                    receive : on
                    autoreceive : on
                    lifecycle : on
                    event-stream : on
                    unhandled : on
                }
            }
            remote {
                helios.tcp {
                    port = 3000
                    hostname = 10.20.206.14
                }
            }
        }")

let mutable count = 0
let mutable ref = null
let system = ActorSystem.Create("RemoteFSharp", configuration)

type Information = 
    | Parameter of (string*int)
    | Finish of (string)
    | Input of (string)
    | Finished of (string*string)

let rec sets n l = 
                match n, l with
                | 0, _ -> [[]]
                | _, [] -> []
                | k, (x::xs) -> List.map ((@) [x]) (sets (k-1) xs) @ sets k xs


//to keep track of the workers

let ChildActor (mailbox:Actor<_>)=
    let rec loop()=actor{
        let! message = mailbox.Receive()
        match message with

        | Parameter(h, setLength) -> 
            let result = sets setLength ['a'..'z']
            for x in result do
                // printfn "in loop"
                let string = "rachit.rathi" + System.String.Concat(Array.ofList(x))
                let hash = SHA256Managed.Create().ComputeHash(Encoding.UTF8.GetBytes(string)) //Computes Hash
                let mutable res = ""
                
                for x in hash do 
                    res <- res + x.ToString("x2")  
               
                let length = String.length h 
                let bitCount = res.[0 .. length-1] 
                if bitCount = h && res.[length] <> '0' then 
                    let result = string + "\t" + res 
                    printfn "Sent to client"
                    mailbox.Sender() <! Finished("Done", result.ToString()) //Send Result to mailbox
            count <- count + 1
            // printfn "in"
            mailbox.Sender() <! Finish("Finished")

        | _ -> ()

        return! loop()
    }
    loop()


let ParentActor (mailbox:Actor<_>) =
    let totalActors = 7
    let childActorsPool = 
            [for i in 1 .. (totalActors+1) do yield spawn system ("Child" + (string) i) ChildActor]

    let rec loop () = actor {   

        let! message = mailbox.Receive()
        match message with 

        | Input(header) -> 
        
            for setLength in [1 .. totalActors] do       //Task Allocation
                childActorsPool.[setLength] <! Parameter(header, setLength)

        | Finished("Done", result) -> 
            //printf("Done Processing")
            ref <! "result" + ","+ result //Send result to mailbox

        | Finish("Finished") -> 
     //printf("Done Processing")
            if count = totalActors then
                ref <! "ProcessingDone"
                system.Terminate() |> ignore
         
        | _ -> ()
       
        return! loop()
    }
    loop()

let localRef = spawn system "localRef" ParentActor


//Server side connection
let comm = 
    spawn system "server"
    <| fun mailbox ->
        let rec loop() =
            actor {
                let! msg = mailbox.Receive()
                printfn "%s" msg 
                localRef <! Input(msg)
                ref <- mailbox.Sender() //Send messages to client
                return! loop() 
            }
        loop()


system.WhenTerminated.Wait()

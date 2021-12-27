
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

let k = 4
let mutable zeroBits= ""
for i in 1 .. k do 
    zeroBits <- zeroBits + "0"

printfn "Bitcoin mining started!"

let serverIp = fsi.CommandLineArgs.[1] |> string
let serverPort = fsi.CommandLineArgs.[2] |>string

let address = "akka.tcp://RemoteFSharp@" + serverIp + ":" + serverPort + "/user/server"  // Calling Server using IP and Port
let mutable remoteWorkDone = false
let mutable localWorkDone = false
let mutable count = 0

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                
            }
            remote {
                helios.tcp {
                    port = 8778
                    hostname = localhost
                }
            }
        }")

let system = ActorSystem.Create("ClientFsharp", configuration)

type Information = 
    | Parameter of (string*int)
    | Finish of (string)
    | Input of (string)

let rec sets n l = 
                match n, l with
                | 0, _ -> [[]]
                | _, [] -> []
                | k, (x::xs) -> List.map ((@) [x]) (sets (k-1) xs) @ sets k xs


let ChildActor (mailbox:Actor<_>)=
    let rec loop()=actor{
        let! message = mailbox.Receive()
        match message with

        | Parameter(h, setLength) -> 
            let result = sets setLength ['a'..'z']
            for x in result do

                let string = "rachit.rathi" + System.String.Concat(Array.ofList(x))
                let hash = SHA256Managed.Create().ComputeHash(Encoding.UTF8.GetBytes(string)) //Computes Hash
                let mutable res = ""
     
                for x in hash do 
                    res <- res + x.ToString("x2")  
               
                let length = String.length h 
                let bitCount = res.[0 .. length-1] 
                
                if bitCount = h && res.[length] <> '0' then 
                    let result = string + "\t" + res 
                    printfn "From Client"
                    printfn $"{result}"

            count <- count + 1
            mailbox.Sender() <! Finish("Done")

        | _ -> ()

        return! loop()
    }
    loop()

let ParentActor (mailbox:Actor<_>) =
    let totalActors = 6
    let childActorsPool = 
            [for i in 1 .. (totalActors+1) do yield spawn system ("Child" + (string) i) ChildActor]

    let rec loop () = actor {   

        let! message = mailbox.Receive()
        match message with 

        | Input(header) -> 
        
            for setLength in [1 .. totalActors] do       //Task Allocation
                childActorsPool.[setLength] <! Parameter(header, setLength)
                    
        | Finish("Done") -> 
        
            if count = totalActors then
               localWorkDone <- true;
        | _ -> ()
       
        return! loop()
    }
    loop()

//CLient to server connection
let comm = 
    spawn system "client"
    <| fun mailbox ->
        let rec loop() =
            actor {
                let! msg = mailbox.Receive()
                // printfn "%s" msg 
                let response =msg|>string
                let command = (response).Split ','
                if command.[0].CompareTo("init")=0 then
                    let echoClient = system.ActorSelection(address)
                    let msgToServer = (zeroBits)
                    echoClient <! msgToServer //Send to Server
                    let dispatcherRef = spawn system "Dispatcher" ParentActor
                    dispatcherRef <! Input(zeroBits)
                elif command.[0].CompareTo("result")=0 then //To check response from server
                     printfn "From Server"
                     printfn "%s" command.[1]
                elif response.CompareTo("ProcessingDone")=0 then
                     system.Terminate() |> ignore
                     remoteWorkDone <- true
                else
                    printfn "-%s-" msg

                return! loop() 
            }
        loop()



printfn "****************************Mining started!************************************"
comm <! "init"
while (not localWorkDone && not remoteWorkDone) do
system.WhenTerminated.Wait()
printfn "***************************Mining ended!***************************************"



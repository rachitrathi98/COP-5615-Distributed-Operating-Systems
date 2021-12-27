#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
open System
open Akka.FSharp
open System.Security.Cryptography
open System.Text


//Creates Actor System
let system = System.create "BitcoinMiner" (Configuration.defaultConfig())

//Defines discriminated Unions of different types
type Information = 
    | Parameter of (string*int)
    | Finish of (string)
    | Input of (string)

//To create different distinct sets/combinations from 'a' to 'z'
let rec sets n l = 
                match n, l with
                | 0, _ -> [[]]
                | _, [] -> []
                | k, (x::xs) -> List.map ((@) [x]) (sets (k-1) xs) @ sets k xs

let mutable count = 0

// Child Actors - Performs the main business logic by taking input from parent, gneerates hashes for the combinations and finds the hash having zeroBits from the start
let ChildActor (mailbox:Actor<_>) =
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with

        | Parameter(bits, setLength) -> 
            let result = sets setLength ['a'..'z']
            for x in result do

                let string = "rachit.rathi" + System.String.Concat(Array.ofList(x))
                let hash = SHA256Managed.Create().ComputeHash(Encoding.UTF8.GetBytes(string)) //Computes Hash
                let mutable res = ""
     
                for x in hash do 
                    res <- res + x.ToString("x2")  
               
                let length = String.length bits 
                let bitCount = res.[0 .. length-1] 
                
                if bitCount = bits && res.[length] <> '0' then 
                    let result = string + "\t" + res 
                    printfn $"{result}"

            count <- count + 1
            mailbox.Sender() <! Finish("Done")

        | _ -> ()

        return! loop()
    }
    loop()
             
// Parent - Spawns the child actor pool and splits the tasks to child actors based on processor cores
let ParentActor (mailbox:Actor<_>) =
    
    let totalActors = 6;
    let childActorsPool = 
            [for i in 1 .. (totalActors+1) do yield spawn system ("Child" + (string) i) ChildActor]   //Call Child Actor

    let rec loop () = actor {   

        let! message = mailbox.Receive()
        match message with 

        | Input(bits) -> 
        
            for setLength in [1 .. totalActors] do                       //Task Allocation to child Actors
                childActorsPool.[setLength] <! Parameter(bits, setLength)
                    
        | Finish("Done") ->  // Check if alloted task is finished for each actor.
        
            if count = totalActors then
                mailbox.Context.System.Terminate() |> ignore
        | _ -> ()
       
        return! loop()
    }
    loop()

//Call the Parent Actor
let parent = spawn system "parent" ParentActor

// Input from Command Line
let K = fsi.CommandLineArgs.[1] |> int

//Append the required K zeroes to compare with intial hash bits
let mutable zeroBits= ""

//Create string for header
for i in 1 .. K do 
    zeroBits <- zeroBits + "0"

printfn "********************Mining started!*******************************"
parent <! Input(zeroBits)
system.WhenTerminated.Wait()
printfn "****************** Mining ended!**********************************"


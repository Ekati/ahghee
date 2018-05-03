namespace Ahghee

open Google.Protobuf
open Google.Protobuf.Collections
open Microsoft.AspNetCore.Mvc
open System
open System.Threading
open System.Threading.Tasks

type MemoryStore() =
    let mutable _nodes:seq<Node> = Seq.empty
    interface IStorage with
        member this.Nodes = _nodes
        member x.Flush () = ()
        member this.Add (nodes:seq<Node>) = 
            _nodes <- Seq.append _nodes nodes
            Task.CompletedTask
        member this.Remove (nodeIDs:seq<AddressBlock>) = 
            _nodes <- _nodes |> Seq.filter (fun n -> 
                                                    let head = n.NodeIDs |> Seq.head 
                                                    nodeIDs |> Seq.contains head |> not)
            Task.CompletedTask    
        member this.Items (addresses:seq<AddressBlock>) =
            let matches = addresses |> Seq.map (fun addr -> 
                                                match addr with 
                                                | NodeID(id) -> 
                                                                let isLocal = _nodes 
                                                                              |> Seq.tryFind ( fun n -> n.NodeIDs |> Seq.exists (fun nn -> nn = addr))
                                                                match isLocal with 
                                                                | Some node -> (addr, Left(node))
                                                                | None -> (addr, Right (Failure "remote nodes not supported yet"))
                                                | _ -> raise (new NotImplementedException())
                                                )
            Task.FromResult matches      
        member this.First (predicate: (Node -> bool)) : System.Threading.Tasks.Task<Option<Node>> =
            _nodes
            |> Seq.tryFind predicate  
            |> Task.FromResult 
        member this.Stop() = ()                                              

module Tests

open System
open Xunit
open Ahghee
open System


let Id id = AddressBlock.NodeID { Domain = "biggraph://example.com"; Database="test"; Graph="People"; NodeId=id; RouteKey= None} 
let TestCreateBinary = MimeBytes { MimeBytes.Mime= Some("application/json"); MimeBytes.Bytes = Array.Empty<byte>() } 

[<Fact>]
let ``Can create an InternalIRI type`` () =
    let id = Data.AddressBlock ( Id "1" ) 
    let success = match id with 
        | Data.AddressBlock(AddressBlock.NodeID nodeid) -> true
        | Data.AddressBlock(AddressBlock.MemoryPointer pointer) -> true
        | Data.BinaryBlock(BinaryBlock.MimeBytes data) -> false
        | Data.BinaryBlock(BinaryBlock.MemoryPointer pointer) -> false   
    Assert.True(success)  
    
[<Fact>]
let ``Can create a Binary type`` () =
    let d : Data = BinaryBlock TestCreateBinary
    let success = match d with 
        | Data.AddressBlock(AddressBlock.NodeID nodeId) -> false
        | Data.AddressBlock(AddressBlock.MemoryPointer pointer) -> false
        | Data.BinaryBlock(BinaryBlock.MimeBytes data) -> true
        | Data.BinaryBlock(BinaryBlock.MemoryPointer pointer) -> true
    Assert.True success   

let mimePlainTextUtf8 = Some("text/plain;charset=utf-8")
let BStr (text:string) = BinaryBlock (MimeBytes { Mime = mimePlainTextUtf8 ; Bytes = Text.UTF8Encoding.UTF8.GetBytes(text) })
    
let Prop (key:Data) (values:seq<Data>) =
    let pair =  { KeyValue.Key = key; Value = (values |> Seq.toArray)}   
    pair  
    
let PropStr (key:string) (values:seq<string>) = Prop (BStr key) (values |> Seq.map(fun x -> BStr x))  
let PropStrData (key:string) (values:seq<Data>) = Prop (BStr key) values      
    
[<Fact>]
let ``Can create a Pair`` () =
    let pair = PropStr "firstName" [|"Richard"; "Dick"|]

    let success = match pair.Key with 
                | BinaryBlock (MimeBytes b) when b.Mime.IsSome && b.Mime.Value = mimePlainTextUtf8.Value -> true 
                | _ -> false
    Assert.True success     
    
[<Fact>]
let ``Can create a Node`` () =
    let node = { 
                Node.NodeIDs = [| Id "1" |] 
                Node.Attributes = [|
                                    PropStr "firstName" [|"Richard"; "Dick"|] 
                                  |]
               }

    let empty = node.Attributes |> Seq.isEmpty
    Assert.True (not empty)



let buildGraph : Graph = 
    let node1 = { 
                Node.NodeIDs = [| Id "1" |] 
                Node.Attributes = [|
                                    PropStr "firstName" [|"Richard"; "Dick"|] 
                                    PropStrData "follows" [| Data.AddressBlock (Id "2") |] 
                                  |]
               }
               
    let node2 = { 
                Node.NodeIDs = [| Id "2" |] 
                Node.Attributes = [|
                                    PropStr "firstName" [|"Sam"; "Sammy"|] 
                                    PropStrData "follows" [| Data.AddressBlock (Id "1") |]
                                  |]
               }
               
    let node3 = { 
                Node.NodeIDs = [| Id "3" |] 
                Node.Attributes = [|
                                    PropStr "firstName" [|"Jim"|]
                                    PropStrData "follows" [| Data.AddressBlock (Id "1"); Data.AddressBlock (Id "2") |] 
                                  |]
               }      
               
    let graph:Graph = new Graph(new MemoryStore())
    graph.Add [| node1; node2; node3 |]
    graph
    
[<Fact>]
let ``Can traverse local graph index`` () =
    
    let g = buildGraph
    let nodesWithIncomingEdges = g.Nodes 
                                     |> Seq.collect (fun n -> n.Attributes) 
                                     |> Seq.collect (fun y -> y.Value 
                                                           |> Seq.map (fun x -> match x with  
                                                           | Data.AddressBlock(id) -> Some(id) 
                                                           | _ -> None))   
                                     |> Seq.filter (fun x -> match x with 
                                                             | Some id -> true 
                                                             | _ -> false)
                                     |> Seq.map    (fun x -> x.Value )
                                     |> Seq.distinct
                                     |> g.TryFind 
                                                                           

    Assert.NotEmpty nodesWithIncomingEdges.Result

    

module Tests

open System
open Xunit
open Xunit.Abstractions
open Ahghee
open Ahghee.Utils
open Ahghee.TinkerPop
open System
open System.Collections
open System.IO
open System.Text
open System.Threading.Tasks
open FSharp.Data

  


type MyTests(output:ITestOutputHelper) =
    [<Fact>]
    member __.``Can create an InternalIRI type`` () =
        let id = Data.AddressBlock ( TestId "1" ) 
        let success = match id with 
                        | Data.AddressBlock(AddressBlock.NodeID nodeid) -> true
                        | Data.AddressBlock(AddressBlock.MemoryPointer pointer) -> true
                        | Data.BinaryBlock(BinaryBlock.MimeBytes data) -> false
                        | Data.BinaryBlock(BinaryBlock.MemoryPointer pointer) -> false   
        Assert.True(success)  
        
    [<Fact>]
    member __.``Can create a Binary type`` () =
        let d : Data = BinaryBlock ( MimeBytes { MimeBytes.Mime= mimePlainTextUtf8; MimeBytes.Bytes = Array.Empty<byte>() } )
        let success = match d with 
                        | Data.AddressBlock(AddressBlock.NodeID nodeId) -> false
                        | Data.AddressBlock(AddressBlock.MemoryPointer pointer) -> false
                        | Data.BinaryBlock(BinaryBlock.MimeBytes data) -> true
                        | Data.BinaryBlock(BinaryBlock.MemoryPointer pointer) -> true
        Assert.True success   
    
   
        
    [<Fact>]
    member __.``Can create a Pair`` () =
        let pair = PropString "firstName" [|"Richard"; "Dick"|]
    
        let success = match pair.Key with 
                        | BinaryBlock (MimeBytes b) when b.Mime.IsSome && b.Mime.Value = mimePlainTextUtf8.Value -> true 
                        | _ -> false
        Assert.True success     
        
    [<Fact>]
    member __.``Can create a Node`` () =
        let node = { 
                    Node.NodeIDs = [| TestId "1" |] 
                    Node.Attributes = [|
                                        PropString "firstName" [|"Richard"; "Dick"|] 
                                      |]
                   }
    
        let empty = node.Attributes |> Seq.isEmpty
        Assert.True (not empty)
    
    
    
    member __.buildNodes : seq<Node> = 
        let node1 = { 
                    Node.NodeIDs = [| TestId "1" |] 
                    Node.Attributes = [|
                                        PropString "firstName" [|"Richard"; "Dick"|] 
                                        PropData "follows" [| Data.AddressBlock (TestId "2") |] 
                                      |]
                   }
                   
        let node2 = { 
                    Node.NodeIDs = [| TestId "2" |] 
                    Node.Attributes = [|
                                        PropString "firstName" [|"Sam"; "Sammy"|] 
                                        PropData "follows" [| Data.AddressBlock (TestId "1") |]
                                      |]
                   }
                   
        let node3 = { 
                    Node.NodeIDs = [| TestId "3" |] 
                    Node.Attributes = [|
                                        PropString "firstName" [|"Jim"|]
                                        PropData "follows" [| Data.AddressBlock (TestId "1"); Data.AddressBlock (TestId "2") |] 
                                      |]
                   }      
        [| node1; node2; node3 |]      
        |> Array.toSeq             
    
    
    
    member __.buildGraph : Graph =
        let g:Graph = new Graph(new MemoryStore())
        let nodes = __.buildNodes
        let task = g.Add nodes
        match task.Status with
        | TaskStatus.Created -> task.Start()
        | _ -> ()                                                                     
        task.Wait()
        g

    [<Fact>]
    member __.``Can Add nodes to graph`` () =
        let g:Graph = new Graph(new MemoryStore())
        let nodes = __.buildNodes
        let task = g.Add nodes
        match task.Status with
        | TaskStatus.Created -> task.Start()
        | _ -> ()                                                                     
        task.Wait()
        
        Assert.Equal( task.Status, TaskStatus.RanToCompletion)
        Assert.Equal( task.IsCompletedSuccessfully, true)

    [<Fact>]
    member __.``Can Remove nodes from graph`` () =
        let g:Graph = __.buildGraph

        let n1 = g.Nodes
        let len1 = n1 |> Seq.length
        
        let toRemove = n1 
                        |> Seq.head 
                        |> (fun n -> n.NodeIDs)
        let task = g.Remove(toRemove)
        task.Wait()
        let n2 = g.Nodes
        let len2 = n2 |> Seq.length                        
        
        Assert.NotEqual<Node> (n1, n2)
        output.WriteLine("len1: {0}; len2: {1}", len1, len2)
        (len1 = len2 + 1) |> Assert.True 
    
        
    [<Fact>]
    member __.``Can traverse local graph index`` () =
        
        let g = __.buildGraph
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
    
    [<Fact>] 
    member __.``Can get IDs after load tinkerpop-crew.xml into graph`` () =
         let g:Graph = new Graph(new MemoryStore())
         let nodes = buildNodesTheCrew
         let task = g.Add nodes
         match task.Status with
             | TaskStatus.Created -> task.Start()
             | _ -> ()                                                                     
         task.Wait()
                  
         output.WriteLine("g.Nodes length: {0}", g.Nodes |> Seq.length )
         
         let loadedIds = g.Nodes
                         |> Seq.collect (fun n -> n.NodeIDs)
                         |> Seq.map (fun id -> match id with    
                                               | NodeID(nid) -> Some(nid.NodeId)
                                               | MemoryPointer(mp) -> None)  
                         |> Seq.filter (fun x -> x.IsSome)
                         |> Seq.map (fun x -> x.Value)        
                         
         output.WriteLine("loadedIds: {0}", loadedIds |> String.concat " ")
                                       
         let expectedIds = [| 1;2;3;4;5;6; |] 
                           |> Array.toSeq 
                           |> Seq.map (fun n -> n.ToString())
                           
         Assert.Equal<string>(expectedIds,loadedIds)                             
             
    [<Fact>] 
    member __.``Can get labelV after load tinkerpop-crew.xml into graph`` () =
         let g:Graph = new Graph(new MemoryStore())
         let nodes = buildNodesTheCrew
         let task = g.Add nodes
         match task.Status with
             | TaskStatus.Created -> task.Start()
             | _ -> ()                                                                     
         task.Wait()
                  
         output.WriteLine("g.Nodes length: {0}", g.Nodes |> Seq.length )
         
         let actual = g.Nodes
                         |> Seq.collect (fun n -> n.Attributes 
                                                  |> Seq.filter (fun attr -> match attr.Key with 
                                                                             | BinaryBlock(MimeBytes mb) when mb.Mime = mimePlainTextUtf8 -> 
                                                                               ( "labelV" , Encoding.UTF8.GetString mb.Bytes) |> String.Equals 
                                                                             | _ -> false
                                                                )
                                                  |> Seq.map    (fun attr -> n, attr) 
                                                  |> Seq.collect (fun (n,attr) -> 
                                                                let _id = n.NodeIDs |> Seq.head |> (fun id -> match id with    
                                                                                              | NodeID(nid) -> nid.NodeId
                                                                                              | MemoryPointer(mp) -> "")  
                                                                                              
                                                                let labelV = match attr.Key with 
                                                                             | BinaryBlock(MimeBytes mb) when mb.Mime = mimePlainTextUtf8 ->
                                                                                    Encoding.UTF8.GetString mb.Bytes
                                                                             | _ -> ""
                                                                             
                                                                let values = attr.Value
                                                                            |> Seq.map (fun v -> match v with
                                                                                                 | BinaryBlock(MimeBytes mb) when mb.Mime = mimePlainTextUtf8 ->
                                                                                                      Encoding.UTF8.GetString mb.Bytes
                                                                                                 | _ -> "" 
                                                                                       )                                                                                                           
                                                                values 
                                                                |> Seq.map (fun v -> _id,labelV,v)
                                                             )                                                           
                                         )
                         |> List.ofSeq                                         
              
         output.WriteLine(sprintf "foundData: %A" actual)
                                       
         let expected = [ 
                                "1","labelV","person";
                                "2","labelV","person";
                                "3","labelV","software";
                                "4","labelV","person";
                                "5","labelV","software";
                                "6","labelV","person"; 
                           ]
                           
         Assert.Equal<string * string * string>(expected,actual)

    member __.CollectValues key (graph:Graph) =
        graph.Nodes
             |> Seq.collect (fun n -> n.Attributes 
                                      |> Seq.filter (fun attr -> match attr.Key with 
                                                                 | BinaryBlock(MimeBytes mb) when mb.Mime = mimePlainTextUtf8 -> 
                                                                   ( key , Encoding.UTF8.GetString mb.Bytes) |> String.Equals 
                                                                 | _ -> false
                                                    )
                                      |> Seq.map    (fun attr -> n, attr) 
                                      |> Seq.collect (fun (n,attr) -> 
                                                    let _id = n.NodeIDs |> Seq.head |> (fun id -> match id with    
                                                                                  | NodeID(nid) -> nid.NodeId
                                                                                  | MemoryPointer(mp) -> "")  
                                                                                  
                                                    let labelV = match attr.Key with 
                                                                 | BinaryBlock(MimeBytes mb) when mb.Mime = mimePlainTextUtf8 ->
                                                                        Encoding.UTF8.GetString mb.Bytes
                                                                 | _ -> ""
                                                                 
                                                    let values = attr.Value
                                                                |> Seq.map (fun v -> match v with
                                                                                     | BinaryBlock(MimeBytes mb) ->
                                                                                          mb.Mime, mb.Bytes
                                                                                     | _ -> None,Array.empty<byte> 
                                                                           )                                                                                                           
                                                    values 
                                                    |> Seq.map (fun (at,v) -> _id,labelV,at,v)
                                                 )                                                           
                             )
             |> List.ofSeq
         
    [<Fact>] 
    member __.``After load tinkerpop-crew.xml Age has mime type int and comes out as int`` () =
         let g:Graph = new Graph(new MemoryStore())
         let nodes = buildNodesTheCrew
         let task = g.Add nodes
         match task.Status with
             | TaskStatus.Created -> task.Start()
             | _ -> ()                                                                     
         task.Wait()
                  
         output.WriteLine("g.Nodes length: {0}", g.Nodes |> Seq.length )
         let attrName = "age"
         let actual = __.CollectValues attrName g                                         

         let expected = [ 
                        "1",attrName,mimeXmlInt, BitConverter.GetBytes 29;
                        "2",attrName,mimeXmlInt, BitConverter.GetBytes 27;
                        "4",attrName,mimeXmlInt, BitConverter.GetBytes 32;
                        "6",attrName,mimeXmlInt, BitConverter.GetBytes 35; 
                        ]
         output.WriteLine(sprintf "foundData: %A" actual)
         output.WriteLine(sprintf "expectedData: %A" expected)
         Assert.Equal<string * string * Option<string> * byte[]>(expected,actual)         

//    [<Fact>]
//    member __.``I can use the file api`` () =
//        let f = System.IO.File.Open("/home/austin/foo",FileMode.OpenOrCreate,FileAccess.ReadWrite)
//        f.Seek(10L, SeekOrigin.Begin)
//        let buffer = Array.zeroCreate 100
//        let doit = f.AsyncRead(buffer, 0, 100)
//        let size = doit |> Async.RunSynchronously
//        output.WriteLine("we read out size:{0}: {1}", size , Encoding.UTF8.GetString(buffer))
//        f.Close
//        
//        Assert.True(true)
        
    
      

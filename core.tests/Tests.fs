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


type StorageType =
    | Memory
    | GrpcFile
    

type MyTests(output:ITestOutputHelper) =
    [<Fact>]
    member __.``Can create an InternalIRI type`` () =
        let id = Data.AddressBlock ( ABTestId "1" ) 
        let success = match id with 
                        | Data.AddressBlock(AddressBlock.NodeID nodeid) -> true
                        //| Data.AddressBlock(AddressBlock.MemoryPointer pointer) -> true
                        | Data.BinaryBlock(BinaryBlock.MetaBytes data) -> false
                        | Data.BinaryBlock(BinaryBlock.MemoryPointer pointer) -> false   
        Assert.True(success)  
        
    [<Fact>]
    member __.``Can create a Binary type`` () =
        let d : Data = BinaryBlock ( MetaBytes { MetaBytes.Meta= metaPlainTextUtf8; MetaBytes.Bytes = Array.Empty<byte>() } )
        let success = match d with 
                        | Data.AddressBlock(AddressBlock.NodeID nodeId) -> false
                        //| Data.AddressBlock(AddressBlock.MemoryPointer pointer) -> false
                        | Data.BinaryBlock(BinaryBlock.MetaBytes data) -> true
                        | Data.BinaryBlock(BinaryBlock.MemoryPointer pointer) -> true
        Assert.True success   
    
   
        
    [<Fact>]
    member __.``Can create a Pair`` () =
        let pair = PropString "firstName" [|"Richard"; "Dick"|]
    
        let success = match pair.Key with 
                        | BinaryBlock (MetaBytes b) when b.Meta.IsSome && b.Meta.Value = metaPlainTextUtf8.Value -> true 
                        | _ -> false
        Assert.True success     
        
    [<Fact>]
    member __.``Can create a Node`` () =
        let node = { 
                    Node.NodeIDs = [| ABTestId "1" |] 
                    Node.Attributes = [|
                                        PropString "firstName" [|"Richard"; "Dick"|] 
                                      |]
                   }
    
        let empty = node.Attributes |> Seq.isEmpty
        Assert.True (not empty)
    
    
    
    member __.buildNodes : seq<Node> = 
        let node1 = { 
                    Node.NodeIDs = [| ABTestId "1" |] 
                    Node.Attributes = [|
                                        PropString "firstName" [|"Richard"; "Dick"|] 
                                        PropData "follows" [| DABTestId "2" |] 
                                      |]
                   }
                   
        let node2 = { 
                    Node.NodeIDs = [| ABTestId "2" |] 
                    Node.Attributes = [|
                                        PropString "firstName" [|"Sam"; "Sammy"|] 
                                        PropData "follows" [| DABTestId "1" |]
                                      |]
                   }
                   
        let node3 = { 
                    Node.NodeIDs = [| ABTestId "3" |] 
                    Node.Attributes = [|
                                        PropString "firstName" [|"Jim"|]
                                        PropData "follows" [| DABTestId "1"; DABTestId "2" |] 
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
        
    member __.toyGraph : Graph =
        let g:Graph = new Graph(new MemoryStore())
        let nodes = buildNodesTheCrew
        let task = g.Add nodes
        match task.Status with
             | TaskStatus.Created -> task.Start()
             | _ -> ()                                                                     
        task.Wait()
        g

    [<Theory>]
    [<InlineData("StorageType.Memory")>]
    [<InlineData("StorageType.GrpcFile")>]
    member __.``Can Add nodes to graph`` (storeType) =
        let g:Graph = 
            match storeType with 
            | "StorageType.Memory" ->   new Graph(new MemoryStore())
            | "StorageType.GrpcFile" -> new Graph(new GrpcFileStore({
                                                                    Config.ParitionCount=12; 
                                                                    log = (
                                                                            fun msg -> output.WriteLine msg) 
                                                                    }))
            
        let nodes = __.buildNodes
        let task = g.Add nodes
        output.WriteLine <| sprintf "task is: %A" task.Status
        let result = task.Wait(10000)
        g.Stop()
        output.WriteLine <| sprintf "task is now : %A" task.Status
        Assert.Equal( TaskStatus.RanToCompletion, task.Status)
        Assert.Equal( task.IsCompletedSuccessfully, true)
        ()

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
                                         |> g.Items 
                                                                               
    
        Assert.NotEmpty nodesWithIncomingEdges.Result
    
    [<Fact>] 
    member __.``Can get IDs after load tinkerpop-crew.xml into graph`` () =
         let g:Graph = __.toyGraph
                  
         output.WriteLine("g.Nodes length: {0}", g.Nodes |> Seq.length )
         
         let actual = g.Nodes
                         |> Seq.collect (fun n -> n.NodeIDs)
                         |> Seq.map (fun id -> match id with    
                                               | NodeID(nid) -> Some(nid.NodeId)
                                               //| MemoryPointer(mp) -> None
                                               )  
                         |> Seq.filter (fun x -> x.IsSome)
                         |> Seq.map (fun x -> x.Value)        
                         |> Seq.sort
                         
         output.WriteLine("loadedIds: {0}", actual |> String.concat " ")
                                       
         let expectedIds = seq { 1 .. 12 }
                           |> Seq.map (fun n -> n.ToString())
                           |> Seq.sort 
                           
         Assert.Equal<string>(expectedIds,actual)                             

    member __.CollectValues key (graph:Graph) =
        graph.Nodes
             |> Seq.collect (fun n -> n.Attributes 
                                      |> Seq.filter (fun attr -> match attr.Key with 
                                                                 | BinaryBlock(MetaBytes mb) when mb.Meta = metaPlainTextUtf8 -> 
                                                                   ( key , Encoding.UTF8.GetString mb.Bytes) |> String.Equals 
                                                                 | _ -> false
                                                    )
                                      |> Seq.map    (fun attr -> n, attr) 
                                      |> Seq.map (fun (n,attr) -> 
                                                    let _id = n.NodeIDs |> Seq.head |> (fun id -> match id with    
                                                                                  | NodeID(nid) -> nid.NodeId
                                                                                  //| MemoryPointer(mp) -> ""
                                                                                  )  
                                                                                  
                                                    let labelV = match attr.Key with 
                                                                 | BinaryBlock(MetaBytes mb) when mb.Meta = metaPlainTextUtf8 ->
                                                                        Encoding.UTF8.GetString mb.Bytes
                                                                 | _ -> ""
                                                                                                          
                                                    _id,labelV,attr.Value |> List.ofSeq
                                                 )                                                           
                             )
             |> List.ofSeq
             
    [<Fact>] 
    member __.``Can get labelV after load tinkerpop-crew.xml into graph`` () =
         let g:Graph = __.toyGraph
                  
         output.WriteLine("g.Nodes length: {0}", g.Nodes |> Seq.length )
         
         let attrName = "labelV"
         let actual = __.CollectValues attrName g
                                       
         let expected = [ 
                                "1",attrName, [DBBString "person"]
                                "2",attrName, [DBBString "person"]
                                "3",attrName, [DBBString "software"]
                                "4",attrName, [DBBString "person"]
                                "5",attrName, [DBBString "software"]
                                "6",attrName, [DBBString "person"] 
                           ]
                           
         output.WriteLine(sprintf "foundData: %A" actual)
         output.WriteLine(sprintf "expectedData: %A" expected)                           
         Assert.Equal<string * string * list<Data>>(expected,actual) 
         
    [<Fact>] 
    member __.``After load tinkerpop-crew.xml Age has meta type int and comes out as int`` () =
         let g:Graph = __.toyGraph
                  
         output.WriteLine("g.Nodes length: {0}", g.Nodes |> Seq.length )
         let attrName = "age"
         let actual = __.CollectValues attrName g                                         

         let expected = [ 
                        "1",attrName, [DBBInt 29]
                        "2",attrName, [DBBInt 27]
                        "4",attrName, [DBBInt 32]
                        "6",attrName, [DBBInt 35]
                        ]
         output.WriteLine(sprintf "foundData: %A" actual)
         output.WriteLine(sprintf "expectedData: %A" expected)
         Assert.Equal<string * string * list<Data>>(expected,actual)         

    [<Fact>] 
    member __.``After load tinkerpop-crew.xml Nodes have 'out.knows' Edges`` () =
        let g:Graph = __.toyGraph
              
        let attrName = "out.knows"
        let actual = __.CollectValues attrName g                                         
        
        let expected = [ 
                    "1",attrName, [DABTestId "7"]
                    "1",attrName, [DABTestId "8"]
                    ]
        output.WriteLine(sprintf "foundData: %A" actual)
        output.WriteLine(sprintf "expectedData: %A" expected)
        Assert.Equal<string * string * list<Data>>(expected,actual)
        
    [<Fact>] 
    member __.``After load tinkerpop-crew.xml Nodes have 'out.created' Edges`` () =        
        let g:Graph = __.toyGraph
        let attrName = "out.created"
        let actual = __.CollectValues attrName g                                         
        
        let expected = [ 
                     "1",attrName, [DABTestId "9"]
                     "4",attrName, [DABTestId "10"]
                     "4",attrName, [DABTestId "11"]
                     "6",attrName, [DABTestId "12"]
                     ]
        output.WriteLine(sprintf "foundData: %A" actual)
        output.WriteLine(sprintf "expectedData: %A" expected)
        Assert.Equal<string * string * list<Data>>(expected,actual)
    

    [<Fact>] 
    member __.``After load tinkerpop-crew.xml Nodes have 'in.knows' Edges`` () =
        let g:Graph = __.toyGraph
              
        let attrName = "in.knows"
        let actual = __.CollectValues attrName g                                         
        
        let expected = [ 
                    "2",attrName, [DABTestId "7"]
                    "4",attrName, [DABTestId "8"]
                    ]
        output.WriteLine(sprintf "foundData: %A" actual)
        output.WriteLine(sprintf "expectedData: %A" expected)
        Assert.Equal<string * string * list<Data>>(expected,actual)
        
    [<Fact>] 
    member __.``After load tinkerpop-crew.xml Nodes have 'in.created' Edges`` () =        
        let sortedByNodeIdEdgeId (data: list<string * string * list<Data>>) = 
            data 
            |> List.sortBy (fun (a,b,c) -> a , match (c |> List.head) with 
                                                                      | Data.AddressBlock(AddressBlock.NodeID ab) -> ab.NodeId
                                                                      | _ ->  ""
                                                  )
        let g:Graph = __.toyGraph
        let attrName = "in.created"
        let actual = __.CollectValues attrName g
                    |> sortedByNodeIdEdgeId                                         
        
        let expected = [ 
                         "3",attrName, [DABTestId "9"]
                         "5",attrName, [DABTestId "10"]
                         "3",attrName, [DABTestId "11"]
                         "3",attrName, [DABTestId "12"]
                       ] 
                       |> sortedByNodeIdEdgeId
                     
        output.WriteLine(sprintf "foundData: %A" actual)
        output.WriteLine(sprintf "expectedData: %A" expected)
        Assert.Equal<string * string * list<Data>>(expected,actual)
         
    [<Fact>] 
    member __.``After load tinkerpop-crew.xml has Edge-nodes`` () =        
        let g:Graph = __.toyGraph
        let attrName = "labelE"
        let actual = __.CollectValues attrName g                                       
        
        let expected = [ 
                         "7",attrName, [DBBString "knows"]
                         "8",attrName, [DBBString "knows"]
                         "9",attrName, [DBBString "created"]
                         "10",attrName, [DBBString "created"]
                         "11",attrName, [DBBString "created"]
                         "12",attrName, [DBBString "created"]
                       ] 
                     
        output.WriteLine(sprintf "foundData: %A" actual)
        output.WriteLine(sprintf "expectedData: %A" expected)
        Assert.Equal<string * string * list<Data>>(expected,actual)         
         
    [<Fact>] 
    member __.``After load tinkerpop-crew.xml has node data`` () =        
        let g:Graph = __.toyGraph
        output.WriteLine(sprintf "%A" g.Nodes)
        Assert.NotEmpty(g.Nodes)

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
        
    
      

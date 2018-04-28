module Tests

open System
open Xunit
open Xunit.Abstractions
open Ahghee
open System
open System.IO
open System.Text
open System.Threading.Tasks

let mimePlainTextUtf8 = Some("text/plain;charset=utf-8")
let BStr (text:string) = BinaryBlock (MimeBytes { Mime = mimePlainTextUtf8 ; Bytes = Text.UTF8Encoding.UTF8.GetBytes(text) })
    
let Prop (key:Data) (values:seq<Data>) =
    let pair =  { KeyValue.Key = key; Value = (values |> Seq.toArray)}   
    pair  
    
let PropStr (key:string) (values:seq<string>) = Prop (BStr key) (values |> Seq.map(fun x -> BStr x))  
let PropStrData (key:string) (values:seq<Data>) = Prop (BStr key) values      

type MyTests(output:ITestOutputHelper) =
    
    let Id id = AddressBlock.NodeID { Domain = "biggraph://example.com"; Database="test"; Graph="People"; NodeId=id; RouteKey= None} 
    let TestCreateBinary = MimeBytes { MimeBytes.Mime= Some("application/json"); MimeBytes.Bytes = Array.Empty<byte>() } 
    
    [<Fact>]
    member __.``Can create an InternalIRI type`` () =
        let id = Data.AddressBlock ( Id "1" ) 
        let success = match id with 
            | Data.AddressBlock(AddressBlock.NodeID nodeid) -> true
            | Data.AddressBlock(AddressBlock.MemoryPointer pointer) -> true
            | Data.BinaryBlock(BinaryBlock.MimeBytes data) -> false
            | Data.BinaryBlock(BinaryBlock.MemoryPointer pointer) -> false   
        Assert.True(success)  
        
    [<Fact>]
    member __.``Can create a Binary type`` () =
        let d : Data = BinaryBlock TestCreateBinary
        let success = match d with 
            | Data.AddressBlock(AddressBlock.NodeID nodeId) -> false
            | Data.AddressBlock(AddressBlock.MemoryPointer pointer) -> false
            | Data.BinaryBlock(BinaryBlock.MimeBytes data) -> true
            | Data.BinaryBlock(BinaryBlock.MemoryPointer pointer) -> true
        Assert.True success   
    
   
        
    [<Fact>]
    member __.``Can create a Pair`` () =
        let pair = PropStr "firstName" [|"Richard"; "Dick"|]
    
        let success = match pair.Key with 
                    | BinaryBlock (MimeBytes b) when b.Mime.IsSome && b.Mime.Value = mimePlainTextUtf8.Value -> true 
                    | _ -> false
        Assert.True success     
        
    [<Fact>]
    member __.``Can create a Node`` () =
        let node = { 
                    Node.NodeIDs = [| Id "1" |] 
                    Node.Attributes = [|
                                        PropStr "firstName" [|"Richard"; "Dick"|] 
                                      |]
                   }
    
        let empty = node.Attributes |> Seq.isEmpty
        Assert.True (not empty)
    
    
    
    member __.buildNodes : seq<Node> = 
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
    member __.``I can use the file api`` () =
        let f = System.IO.File.Open("/home/austin/foo",FileMode.OpenOrCreate,FileAccess.ReadWrite)
        f.Seek(10L, SeekOrigin.Begin)
        let buffer = Array.zeroCreate 100
        let doit = f.AsyncRead(buffer, 0, 100)
        let size = doit |> Async.RunSynchronously
        output.WriteLine("we read out size:{0}: {1}", size , Encoding.UTF8.GetString(buffer))
        f.Close
        
        Assert.True(true)
        
    
      

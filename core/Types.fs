namespace Ahghee

open Google.Protobuf
open Google.Protobuf.Collections
open Microsoft.AspNetCore.Mvc
open System
open System.Threading.Tasks

// Node Block
// |----------
// | Ids : Seq<AddressBlock>
// | Attrs: Seq<KeyValue>
// |----------

// KeyValue
// |---------
// | Key: Data 
// | Value: Seq<Data>
// |---------

// Data: (AddressBlock | BinaryBlock)

// Address Block
// |-----------
// | (NodeID | MemoryPointer )
// |-----------

// Binary Block
// |----------
// | (MetaBytes | MemoryPointer )
// |----------

type MemoryPointer = { 
    PartitionKey: string
    FileName: string; 
    offset: int64; 
    length: int64 
    }
           
type MetaBytes = { 
    Meta: Option<string>; 
    Bytes : Byte[] 
    }

type NodeID = { 
    Graph: string 
    NodeId: string 
    Pointer: Option<MemoryPointer>
    } 
    
type GlobalNodeID = { 
    Domain: string
    Database: string 
    NodeId: NodeID
    }     
     
type BinaryBlock =
    | MetaBytes of MetaBytes
    | MemoryPointer of MemoryPointer

type AddressBlock =
    | NodeID of NodeID
    | GlobalNodeID of GlobalNodeID

type Data =
  | AddressBlock of AddressBlock
  | BinaryBlock of BinaryBlock
  
type KeyValue = { Key: Data; Value : seq<Data> }
type Node = { NodeIDs: seq<AddressBlock>; Attributes: seq<KeyValue> }

type Either<'L, 'R> =
    | Left of 'L
    | Right of 'R

type IStorage =
    abstract member Nodes: seq<Node>
    abstract member Flush: unit -> unit
    abstract member Add: seq<Node> -> System.Threading.Tasks.Task
    abstract member Remove: seq<AddressBlock> -> System.Threading.Tasks.Task
    abstract member Items: seq<AddressBlock> -> System.Threading.Tasks.Task<seq<AddressBlock * Either<Node, Exception>>>
    abstract member First: (Node -> bool) -> System.Threading.Tasks.Task<Option<Node>> 

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

type Config = {
    ParitionCount:int
    log: string -> unit
    }
    


type GrpcFileStore(config:Config) = 

    let rec ChoosePartition (ab:AddressBlock) =
        let hash = match ab with
                    | NodeID nid -> nid.Graph.GetHashCode() * 31 + nid.NodeId.GetHashCode()
                    | GlobalNodeID gnid -> ChoosePartition (NodeID gnid.NodeId)
        Math.Abs(hash) % config.ParitionCount                    

    let ChooseNodePartition (n:Node) =
        n.NodeIDs 
            |> Seq.map (fun x -> ChoosePartition x) 
            |> Seq.head

    let NullPointer = 
        let p = new Grpc.MemoryPointer()
        p.Filename <- ""
        p.Partitionkey <- ""
        p.Offset <- 0L
        p.Length <- 0L
        p
        
    let ToGrpcNodeId nodeid =
        let gid = new Grpc.NodeID()
        gid.Graph <- nodeid.Graph
        gid.Nodeid <- nodeid.NodeId
        gid.Pointer <- NullPointer
        gid
    
    let ToGrpcGlobalNodeId nodeid =
        let gnodeid = new Grpc.GlobalNodeID()
        gnodeid.Domain <- nodeid.Domain
        gnodeid.Database <- nodeid.Database
        gnodeid.Nodeid <- ToGrpcNodeId nodeid.NodeId
        gnodeid       
    
    let ToGrpcAddressBlock ab =
        let abbb = new Grpc.AddressBlock()
        match ab with 
          | NodeID(a) -> 
                abbb.Nodeid <- ToGrpcNodeId a
                abbb
          | GlobalNodeID(b) -> 
                abbb.Globalnodeid <- ToGrpcGlobalNodeId b
                abbb
    
    let ToGrpcMetaBytes (mb:MetaBytes) =
        let metaBytes = new Grpc.MetaBytes()
        metaBytes.Meta <- (mb.Meta |> Option.defaultValue "")
        metaBytes.Bytes <- ByteString.CopyFrom(mb.Bytes)
        metaBytes
    
    let ToGrpcMemoryPointer (mp:MemoryPointer) =
        let memoryPointer = new Grpc.MemoryPointer()
        memoryPointer.Partitionkey <- mp.PartitionKey
        memoryPointer.Filename <- mp.FileName
        memoryPointer.Offset <- mp.offset
        memoryPointer.Length <- mp.length
        memoryPointer
    
    let ToGrpcDataBlock (data:Data) =
        let db = new Grpc.DataBlock()
        match data with 
        | AddressBlock (NodeID n) -> 
            db.Address <- new Grpc.AddressBlock()
            db.Address.Nodeid <- ToGrpcNodeId n
            db
        | AddressBlock (GlobalNodeID n) -> 
            db.Address <- new Grpc.AddressBlock()
            db.Address.Globalnodeid <- ToGrpcGlobalNodeId n
            db              
        | BinaryBlock (MetaBytes mb) ->
            db.Binary <- new Grpc.BinaryBlock()
            db.Binary.Metabytes <- ToGrpcMetaBytes mb
            db
        | BinaryBlock (MemoryPointer mp) ->
            db.Binary <- new Grpc.BinaryBlock()
            db.Binary.Memorypointer <- ToGrpcMemoryPointer mp
            db   
    
    let ToGrpcKeyValue (kv:KeyValue ) =
        let gkv = new Grpc.KeyValue()
        gkv.Key <- ToGrpcDataBlock kv.Key
        gkv.Value.AddRange ( kv.Value
                                |> Seq.map (fun x -> ToGrpcDataBlock x)
                                )
        gkv                                
    
    // TODO: Switch to PebblesDB when index gets to big
    let ``Index of NodeID -> MemoryPointer`` = new System.Collections.Concurrent.ConcurrentDictionary<Grpc.NodeID,Grpc.MemoryPointer>()
    let ``Index of NodeID without MemoryPointer -> NodeId that need them`` = new System.Collections.Concurrent.ConcurrentDictionary<Grpc.NodeID,list<Grpc.NodeID>>()
    
    let IndexMaintainer =
        MailboxProcessor<Grpc.NodeID * Grpc.MemoryPointer >.Start(fun inbox ->
            let rec messageLoop() = 
                async{
                    let! (sn,mp) = inbox.Receive()
                    ``Index of NodeID -> MemoryPointer``.AddOrUpdate(sn, mp, (fun x y -> mp)) |> ignore
                    return! messageLoop()
                }
            messageLoop()    
        )
        
    let PartitionWriters = 
        seq {0 .. (config.ParitionCount - 1)}
        |>  Seq.map (fun i -> 
            let bc = new System.Collections.Concurrent.BlockingCollection<Node>()
            let a = Task.Factory.StartNew((fun () -> 
                
                let fileName = sprintf "/home/austin/git/ahghee/data/ahghee.%i.tmp" i
                
                let stream = new IO.FileStream(fileName,IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite,IO.FileShare.Read,1024,IO.FileOptions.Asynchronous ||| IO.FileOptions.RandomAccess)
                let posEnd = stream.Seek (0L, IO.SeekOrigin.End)
                let out = new CodedOutputStream(stream)
                for item in bc.GetConsumingEnumerable() do 
                    try
                        config.log <| sprintf "GotSome[%A]: %A" i item
                        let offset = out.Position
                        let mp = Ahghee.Grpc.MemoryPointer()
                        mp.Partitionkey <- i.ToString() 
                        mp.Filename <- fileName
                        mp.Offset <- offset
                        let sn = new Grpc.Node()
                        sn.Ids.AddRange (item.NodeIDs
                                            |> Seq.map (fun ab -> ToGrpcAddressBlock ab)
                                            ) 
                            
                        sn.Attributes.AddRange (item.Attributes
                                                    |> Seq.map (fun kv -> ToGrpcKeyValue kv)  
                                                    )
                        mp.Length <- (sn.CalculateSize() |> int64)
                        sn.WriteTo out
                        
                        // todo: Don't always flush.. might not need to manually flush ever except for testing. Or maybe on shutdown.
                        out.Flush()
                        config.log <| sprintf "Finished[%A]: %A" i item
                    with 
                    | :? Exception as ex -> config.log <| sprintf "ERROR[%A]: %A" i ex
                    
                ()),TaskCreationOptions.LongRunning)
            (bc, a)
            )            
        |> Array.ofSeq                 
        
    interface IStorage with
        member x.Nodes = raise (new NotImplementedException())
        member x.Flush () = 
            // wait for the PartitionWriters
            for (p,a) in PartitionWriters do
                while p.Count > 0 do 
                    System.Threading.Thread.Sleep(10)
                
            // wait for the indexMaintainers
            while IndexMaintainer.CurrentQueueLength > 0 do
                System.Threading.Thread.Sleep(10)
            ()
            
        member this.Add (nodes:seq<Node>) = 
            for n in nodes do
                let partition = ChooseNodePartition n
                let (bc,a) = PartitionWriters.[partition] 
                bc.Add n
            Task.CompletedTask    
                        
        member x.Remove (nodes:seq<AddressBlock>) = raise (new NotImplementedException())
        member x.Items (addressBlock:seq<AddressBlock>) = raise (new NotImplementedException())
        member x.First (predicate: (Node -> bool)) = raise (new NotImplementedException())
 
type Graph(storage:IStorage) =  
    member x.Nodes = storage.Nodes
    member x.Flush () = storage.Flush()
    member x.Add (nodes:seq<Node>) = storage.Add nodes
    member x.Remove (nodes:seq<AddressBlock>) = storage.Remove nodes
    member x.Items (addressBlock:seq<AddressBlock>) = storage.Items addressBlock
    member x.First (predicate: (Node -> bool)) : System.Threading.Tasks.Task<Option<Node>> = storage.First predicate

module Utils =
    let metaPlainTextUtf8 = Some("xs:string")
    let metaXmlInt = Some("xs:int")
    let metaXmlDouble = Some("xs:double")
    
    let ABTestId id = AddressBlock.NodeID { Graph="People"; NodeId=id; Pointer=None;}
    let BBString (text:string) = BinaryBlock.MetaBytes { Meta = metaPlainTextUtf8 ; Bytes = Text.UTF8Encoding.UTF8.GetBytes(text) }
    let BBInt (value:int) = BinaryBlock.MetaBytes { Meta = metaXmlInt ; Bytes = BitConverter.GetBytes value }
    let BBDouble (value:double) = BinaryBlock.MetaBytes { Meta = metaXmlDouble ; Bytes = BitConverter.GetBytes value }
    
    let DABTestId id = Data.AddressBlock (ABTestId id)    
    let DBBString (text:string) = Data.BinaryBlock (BBString text)
    let DBBInt (value:int) = Data.BinaryBlock (BBInt value)
    let DBBDouble (value:double) = Data.BinaryBlock (BBDouble value)
    let Prop (key:Data) (values:seq<Data>) =
        let pair =  { KeyValue.Key = key; Value = (values |> Seq.toArray)}   
        pair  
        
    let PropString (key:string) (values:seq<string>) = Prop (DBBString key) (values |> Seq.map(fun x -> DBBString x))  
    let PropInt (key:string) (values:seq<int>) = Prop (DBBString key) (values |> Seq.map(fun x -> DBBInt x))
    let PropDouble (key:string) (values:seq<double>) = Prop (DBBString key) (values |> Seq.map(fun x -> DBBDouble x))
    let PropData (key:string) (values:seq<Data>) = Prop (DBBString key) values
    
module TinkerPop =
    open Utils
    open FSharp.Data
    open System.Text
    
    type GraphML = XmlProvider<"""https://raw.githubusercontent.com/apache/tinkerpop/master/data/tinkerpop-modern.xml""">
    let TheCrew = lazy ( GraphML.Load("https://raw.githubusercontent.com/apache/tinkerpop/master/data/tinkerpop-modern.xml") )

    
    let xsType graphMlType : Option<string> =
        match graphMlType with
        | "string" -> metaPlainTextUtf8
        | "int" -> metaXmlInt
        | "double" -> metaXmlDouble
        | _ -> None
        
    let buildNodesTheCrew : seq<Node> =
        let attrs forType= 
            TheCrew.Value.Keys 
            |> Seq.ofArray
            |> Seq.filter (fun k -> k.For = forType)
            |> Seq.map (fun k -> k.Id, (k.AttrName, k.AttrType))
            |> Map.ofSeq
        let NodeAttrs = attrs "node" 
        let EdgeAttrs = attrs "edge" 
        
        let Id id = AddressBlock.NodeID { Graph="TheCrew"; NodeId=id; Pointer=None;}
        
        let buildNodesFromGraphMlNodes (nodes:seq<GraphML.Node>) (edges:seq<GraphML.Edge>) = 
            nodes
            |> Seq.map (fun n -> 
                     { 
                        Node.NodeIDs = [| Id (n.Id.ToString()) |];  
                        Node.Attributes = 
                            n.Datas
                            |> Seq.ofArray
                            |> Seq.map (fun d ->
                                let keyBytes = 
                                    let (name, typ) = NodeAttrs.Item d.Key
                                    Encoding.UTF8.GetBytes name
                                
                                let valueMeta =
                                    let (name, typ) = NodeAttrs.Item d.Key 
                                    xsType typ
                                
                                let valueBytes =
                                    match valueMeta with
                                    | m when m = metaPlainTextUtf8 -> match d.String with  
                                                                       | Some(s) -> Encoding.UTF8.GetBytes s
                                                                       | _ -> Array.empty<byte>
                                    | m when m = metaXmlDouble -> match d.String with  
                                                                   | Some(s) -> BitConverter.GetBytes (double s)
                                                                   | _ -> Array.empty<byte>
                                    | m when m = metaXmlInt -> match d.String with  
                                                                | Some(s) -> BitConverter.GetBytes (int32 s)
                                                                | _ -> Array.empty<byte>
                                    | _ -> Array.empty<byte>                                                                                    
                                
                                { 
                                    KeyValue.Key= BinaryBlock (MetaBytes {
                                                                        MetaBytes.Meta= metaPlainTextUtf8;
                                                                        Bytes= keyBytes
                                                                             })
                                    Value= [BinaryBlock (MetaBytes {
                                                                    MetaBytes.Meta= valueMeta
                                                                    Bytes= valueBytes})]
                                }
                            )
                            |> Seq.append (edges
                                             |> Seq.filter (fun e -> e.Source = n.Id)
                                             |> Seq.map (fun e -> 
                                                            {
                                                                KeyValue.Key = DBBString (e.Datas 
                                                                                         |> Seq.find (fun d -> d.Key = "labelE")
                                                                                         |> (fun d -> "out." + d.String.Value)
                                                                                         );
                                                                Value = [DABTestId (e.Id.ToString())]
                                                            }                         
                                                         )
                                             ) 
                            |> Seq.append (edges
                                             |> Seq.filter (fun e -> e.Target = n.Id)
                                             |> Seq.map (fun e -> 
                                                            {
                                                                KeyValue.Key = DBBString (e.Datas 
                                                                                         |> Seq.find (fun d -> d.Key = "labelE")
                                                                                         |> (fun d -> "in." + d.String.Value)
                                                                                         );
                                                                Value = [DABTestId (e.Id.ToString())]
                                                            }                         
                                                         )
                                             )                  
                     }
                )
                
        let buildEdgeNodesFromGraphMlEdges (edges:seq<GraphML.Edge>) = 
            edges
            |> Seq.map (fun n -> 
                     { 
                        Node.NodeIDs = [| Id (n.Id.ToString()) |];  
                        Node.Attributes = 
                            n.Datas
                            |> Seq.ofArray
                            |> Seq.map (fun d ->
                                let keyBytes = 
                                    let (name, typ) = EdgeAttrs.Item d.Key
                                    Encoding.UTF8.GetBytes name
                                
                                let valueMeta =
                                    let (name, typ) = EdgeAttrs.Item d.Key 
                                    xsType typ
                                
                                let valueBytes =
                                    match valueMeta with
                                    | m when m = metaPlainTextUtf8 -> match d.String with  
                                                                       | Some(s) -> Encoding.UTF8.GetBytes s
                                                                       | _ -> Array.empty<byte>
                                    | m when m = metaXmlDouble -> match d.String with  
                                                                   | Some(s) -> BitConverter.GetBytes (double s)
                                                                   | _ -> Array.empty<byte>
                                    | m when m = metaXmlInt -> match d.String with  
                                                                | Some(s) -> BitConverter.GetBytes (int32 s)
                                                                | _ -> Array.empty<byte>
                                    | _ -> Array.empty<byte>                                                                                    
                                
                                { 
                                    KeyValue.Key= BinaryBlock (MetaBytes {
                                                                        MetaBytes.Meta= metaPlainTextUtf8;
                                                                        Bytes= keyBytes
                                                                             })
                                    Value= [BinaryBlock (MetaBytes {
                                                                    MetaBytes.Meta= valueMeta
                                                                    Bytes= valueBytes})]
                                }
                            )
                            |> Seq.append (edges
                                             |> Seq.filter (fun e -> e.Source = n.Id)
                                             |> Seq.map (fun e -> 
                                                            {
                                                                KeyValue.Key = DBBString (e.Datas 
                                                                                         |> Seq.find (fun d -> d.Key = "labelE")
                                                                                         |> (fun d -> "out." + d.String.Value)
                                                                                         );
                                                                Value = [DABTestId (e.Id.ToString())]
                                                            }                         
                                                         )
                                             ) 
                            |> Seq.append (edges
                                             |> Seq.filter (fun e -> e.Target = n.Id)
                                             |> Seq.map (fun e -> 
                                                            {
                                                                KeyValue.Key = DBBString (e.Datas 
                                                                                         |> Seq.find (fun d -> d.Key = "labelE")
                                                                                         |> (fun d -> "in." + d.String.Value)
                                                                                         );
                                                                Value = [DABTestId (e.Id.ToString())]
                                                            }                         
                                                         )
                                             )
                            |> Seq.append ( [ 
                                                { KeyValue.Key= DBBString "source"; Value= [DABTestId (n.Source.ToString())]}
                                                { KeyValue.Key= DBBString "target"; Value= [DABTestId (n.Target.ToString())]}
                                            ] )          
                     }
                )
        
        buildNodesFromGraphMlNodes TheCrew.Value.Graph.Nodes TheCrew.Value.Graph.Edges
        |> Seq.append (buildEdgeNodesFromGraphMlEdges TheCrew.Value.Graph.Edges)
        
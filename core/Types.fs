namespace Ahghee

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
// | (MimeBytes | MemoryPointer )
// |----------

type MemoryPointer = { 
    FileName: string; 
    offset: int64; 
    length: int64 
    }
           
type MimeBytes = { 
    Mime: Option<string>; 
    Bytes : Byte[] 
    }

type NodeID = { 
    Domain: string; 
    Database: string; 
    Graph: string; 
    NodeId: string; 
    RouteKey: Option<string>
    } 
     
type BinaryBlock =
    | MimeBytes of MimeBytes
    | MemoryPointer of MemoryPointer

type AddressBlock =
    | NodeID of NodeID
    | MemoryPointer of MemoryPointer

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
    abstract member Add: seq<Node> -> System.Threading.Tasks.Task
    abstract member Remove: seq<AddressBlock> -> System.Threading.Tasks.Task
    abstract member Items: seq<AddressBlock> -> System.Threading.Tasks.Task<seq<AddressBlock * Either<Node, Exception>>>
    abstract member First: (Node -> bool) -> System.Threading.Tasks.Task<Option<Node>> 

type MemoryStore() =
    let mutable _nodes:seq<Node> = Seq.empty
    interface IStorage with
        member this.Nodes = _nodes
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
                                                | MemoryPointer(pointer) -> (addr, Right (Failure  "MemoryPointer not supported yet")) 
                                                )
            Task.FromResult matches      
        member this.First (predicate: (Node -> bool)) : System.Threading.Tasks.Task<Option<Node>> =
            _nodes
            |> Seq.tryFind predicate  
            |> Task.FromResult                                           
 
type Graph(storage:IStorage) =  
    member x.Nodes = storage.Nodes
    member x.Add (nodes:seq<Node>) = storage.Add nodes
    member x.Remove (nodes:seq<AddressBlock>) = storage.Remove nodes
    member x.Items (addressBlock:seq<AddressBlock>) = storage.Items addressBlock
    member x.First (predicate: (Node -> bool)) : System.Threading.Tasks.Task<Option<Node>> = storage.First predicate

module Utils =
    let mimePlainTextUtf8 = Some("xs:string")
    let mimeXmlInt = Some("xs:int")
    let mimeXmlDouble = Some("xs:double")
    
    let ABTestId id = AddressBlock.NodeID { Domain = "biggraph://example.com"; Database="test"; Graph="People"; NodeId=id; RouteKey= None}
    let BBString (text:string) = BinaryBlock.MimeBytes { Mime = mimePlainTextUtf8 ; Bytes = Text.UTF8Encoding.UTF8.GetBytes(text) }
    let BBInt (value:int) = BinaryBlock.MimeBytes { Mime = mimeXmlInt ; Bytes = BitConverter.GetBytes value }
    let BBDouble (value:double) = BinaryBlock.MimeBytes { Mime = mimeXmlDouble ; Bytes = BitConverter.GetBytes value }
    
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
        | "string" -> mimePlainTextUtf8
        | "int" -> mimeXmlInt
        | "double" -> mimeXmlDouble
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
        
        let Id id = AddressBlock.NodeID { Domain = "biggraph://ahghee.com"; Database="TinkerPop"; Graph="TheCrew"; NodeId=id; RouteKey= None}
        
        TheCrew.Value.Graph.Nodes 
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
                        
                        let valueMime =
                            let (name, typ) = NodeAttrs.Item d.Key 
                            xsType typ
                        
                        let valueBytes =
                            match valueMime with
                            | m when m = mimePlainTextUtf8 -> match d.String with  
                                                   | Some(s) -> Encoding.UTF8.GetBytes s
                                                   | _ -> Array.empty<byte>
                            | m when m = mimeXmlDouble -> match d.String with  
                                               | Some(s) -> BitConverter.GetBytes (double s)
                                               | _ -> Array.empty<byte>
                            | m when m = mimeXmlInt -> match d.String with  
                                            | Some(s) -> BitConverter.GetBytes (int32 s)
                                            | _ -> Array.empty<byte>
                            | _ -> Array.empty<byte>                                                                                    
                        
                        { 
                            KeyValue.Key= BinaryBlock (MimeBytes {
                                                                MimeBytes.Mime= mimePlainTextUtf8;
                                                                Bytes= keyBytes
                                                                     })
                            Value= [BinaryBlock (MimeBytes {
                                                            MimeBytes.Mime= valueMime
                                                            Bytes= valueBytes})]
                        }
                    )
                    |> Seq.append (TheCrew.Value.Graph.Edges
                                     |> Seq.ofArray
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
                    |> Seq.append (TheCrew.Value.Graph.Edges
                                     |> Seq.ofArray
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
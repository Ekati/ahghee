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
    abstract member TryFind: seq<AddressBlock> -> System.Threading.Tasks.Task<seq<AddressBlock * Either<Node, Exception>>>

type MemoryStore() =
    let mutable _nodes:seq<Node> = Seq.empty
    interface IStorage with
        member this.Nodes = _nodes
        member this.Add (nodes:seq<Node>) = 
            _nodes <- Seq.append _nodes nodes
            Task.CompletedTask
        member this.TryFind (addresses:seq<AddressBlock>) =
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
 
type Graph(storage:IStorage) =  
    member x.Nodes = storage.Nodes
    member x.Add (nodes:seq<Node>) = storage.Add nodes
    member x.TryFind (addressBlock:seq<AddressBlock>) = storage.TryFind addressBlock                       

namespace Ahghee

open System

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
 
type Graph() =  
    let mutable _nodes:seq<Node> = Seq.empty 
    member x.Nodes = _nodes
    member x.Add (nodes:seq<Node>) = _nodes <- Seq.append _nodes nodes
    member x.TryFind (addressBlock:AddressBlock):Option<Node> = 
        let isLocal = _nodes 
                      |> Seq.tryFind ( fun n -> n.NodeIDs |> Seq.exists (fun nn -> nn = addressBlock))
        match isLocal with 
        | Some node -> Some(node)
        | None -> failwith "remote nodes not supported yet"                        

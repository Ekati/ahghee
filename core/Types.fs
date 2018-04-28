namespace Ahghee

open System


type MimeBytes = { Mime: Option<string>; Bytes : Byte[] } 
type NodeIRI = { Domain: string; Database: string; Graph: string; NodeId: string; RouteKey: Option<string>} 
type Data =
  | InternalIRI of NodeIRI
  | ExternalIRI of System.Uri
  | Binary of MimeBytes
type Pair = { Key: Data; Value : seq<Data> }
type Node = { NodeIds: seq<NodeIRI>; Attributes: seq<Pair> }
 
type Graph() =  
    let mutable _nodes:seq<Node> = Seq.empty 
    member x.Nodes = _nodes
    member x.Add (nodes:seq<Node>) = _nodes <- Seq.append _nodes nodes
    member x.TryFind (nodeIri:NodeIRI):Option<Node> = 
        let isLocal = _nodes 
                      |> Seq.tryFind ( fun n -> n.NodeIds |> Seq.exists (fun nn -> nn = nodeIri))
        match isLocal with 
        | Some node -> Some(node)
        | None -> failwith "remote nodes not supported yet"                        

namespace Ahghee

open System

type NodeIRI = { Domain: string; Database: string; Graph: string; NodeId: string; RouteKey: Option<string> }
type MimeBytes = { Mime: Option<string>; Bytes : Byte[] }

type Data =
  | InternalIRI of NodeIRI
  | ExternalIRI of System.Uri
  | Binary of MimeBytes

type Pair = { Key: Data; Value : Data[] }
type Node = { NodeIds: NodeIRI[]; Attributes: Pair[] }
namespace Ahghee

open System

type NodeIRI = { Root: string; Database: string; NodeId: string; RouteKey: Option<string> }

type IRI =
  | InternalIRI of NodeIRI
  | ExternalIRI of System.Uri

type Data = { Mimi: Option<string>; Bytes : Byte[] }

type PairKey =
  | IRI
  | Data

type PairValue =
  | IRI
  | Data

type Pair = { Key: PairKey; Value : PairValue[] }
type Node = { NodeIds: NodeIRI[]; Attributes: Pair[] }
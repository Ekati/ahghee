module Tests

open System
open Xunit
open Ahghee
open System


let TestCreateInternalIRI = { NodeIRI.Domain = "biggraph://example.com"; NodeIRI.Database="test"; NodeIRI.Graph="People"; NodeIRI.NodeId="1"; NodeIRI.RouteKey= None} 
let TestCreateExternalIRI = ExternalIRI (System.Uri( "https://ahghee.com" )) 
let TestCreateBinary = Binary { MimeBytes.Mime= Some("application/json"); MimeBytes.Bytes = Array.Empty<byte>() } 

[<Fact>]
let ``Can create an InternalIRI type`` () =
    let d : Data = InternalIRI TestCreateInternalIRI 
    let success = match d with 
        | InternalIRI (nodeIRI) -> true
        | ExternalIRI (external) -> false
        | Binary (data) -> false   
    Assert.True(success)  
    
[<Fact>]
let ``Can create an ExternalIRI type`` () =
    let d : Data = TestCreateExternalIRI
    
    let success = match d with 
        | InternalIRI (nodeIRI) -> false
        | ExternalIRI (external) -> true
        | Binary (data) -> false
    Assert.True success          

[<Fact>]
let ``Can create a Binary type`` () =
    let d : Data = TestCreateBinary
    let success = match d with 
        | InternalIRI (nodeIRI) -> false
        | ExternalIRI (external) -> false
        | Binary (data) -> true
    Assert.True success   

let mimePlainTextUtf8 = Some("text/plain;charset=utf-8")
let BinaryText (text:string) = Binary { Mime = mimePlainTextUtf8 ; Bytes = Text.UTF8Encoding.UTF8.GetBytes(text) }
    
    
let TestCreateStringPairs key (values:seq<string>) = 
    let key = BinaryText key
    let v = values |> Seq.map (fun item -> BinaryText item) |> Seq.toArray
    let pair = { Pair.Key = key; Value = v}   
    pair     
    
[<Fact>]
let ``Can create a Pair`` () =
    let pair = TestCreateStringPairs "firstName" [|"Richard"; "Dick"|]

    let success = match pair.Key with 
                | Binary (b) when b.Mime.IsSome && b.Mime.Value = mimePlainTextUtf8.Value -> true 
                | _ -> false
    Assert.True success     
    
[<Fact>]
let ``Can create a Node`` () =

    let pair = TestCreateStringPairs "firstName" [|"Richard"; "Dick"|] 

    let nodeId = TestCreateInternalIRI
    let node = { Node.NodeIds = [|nodeId|]; Node.Attributes = [|pair|]}

    let hasSome = node.Attributes.Length > 0
    Assert.True hasSome

module Tests

open System
open Xunit
open Ahghee
open System

[<Fact>]
let ``Can create an InternalIRI type`` () =
    let d : Data = InternalIRI { NodeIRI.Domain = "biggraph://example.com"; NodeIRI.Database="test"; NodeIRI.Graph="People"; NodeIRI.NodeId="1"; NodeIRI.RouteKey= None}
    let success = match d with 
        | InternalIRI (nodeIRI) -> true
        | ExternalIRI (external) -> false
        | Binary (data) -> false   
    Assert.True(success)  
    
[<Fact>]
let ``Can create an ExternalIRI type`` () =
    let d : Data = ExternalIRI (System.Uri( "https://ahghee.com" ))
    
    let success = match d with 
        | InternalIRI (nodeIRI) -> false
        | ExternalIRI (external) -> true
        | Binary (data) -> false
    Assert.True success          

[<Fact>]
let ``Can create a Binary type`` () =
    let d : Data = Binary { MimeBytes.Mime= Some("application/json"); MimeBytes.Bytes = Array.Empty<byte>() }
    let success = match d with 
        | InternalIRI (nodeIRI) -> false
        | ExternalIRI (external) -> false
        | Binary (data) -> true
    Assert.True success   
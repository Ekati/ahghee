module Tests

open System
open Xunit
open Ahghee

[<Fact>]
let ``Can create a NodeIRI`` () =
    let n = { NodeIRI.Root = "biggraph://example.com"; NodeIRI.Database="test"; NodeIRI.NodeId="1"; NodeIRI.RouteKey= None}
    Assert.NotNull n.ToString
    Assert.True(true)

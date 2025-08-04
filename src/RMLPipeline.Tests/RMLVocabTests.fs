namespace RMLPipeline.Tests

    module VocabSuite =

        open Expecto
        open RMLPipeline
        open RMLPipeline.Model
        open RMLPipeline.DSL
        
        
        [<Tests>]
        let expressionMapTests =
            testList "ExpressionMap Computation Tests" [
                testCase "can create template expression map" <| fun _ ->
                    let exprMapComp = exprMap {
                        do! template "http://example.org/person/{id}"
                    }
                    let result = buildExpressionMap exprMapComp
                    
                    Expect.equal result.Template (Some "http://example.org/person/{id}") "Template should be set"
                    Expect.isNone result.Reference "Reference should be None"
                    Expect.isNone result.Constant "Constant should be None"

                testCase "can create reference expression map" <| fun _ ->
                    let exprMapComp = exprMap {
                        do! reference "name"
                    }
                    let result = buildExpressionMap exprMapComp
                    
                    Expect.equal result.Reference (Some "name") "Reference should be set"
                    Expect.isNone result.Template "Template should be None"
                    Expect.isNone result.Constant "Constant should be None"

                testCase "can create constant URI expression map" <| fun _ ->
                    let exprMapComp = exprMap {
                        do! constantURI "http://example.org/john"
                    }
                    let result = buildExpressionMap exprMapComp
                    
                    Expect.equal result.Constant (Some (URI "http://example.org/john")) "Constant URI should be set"
                    Expect.isNone result.Template "Template should be None"
                    Expect.isNone result.Reference "Reference should be None"

                testCase "can create constant literal expression map" <| fun _ ->
                    let exprMapComp = exprMap {
                        do! constantLiteral "John Doe"
                    }
                    let result = buildExpressionMap exprMapComp
                    
                    Expect.equal result.Constant (Some (Literal "John Doe")) "Constant literal should be set"

                testCase "can create constant blank node expression map" <| fun _ ->
                    let exprMapComp = exprMap {
                        do! constantBlankNode "person1"
                    }
                    let result = buildExpressionMap exprMapComp
                    
                    Expect.equal result.Constant (Some (BlankNode "person1")) "Constant blank node should be set"
            ]

        [<Tests>]
        let termMapTests =
            testList "TermMap Computation Tests" [
                testCase "can create IRI term map with template" <| fun _ ->
                    let termMapComp = termMap {
                        do! expressionMap (exprMap {
                            do! template "http://example.org/person/{id}"
                        })
                        do! asIRI
                    }
                    let result = buildTermMap termMapComp
                    
                    Expect.equal result.ExpressionMap.Template (Some "http://example.org/person/{id}") "Template should be set"
                    Expect.equal result.TermType (Some IRITerm) "Term type should be IRI"

                testCase "can create literal term map with reference" <| fun _ ->
                    let termMapComp = termMap {
                        do! expressionMap (exprMap {
                            do! reference "name"
                        })
                        do! asLiteral
                    }
                    let result = buildTermMap termMapComp
                    
                    Expect.equal result.ExpressionMap.Reference (Some "name") "Reference should be set"
                    Expect.equal result.TermType (Some LiteralTerm) "Term type should be Literal"

                testCase "can use helper function refTermAsIRI" <| fun _ ->
                    let termMapComp = refTermAsIRI "id"
                    let result = buildTermMap termMapComp
                    
                    Expect.equal result.ExpressionMap.Reference (Some "id") "Reference should be set"
                    Expect.equal result.TermType (Some IRITerm) "Term type should be IRI"

                testCase "can use helper function templateTermAsIRI" <| fun _ ->
                    let termMapComp = templateTermAsIRI "http://example.org/{id}"
                    let result = buildTermMap termMapComp
                    
                    Expect.equal result.ExpressionMap.Template (Some "http://example.org/{id}") "Template should be set"
                    Expect.equal result.TermType (Some IRITerm) "Term type should be IRI"
            ]

        [<Tests>]
        let subjectMapTests =
            testList "SubjectMap Computation Tests" [
                testCase "can create subject map with class" <| fun _ ->
                    let subjectMapComp = subjectMap {
                        do! subjectTermMap (templateTermAsIRI "http://example.org/person/{id}")
                        do! addClass foafPerson
                    }
                    let result = buildSubjectMap subjectMapComp

                    Expect.equal result.SubjectTermMap.ExpressionMap.Template (Some "http://example.org/person/{id}") "Template should be set"
                    Expect.equal result.SubjectTermMap.TermType (Some IRITerm) "Term type should be IRI"
                    Expect.contains result.Class foafPerson "Should contain FOAF Person class"

                testCase "can add multiple classes" <| fun _ ->
                    let subjectMapComp = subjectMap {
                        do! subjectTermMap (templateTermAsIRI "http://example.org/person/{id}")
                        do! addClass foafPerson
                        do! addClass schemaPerson
                        do! addClasses ["http://example.org/Employee"; "http://example.org/Agent"]
                    }
                    let result = buildSubjectMap subjectMapComp
                    
                    Expect.contains result.Class foafPerson "Should contain FOAF Person"
                    Expect.contains result.Class schemaPerson "Should contain Schema Person"
                    Expect.contains result.Class "http://example.org/Employee" "Should contain Employee"
                    Expect.contains result.Class "http://example.org/Agent" "Should contain Agent"
                    Expect.equal result.Class.Length 4 "Should have 4 classes"
            ]

        [<Tests>]
        let objectMapTests =
            testList "ObjectMap Computation Tests" [
                testCase "can create literal object map" <| fun _ ->
                    let objectMapComp = objectMap {
                        do! objectTermMap (refTermAsLiteral "name")
                    }
                    let result = buildObjectMap objectMapComp

                    Expect.equal result.ObjectTermMap.ExpressionMap.Reference (Some "name") "Reference should be set"
                    Expect.equal result.ObjectTermMap.TermType (Some LiteralTerm) "Term type should be Literal"
                    Expect.isNone result.Datatype "Datatype should be None"

                testCase "can create typed literal object map" <| fun _ ->
                    let objectMapComp = objectMap {
                        do! objectTermMap (refTermAsLiteral "age")
                        do! datatype xsdInteger
                    }
                    let result = buildObjectMap objectMapComp

                    Expect.equal result.ObjectTermMap.ExpressionMap.Reference (Some "age") "Reference should be set"
                    Expect.equal result.Datatype (Some xsdInteger) "Datatype should be xsd:integer"

                testCase "can create language-tagged literal object map" <| fun _ ->
                    let objectMapComp = objectMap {
                        do! objectTermMap (refTermAsLiteral "description")
                        do! language "en"
                    }
                    let result = buildObjectMap objectMapComp
                    
                    Expect.equal result.Language (Some "en") "Language should be en"
            ]

        [<Tests>]
        let joinTests =
            testList "Join Computation Tests" [
                testCase "can create simple join condition" <| fun _ ->
                    let joinComp = join {
                        do! child "departmentId"
                        do! parent "id"
                    }
                    let result = { Child = None; Parent = None; ChildMap = None; ParentMap = None }
                    let finalResult = execState joinComp result
                    
                    Expect.equal finalResult.Child (Some "departmentId") "Child should be set"
                    Expect.equal finalResult.Parent (Some "id") "Parent should be set"
            ]

        [<Tests>]
        let predicateObjectMapTests =
            testList "PredicateObjectMap Computation Tests" [
                testCase "can create simple predicate-object mapping" <| fun _ ->
                    let pomComp = simplePredObj foafName "name"
                    let result = buildPredicateObjectMap pomComp
                    
                    Expect.contains result.Predicate foafName "Should contain foaf:name predicate"
                    Expect.equal result.ObjectMap.Length 1 "Should have one object map"
                    Expect.equal result.ObjectMap.[0].ObjectTermMap.ExpressionMap.Reference (Some "name") "Object should reference 'name'"

                testCase "can create typed predicate-object mapping" <| fun _ ->
                    let pomComp = typedPredObj foafAge "age" xsdInteger
                    let result = buildPredicateObjectMap pomComp
                    
                    Expect.contains result.Predicate foafAge "Should contain foaf:age predicate"
                    Expect.equal result.ObjectMap.[0].Datatype (Some xsdInteger) "Should have xsd:integer datatype"

                testCase "can add multiple predicates and objects" <| fun _ ->
                    let pomComp = predicateObjectMap {
                        do! addPredicate foafName
                        do! addPredicate "http://example.org/fullName"
                        do! addObjectMap (objectMap {
                            do! objectTermMap (refTermAsLiteral "firstName")
                        })
                        do! addObjectMap (objectMap {
                            do! objectTermMap (refTermAsLiteral "lastName")
                        })
                    }
                    let result = buildPredicateObjectMap pomComp
                    
                    Expect.equal result.Predicate.Length 2 "Should have 2 predicates"
                    Expect.equal result.ObjectMap.Length 2 "Should have 2 object maps"
            ]

        [<Tests>]
        let logicalSourceTests =
            testList "LogicalSource Computation Tests" [
                testCase "can create JSONPath logical source" <| fun _ ->
                    let lsComp = logicalSource {
                        do! iterator "$.people[*]"
                        do! asJSONPath
                    }
                    let result = buildLogicalSource lsComp
                    
                    Expect.equal result.SourceIterator (Some "$.people[*]") "Iterator should be set"
                    Expect.equal result.SourceReferenceFormulation (Some JSONPath) "Should be JSONPath"

                testCase "can create XPath logical source" <| fun _ ->
                    let lsComp = logicalSource {
                        do! iterator "//person"
                        do! asXPath
                    }
                    let result = buildLogicalSource lsComp

                    Expect.equal result.SourceIterator (Some "//person") "Iterator should be set"
                    Expect.equal result.SourceReferenceFormulation (Some XPath) "Should be XPath"

                testCase "can create CSV logical source" <| fun _ ->
                    let lsComp = logicalSource {
                        do! asCSV
                    }
                    let result = buildLogicalSource lsComp

                    Expect.equal result.SourceReferenceFormulation (Some CSV) "Should be CSV"
            ]

        [<Tests>]
        let triplesMapTests =
            testList "TriplesMap Computation Tests" [
                testCase "can create simple person triples map" <| fun _ ->
                    let personTriplesMapComp = triplesMap {
                        do! setLogicalSource (logicalSource {
                            do! iterator "$.people[*]"
                            do! asJSONPath
                        })
                        
                        do! setSubjectMap (subjectMap {
                            do! subjectTermMap (templateTermAsIRI "http://example.org/person/{id}")
                            do! addClass foafPerson
                        })
                        
                        do! addPredicateObjectMap (simplePredObj foafName "name")
                        do! addPredicateObjectMap (typedPredObj foafAge "age" xsdInteger)
                    }
                    
                    let result = buildTriplesMap personTriplesMapComp
                    
                    Expect.equal result.LogicalSource.SourceIterator (Some "$.people[*]") "Iterator should be set"
                    Expect.equal result.LogicalSource.SourceReferenceFormulation (Some JSONPath) "Should be JSONPath"
                    Expect.isSome result.SubjectMap "Should have subject map"
                    Expect.equal result.PredicateObjectMap.Length 2 "Should have 2 predicate-object maps"

                testCase "can create complex mapping with multiple classes" <| fun _ ->
                    let complexMapping = triplesMap {
                        do! setLogicalSource (logicalSource {
                            do! iterator "$.employees[*]"
                            do! asJSONPath
                        })
                        
                        do! setSubjectMap (subjectMap {
                            do! subjectTermMap (templateTermAsIRI "http://company.org/employee/{employeeId}")
                            do! addClass "http://company.org/Employee"
                            do! addClass foafPerson
                            do! addClass schemaPerson
                        })
                        
                        do! addPredicateObjectMap (simplePredObj foafName "fullName")
                        do! addPredicateObjectMap (typedPredObj "http://company.org/employeeId" "employeeId" xsdString)
                        do! addPredicateObjectMap (typedPredObj "http://company.org/startDate" "startDate" xsdDateTime)
                        
                        do! baseIRI "http://company.org/"
                    }
                    
                    let result = buildTriplesMap complexMapping
                    
                    Expect.equal result.SubjectMap.Value.Class.Length 3 "Should have 3 classes"
                    Expect.equal result.PredicateObjectMap.Length 3 "Should have 3 predicate-object maps"
                    Expect.equal result.BaseIRI (Some "http://company.org/") "Should have base IRI"

                testCase "can create mapping with for loop" <| fun _ ->
                    let mappings = [
                        (foafName, "name")
                        (foafEmail, "email")
                        ("http://company.org/department", "dept")
                    ]
                    
                    let loopMapping = triplesMap {
                        do! setLogicalSource (logicalSource {
                            do! iterator "$.people[*]"
                            do! asJSONPath
                        })
                        
                        do! setSubjectMap (subjectMap {
                            do! subjectTermMap (templateTermAsIRI "http://example.org/person/{id}")
                        })
                        
                        for (predicate, reference) in mappings do
                            do! addPredicateObjectMap (simplePredObj predicate reference)
                    }
                    
                    let result = buildTriplesMap loopMapping
                    
                    Expect.equal result.PredicateObjectMap.Length 3 "Should have 3 predicate-object maps from loop"

                testCase "can validate built triples map" <| fun _ ->
                    let validMapping = triplesMap {
                        do! setLogicalSource (logicalSource {
                            do! iterator "$.people[*]"
                            do! asJSONPath
                        })
                        
                        do! setSubjectMap (subjectMap {
                            do! subjectTermMap (templateTermAsIRI "http://example.org/person/{id}")
                        })
                        
                        do! addPredicateObjectMap (simplePredObj foafName "name")
                    }
                    
                    let result = buildTriplesMap validMapping
                    let validation = Model.Validation.validateTriplesMap result
                    
                    Expect.isOk validation "Should be valid triples map"
            ]

        [<Tests>]
        let composabilityTests =
            testList "Composability and Reusability Tests" [
                testCase "can reuse subject map components" <| fun _ ->
                    let standardPersonSubject = subjectMap {
                        do! subjectTermMap (templateTermAsIRI "http://example.org/person/{id}")
                        do! addClass foafPerson
                    }
                    
                    let mapping1 = triplesMap {
                        do! setLogicalSource (logicalSource {
                            do! iterator "$.people[*]"
                            do! asJSONPath
                        })
                        do! setSubjectMap standardPersonSubject
                        do! addPredicateObjectMap (simplePredObj foafName "name")
                    }
                    
                    let mapping2 = triplesMap {
                        do! setLogicalSource (logicalSource {
                            do! iterator "$.contacts[*]"
                            do! asJSONPath
                        })
                        do! setSubjectMap standardPersonSubject
                        do! addPredicateObjectMap (simplePredObj foafEmail "email")
                    }
                    
                    let result1 = buildTriplesMap mapping1
                    let result2 = buildTriplesMap mapping2
                    
                    Expect.equal result1.SubjectMap.Value.Class result2.SubjectMap.Value.Class "Should have same classes"
                    Expect.equal result1.SubjectMap.Value.SubjectTermMap.ExpressionMap.Template 
                                result2.SubjectMap.Value.SubjectTermMap.ExpressionMap.Template "Should have same template"

                testCase "can compose helper functions" <| fun _ ->
                    let createContactMapping nameRef emailRef = triplesMap {
                        do! setLogicalSource (logicalSource {
                            do! iterator "$.contacts[*]"
                            do! asJSONPath
                        })
                        
                        do! setSubjectMap (subjectMap {
                            do! subjectTermMap (templateTermAsIRI "http://contacts.org/person/{id}")
                            do! addClass foafPerson
                        })
                        
                        do! addPredicateObjectMap (simplePredObj foafName nameRef)
                        do! addPredicateObjectMap (simplePredObj foafEmail emailRef)
                    }
                    
                    let contactMapping = createContactMapping "fullName" "emailAddress"
                    let result = buildTriplesMap contactMapping
                    
                    Expect.equal result.PredicateObjectMap.Length 2 "Should have 2 predicate-object maps"
                    Expect.equal result.LogicalSource.SourceIterator (Some "$.contacts[*]") "Should have correct iterator"

                testCase "can validate computation expression results" <| fun _ ->
                    let invalidMapping = triplesMap {
                        do! setLogicalSource (logicalSource {
                            do! iterator "$.people[*]"
                            do! asJSONPath
                        })
                        // Missing subject map and predicate-object maps
                    }
                    
                    let result = buildTriplesMap invalidMapping
                    let validation = Model.Validation.validateTriplesMap result
                    
                    Expect.isError validation "Should be invalid without subject and predicate-object maps"
            ]

        [<Tests>]
        let integrationTests =
            testList "Integration Tests" [
                testCase "complete real-world example builds successfully" <| fun _ ->
                    // Department mapping
                    let departmentMapping = triplesMap {
                        do! setLogicalSource (logicalSource {
                            do! iterator "$.departments[*]"
                            do! asJSONPath
                        })
                        
                        do! setSubjectMap (subjectMap {
                            do! subjectTermMap (templateTermAsIRI "http://company.org/department/{id}")
                            do! addClass "http://company.org/Department"
                        })
                        
                        do! addPredicateObjectMap (simplePredObj "http://company.org/departmentName" "name")
                        do! addPredicateObjectMap (typedPredObj "http://company.org/departmentCode" "code" xsdString)
                    }
                    
                    let departmentTriplesMap = buildTriplesMap departmentMapping
                    
                    // Employee mapping with join
                    let employeeMapping = triplesMap {
                        do! setLogicalSource (logicalSource {
                            do! iterator "$.employees[*]"
                            do! asJSONPath
                        })
                        
                        do! setSubjectMap (subjectMap {
                            do! subjectTermMap (templateTermAsIRI "http://company.org/employee/{id}")
                            do! addClass "http://company.org/Employee"
                            do! addClass foafPerson
                        })
                        
                        do! addPredicateObjectMap (simplePredObj foafName "name")
                        do! addPredicateObjectMap (typedPredObj foafAge "age" xsdInteger)
                        
                        // Join to department
                        do! addPredicateObjectMap (predicateObjectMap {
                            do! addPredicate "http://company.org/worksInDepartment"
                            do! addRefObjectMap departmentTriplesMap (refObjectMap {
                                do! addJoinCondition (join {
                                    do! child "departmentId"
                                    do! parent "id"
                                })
                            })
                        })
                    }
                    
                    let employeeTriplesMap = buildTriplesMap employeeMapping
                    
                    // Validate both mappings
                    let deptValidation = Model.Validation.validateTriplesMap departmentTriplesMap
                    let empValidation = Model.Validation.validateTriplesMap employeeTriplesMap
                    
                    Expect.isOk deptValidation "Department mapping should be valid"
                    Expect.isOk empValidation "Employee mapping should be valid"
                    
                    // Check join relationship
                    Expect.equal employeeTriplesMap.PredicateObjectMap.Length 3 "Employee should have 3 predicate-object maps"
                    let joinPOM = employeeTriplesMap.PredicateObjectMap |> List.find (fun pom -> pom.RefObjectMap.Length > 0)
                    Expect.equal joinPOM.RefObjectMap.[0].JoinCondition.Length 1 "Should have one join condition"
            ]

        [<Tests>]
        let allComputationRMLTests = 
            testList "All ComputationRML Tests" [
                expressionMapTests
                termMapTests
                subjectMapTests
                objectMapTests
                joinTests
                predicateObjectMapTests
                logicalSourceTests
                triplesMapTests
                composabilityTests
                integrationTests
            ]

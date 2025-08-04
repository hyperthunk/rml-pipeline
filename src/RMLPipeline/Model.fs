namespace RMLPipeline

module Model = 

    // Base types for URIs and literals
    type URI = string
    type Literal = string
    type IRI = string

    // Language tags
    type LanguageTag = string

    // Basic RDF types
    type RDFNode = 
        | URI of URI
        | Literal of Literal
        | BlankNode of string

    // Core RML Types

    // Term types for RML mapping
    type TermType =
        | BlankNodeTerm
        | IRITerm
        | LiteralTerm
        | URITerm
        | UnsafeIRITerm
        | UnsafeURITerm

    // Reference formulations
    type ReferenceFormulation =
        | CSV
        | JSONPath
        | XPath
        | SQL2008Query
        | SQL2008Table

    // Compression formats
    type Compression =
        | Uncompressed
        | Gzip
        | Zip
        | TarGzip
        | TarXz

    // Encoding formats
    type Encoding =
        | UTF8
        | UTF16

    // Expression Map - base for all maps that generate values
    type ExpressionMap = {
        Template: string option
        Reference: string option
        Constant: RDFNode option
        FunctionExecution: FunctionExecution option
    }

    // Term Map - generates RDF terms
    and TermMap = {
        ExpressionMap: ExpressionMap
        TermType: TermType option
        LogicalTarget: LogicalTarget option
    }

    // Subject Map
    and SubjectMap = {
        SubjectTermMap: TermMap
        Class: IRI list
        GraphMap: GraphMap list
    }

    // Predicate Map
    and PredicateMap = {
        PredicateTermMap: TermMap
    }

    // Object Map
    and ObjectMap = {
        ObjectTermMap: TermMap
        Datatype: IRI option
        DatatypeMap: DatatypeMap option
        Language: LanguageTag option
        LanguageMap: LanguageMap option
    }

    // Graph Map
    and GraphMap = {
        GraphTermMap: TermMap
    }

    // Datatype Map
    and DatatypeMap = {
        DatatypeTermMap: TermMap
    }

    // Language Map
    and LanguageMap = {
        ExpressionMap: ExpressionMap
        LogicalTarget: LogicalTarget option
    }

    // Parent and Child Maps for joins
    and ParentMap = {
        ExpressionMap: ExpressionMap
    }

    and ChildMap = {
        ExpressionMap: ExpressionMap
    }

    // Join condition
    and Join = {
        Child: string option
        Parent: string option
        ChildMap: ChildMap option
        ParentMap: ParentMap option
    }

    // Reference Object Map
    and RefObjectMap = {
        ParentTriplesMap: TriplesMap
        JoinCondition: Join list
    }

    // Predicate-Object Map
    and PredicateObjectMap = {
        Predicate: IRI list
        PredicateMap: PredicateMap list
        Object: RDFNode list
        ObjectMap: ObjectMap list
        RefObjectMap: RefObjectMap list
        GraphMap: GraphMap list
    }

    // Strategy for collections and joins
    and Strategy = 
        | Append
        | CartesianProduct

    // Gather Map for collections
    and GatherMap = {
        GatherTermMap: TermMap
        Gather: IRI list
        AllowEmptyListAndContainer: bool option
        Strategy: Strategy option
        GatherAs: RDFContainerType list
    }

    and RDFContainerType =
        | RDFAlt
        | RDFBag
        | RDFSeq
        | RDFList

    // Function execution for FNML
    and FunctionExecution = {
        Function: IRI option
        FunctionMap: FunctionMap option
        Input: Input list
    }

    and FunctionMap = {
        FunctionTermMap: TermMap
    }

    and Input = {
        Parameter: IRI option
        ParameterMap: ParameterMap option
        InputValue: RDFNode option
        InputValueMap: TermMap option
        FunctionMap: FunctionMap option
    }

    and ParameterMap = {
        ParameterTermMap: TermMap
    }

    and ReturnMap = {
        ReturnTermMap: TermMap
    }

    // Source and Target types (RML-IO)
    and Source = {
        Path: string option
        Compression: Compression option
        Encoding: Encoding option
        Null: Literal list
    }

    and Target = {
        Path: string option
        Compression: Compression option
        Encoding: Encoding option
    }

    and RelativePath = {
        Path: string
        Root: PathRoot
    }

    and PathRoot =
        | CurrentWorkingDirectory
        | MappingDirectory
        | CustomPath of string

    // Namespace for XPath
    and Namespace = {
        NamespacePrefix: string
        NamespaceURL: IRI
    }

    and XPathReferenceFormulation = {
        Namespace: Namespace list
    }

    // Logical Source and Target
    and AbstractLogicalSource = {
        SourceIterator: string option
        SourceReferenceFormulation: ReferenceFormulation option
    }

    and LogicalSource = {
        AbstractLogicalSource: AbstractLogicalSource
        Source: SourceOrPath
        Path: string option
    }

    and SourceOrPath =
        | SourceRef of Source
        | RelativePathRef of RelativePath

    and LogicalTarget = {
        Target: TargetOrPath option
        Serialization: Format option
    }

    and TargetOrPath =
        | TargetRef of Target
        | RelativePathRef of RelativePath

    and Format = IRI // Represents formats:Format

    // Iterable base type
    and Iterable = {
        Iterator: string option
        ReferenceFormulation: ReferenceFormulation option
    }

    // Main Triples Map
    and TriplesMap = {
        LogicalSource: AbstractLogicalSource
        SubjectMap: SubjectMap option
        Subject: IRI option
        PredicateObjectMap: PredicateObjectMap list
        BaseIRI: IRI option
        LogicalTarget: LogicalTarget option
    }

    // RML-star extensions
    and StarMap = unit // Placeholder for RML-star specific functionality

    and AssertedTriplesMap = {
        TriplesMap: TriplesMap
        SubjectMap: SubjectMap option
        QuotedTriplesMap: NonAssertedTriplesMap list
    }

    and NonAssertedTriplesMap = {
        TriplesMap: TriplesMap
        QuotedTriplesMap: AssertedTriplesMap list
    }

    // RML-LV (Logical Views) types
    and LogicalView = {
        AbstractLogicalSource: AbstractLogicalSource
        ViewOn: AbstractLogicalSource
        Field: Field list
        InnerJoin: LogicalViewJoin list
        LeftJoin: LogicalViewJoin list
        StructuralAnnotation: StructuralAnnotation list
    }

    and Field = {
        FieldName: string
    }

    and IterableField = {
        Field: Field
        Iterable: Iterable
    }

    and ExpressionField = {
        Field: Field
        ExpressionMap: ExpressionMap
    }

    and LogicalViewJoin = {
        ParentLogicalView: LogicalView
        JoinCondition: Join list
    }

    and StructuralAnnotation =
        | InclusionDependencyAnnotation of InclusionDependencyAnnotation
        | ForeignKeyAnnotation of ForeignKeyAnnotation
        | NonNullableAnnotation of NonNullableAnnotation
        | UniqueAnnotation of UniqueAnnotation
        | PrimaryKeyAnnotation of PrimaryKeyAnnotation
        | IRISafeAnnotation of IRISafeAnnotation

    and InclusionDependencyAnnotation = {
        OnFields: Field list
        TargetView: LogicalView
        TargetFields: Field list
    }

    and ForeignKeyAnnotation = {
        InclusionDependencyAnnotation: InclusionDependencyAnnotation
    }

    and NonNullableAnnotation = {
        OnFields: Field list
    }

    and UniqueAnnotation = {
        OnFields: Field list
    }

    and PrimaryKeyAnnotation = {
        NonNullableAnnotation: NonNullableAnnotation
        UniqueAnnotation: UniqueAnnotation
    }

    and IRISafeAnnotation = {
        OnFields: Field list
    }
    
    [<Interface>]
    type TripleOutputStream =
        // abstract member EmitTypedTriple: subject: RDFNode * predicate: RDFNode * objValue: RDFNode * datatype: IRI option -> unit
        abstract member EmitTypedTriple: subject: string * predicate: string * objValue: string * datatype: string option -> unit
        abstract member EmitLangTriple: subject: string * predicate: string * objValue: string * lang: string -> unit
        abstract member EmitTriple: subject: string * predicate: string * objValue: string -> unit

    // Helper functions for creating common mappings
    module Builders =
    
        let emptyExpressionMap = {
            Template = None
            Reference = None
            Constant = None
            FunctionExecution = None
        }
        
        let emptyTermMap = {
            ExpressionMap = emptyExpressionMap
            TermType = None
            LogicalTarget = None
        }
        
        let createConstantMap value = {
            emptyExpressionMap with Constant = Some value
        }
        
        let createTemplateMap template = {
            emptyExpressionMap with Template = Some template
        }
        
        let createReferenceMap reference = {
            emptyExpressionMap with Reference = Some reference
        }
        
        let createSubjectMap termType classes = {
            SubjectTermMap = { emptyTermMap with TermType = termType }
            Class = defaultArg classes []
            GraphMap = []
        }
        
        let createPredicateMap termType = {
            PredicateTermMap = { emptyTermMap with TermType = termType }
        }
        
        let createObjectMap termType datatype language = {
            ObjectTermMap = { emptyTermMap with TermType = termType }
            Datatype = datatype
            DatatypeMap = None
            Language = language
            LanguageMap = None
        }
        
        let createTriplesMap logicalSource subjectMap predicateObjectMaps = {
            LogicalSource = logicalSource
            SubjectMap = Some subjectMap
            Subject = None
            PredicateObjectMap = predicateObjectMaps
            BaseIRI = None
            LogicalTarget = None
        }
        
        let createLogicalSource iterator referenceFormulation source = {
            AbstractLogicalSource = {
                SourceIterator = iterator
                SourceReferenceFormulation = referenceFormulation
            }
            Source = source
            Path = None
        }

    // Common predicates and properties as constants
    module Vocabulary =
        
        [<Literal>]
        let RML_NAMESPACE = "http://w3id.org/rml/"
        
        [<Literal>]
        let RML_CORE_NAMESPACE = "http://w3id.org/rml/core/"
        
        [<Literal>]
        let RML_IO_NAMESPACE = "http://w3id.org/rml/io/"
        
        [<Literal>]
        let RML_FNML_NAMESPACE = "http://w3id.org/rml/fnml/"
        
        [<Literal>]
        let RML_LV_NAMESPACE = "http://w3id.org/rml/lv/"
        
        // Core properties
        let logicalSource = RML_CORE_NAMESPACE + "logicalSource"
        let subjectMap = RML_CORE_NAMESPACE + "subjectMap"
        let predicateObjectMap = RML_CORE_NAMESPACE + "predicateObjectMap"
        let predicateMap = RML_CORE_NAMESPACE + "predicateMap"
        let objectMap = RML_CORE_NAMESPACE + "objectMap"
        let template = RML_CORE_NAMESPACE + "template"
        let reference = RML_CORE_NAMESPACE + "reference"
        let constant = RML_CORE_NAMESPACE + "constant"
        let termType = RML_CORE_NAMESPACE + "termType"
        let datatype = RML_CORE_NAMESPACE + "datatype"
        let language = RML_CORE_NAMESPACE + "language"
        let graph = RML_CORE_NAMESPACE + "graph"
        let graphMap = RML_CORE_NAMESPACE + "graphMap"
        let ``class`` = RML_CORE_NAMESPACE + "class"
        let joinCondition = RML_CORE_NAMESPACE + "joinCondition"
        let child = RML_CORE_NAMESPACE + "child"
        let parent = RML_CORE_NAMESPACE + "parent"
        let parentTriplesMap = RML_CORE_NAMESPACE + "parentTriplesMap"

    // Validation helpers
    module Validation =
        
        let validateTermMap (termMap: TermMap) =
            let expr = termMap.ExpressionMap
            let hasValue = 
                expr.Template.IsSome || 
                expr.Reference.IsSome || 
                expr.Constant.IsSome || 
                expr.FunctionExecution.IsSome
            
            if not hasValue then
                Error "TermMap must have at least one value-generating property"
            else
                Ok termMap
        
        let validateTriplesMap (triplesMap: TriplesMap) =
            if triplesMap.SubjectMap.IsNone && triplesMap.Subject.IsNone then
                Error "TriplesMap must have either a SubjectMap or Subject"
            elif triplesMap.PredicateObjectMap.IsEmpty then
                Error "TriplesMap must have at least one PredicateObjectMap"
            else
                Ok triplesMap
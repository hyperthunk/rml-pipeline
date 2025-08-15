// PATCH NOTE:
// If this file already exists in your test project, integrate the promotion-specific
// tests by referencing the new StringInterningPromotionTests module. The original
// tests can remain; remove or un-focus (ftestProperty) the older variant of
// "Same access pattern returns consistent IDs" if you had previously weakened it.
// This stub shows how to ensure the new module is included in the test assembly.

namespace RMLPipeline.Tests

open Expecto

module AllStringInterningTestHost =
    [<Tests>]
    let promotionSuite =
        // We simply reference the other test list so Expecto discovers it.
        RMLPipeline.Tests.StringInterningPromotionTests.promotionTests
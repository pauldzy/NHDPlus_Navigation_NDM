# NHDPlus Navigation using Oracle NDM
#### An alternative prototype using [Oracle Network Data Model](https://docs.oracle.com/en/database/oracle/oracle-database/21/topol/network-data-model-graph-overview.html#GUID-D34F1A0C-4D9B-4185-B965-43B218D3F382) for stream navigation.

The [National Hydrology Dataset Plus](https://www.epa.gov/waterdata/nhdplus-national-hydrography-dataset-plus) has provided [an engine](https://www.epa.gov/waterdata/learn-more#vaa) for stream navigation for almost two decades.  This engine subsequently was ported into Oracle about 12 years ago and provided as public service as an [EPA Office of Water service](https://www.epa.gov/waterdata/navigation-service).  See [this codepen](http://codepen.io/pauldzy/pen/LVoBpj) to view the current EPA service in action.

The existing engine predates the release of modern network and graph analysis tools and there may be some concern regarding the scaleability of the current (v1) engine.  First undertaken around 2014, this project rewrites the navigation logic using the [Oracle Network Data Model](https://docs.oracle.com/en/database/oracle/oracle-database/21/topol/network-data-model-graph-overview.html#GUID-D34F1A0C-4D9B-4185-B965-43B218D3F382) and in fact fails in its ultimate goal of replacing the old Navigation engine due to [problematic Java heap memory management](https://community.oracle.com/tech/apps-infra/discussion/3722944/sdo-net-tracein-and-traceout-features-or-bugs) and other logic limitations within NDM.  While it is possible this situation may one day be corrected by Oracle, the NDM navigation logic functions well for smaller scale navigation tasks and can be used for head-to-head testing and QA duties with both the old navigator and future implementations of NHDPlus navigation.

## Setup

The logic has been tested in both Oracle 11g, 12g and 18c without noticeable difference in performance or functionality.

The navigation logic requires the NHDPlus flowline network to be converted into two [NDM graphs](https://docs.oracle.com/en/database/oracle/oracle-database/21/topol/network-data-model-graph-overview.html#GUID-4D127E98-2856-4C90-9FE0-BCA156E39C7C) - one based on flowline length (km) and the other on flowline flowtime (days).  The **network_builder** package has procedures to create a network in a NHDPLUS_TOPONET schema.  The source NHDPlus flowline and plusflow resources may be requested from EPA via their waters_support@epa.gov helpline or created from [the source NHDPlus data downloads](https://www.epa.gov/waterdata/get-data#Download) by hand.

With the network in place, build the preprocessing and temporary tables using the **build_preprocessing_tables** and **build_temporary_tables** procedures and then run the **navigate** procedure by inspecting the inline documentation.  If you are more interested in the end results, there are working examples running on EPA servers identified as version 2 of navigation you can [view here](https://codepen.io/pauldzy/pen/BajBpzo).

## Limitations

NHDPlus Navigation via NDM fails primarily in two use cases, large Upstream with Tributaries and any Downstream with Divergences navigation.  

For the former the reason is fairly simple, NDM marshals navigation results in memory.  Thus you can navigation only for much PGA memory your database session has.  One would not think the average NDM link would require much memory, but large continent spanning navigation will fail with memory errors.  This is despite code attempts to maximum the Java heap.  For typical use cases running 10 to 50 miles upstream it works fine.  In theory with enough memory this is would not be a problem.  

For the latter situation the problem is tied to how Downstream with Divergences distance and flowtime tallys work or rather don't work with the NDM system.  When we navigate downstream with divergences, the proper method to determine NHDPlus costs is to first calculate the costs on the mainstem.  Then for each divergence calculate it's mainstem cost starting with the cost value at the top from the mainstem.  Then for any divergence off that divergence get its parent main stem costs iteratively until all divergences are calculated. For divergences that loop back into the mainstem, sometimes they are longer than the mainstem or they may well be shorter - such is natural hydrography.  I cannot find a way to prioritize the mainstem within the bounds of Oracle NDM.  There seems no way to control the internal routing logic of which branch to follow to drive costing.  NDM **always** follows the lowest cost routing with that route generating tallied costs.  Altering the costs to drive the prioritization would then bork the cost for knowing when to stop.  Its possible there is a clever solution for this problem but six years on I have not found it.  Any solution or suggestions are **always** appreciated.


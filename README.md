# NHDPlus Navigation using Oracle NDM
#### An alternative prototype using [Oracle Network Data Model](http://docs.oracle.com/database/121/TOPOL/sdo_net_concepts.htm#TOPOL700) for stream navigation.

The [National Hydrology Dataset Plus](https://www.epa.gov/waterdata/nhdplus-national-hydrography-dataset-plus) has provided [an engine](https://www.epa.gov/waterdata/learn-more#vaa) for stream navigation for well over a decade.  This engine subsequently was ported into Oracle and provided as public service as an [EPA Office of Water service](https://www.epa.gov/waterdata/navigation-service).  See [this codepen](http://codepen.io/pauldzy/pen/LVoBpj) to view the current EPA service in action.

The existing engine predates the release of modern network and graph analysis tools and there may be some concern regarding the scaleability of the current engine.  This project rewrites the navigation logic using the [Oracle Network Data Model](http://docs.oracle.com/database/121/TOPOL/sdo_net_concepts.htm#TOPOL700) and in fact fails in its ultimate goal of replacing the old Navigation engine due to [problematic Java heap memory management](https://community.oracle.com/thread/3722944?start=0&tstart=0) within NDM.  While it is possible this situation may be corrected by Oracle, the alternative navigation logic still serves as limited comparison tool for doing head-to-head testing within the confines of the memory issue.

## Setup

The logic has been tested in both Oracle 11g and 12c without noticeable difference in performance or functionality.

The navigation logic requires the NHDPlus flowline network to be converted into an [NDM graph](http://docs.oracle.com/database/121/TOPOL/sdo_net_concepts.htm#TOPOL893).  The **network_builder** package has procedures to create a network in a NHDPLUS_TOPONET schema.  The source NHDPlus flowline and plusflow resources may be requested from EPA via their waters_support@epa.gov helpline or created from [the source NHDPlus data downloads](https://www.epa.gov/waterdata/get-data#Download) by hand.

With the network in place, build the temporary tables using the **build_temporary_tables** procedure and then run the **navigate** procedure by inspecting the inline documentation.  If you are more interested in the end results, there is a working example running (for now) on EPA servers you can [view here](http://codepen.io/pauldzy/pen/JdXjab).

I am interested in the usage and future of Oracle NDM.  Note that this project was disappointing in that Oracle took their system **almost** to end zone just to fail right in the last few yards.  It seems like a lot of engineering effort for not quite getting there.  I would like to see these problems resolved and am interested in your feedback.




# fcculs

**Summary**: (enough to start)

This R script contains three main parts:

1. Download the relevant FCC ULS Database files,
2. Combine them using various database techniques, especially "joins", and
2. Generate a set of "flat-file" files that a 'HAM/Scanner' user might find useful.

There isn't enough room on GitHub to host the resulting files (datasets), so instead they are posted at: https://smithw.org/scma/

There is a file for each SoCal county in two formats: .csv and .xlsx.
  The files are indeed large, but they will load into either the (non-commercial) LibreOffice Calc spreadsheet or the (commercial) MS-Excel spreadsheet.
  Note that the column delimiter in the .csv files is a ***vertical bar*** ("|") not the usual ***comma*** (",").

**Details**: (for the curious)

I've made specific decisions about the data.
  This was partly done to make the overall process feasible, but also so that a single file (.csv or .xlsx) would load into a spreadsheet completely.
  It should be relatively easy to modify the code to alter these assumptions.  Specifically,

1. ***Inclusion***
    * I've included the ten (10) counties in SoCal from the Mexican border in the south to Kern and San Luis Obisbo counties in the north.
    * I've included only the 'Active' licenses.
    * Either the 'location_county' column or 'control_county' column has to be populated.  If both are blank, the record isn't included.
2. ***Exclusion***
    * I've excluded the cellular bands.
    * I've excluded all frequencies above 1.3GHz.

**Timing**:

  * ***Network:*** It takes about 20 minutes to download all the necessary FCC ULS files.
    This rate appears to be limited on the FCC side either at the network-level or the server-level.
  * ***DBMS:***  Locally, with respect to database management, the database joins generally take the most amount of time, even using indexes.
    More RAM and faster hard drives help.
    I've used SQLite internally but I've kept the joins in SQL (rather than, say, dplyr or data.table) so that it's relatively easy to to switch DBMS back-ends.

**Code**:

I've tried to use Base R functionality in most places.
  The few libraries that are used are listed at the top of the R script.
  The code begins with the `main()` function at the bottom of the R script.

I welcome your feedback.


Enjoy,\
Wayne Smith, Ph.D.\
N6LHV\
Southern California Monitoring Association (SCMA)\
<mailto:n6lhv@arrl.net>


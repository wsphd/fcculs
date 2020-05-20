# fcculs

Summary:
This R script contains two main parts:

1. Download the relevant FCC ULS Database files, and
2. Combine them in a way that a 'HAM/Scanner' user might find useful

There isn't enough room on GitHub to host the resulting datasets, so they are posted at:
[FCC ULS CA SoCal Counties data] https://smithw.org/scma/

Details:
I've made specific decisions about the data.
  This was partly done to make the overall process feasible, but also so that single file (.csv or .xlsx) would load into a spreadsheet completely.
  It should be relatively easy to modify the code alter these assumptions.  Specifically,

1. Inclusion
    * I've included the ten counties in SoCal from the Mexican border to both Kern and San luis Obisbo in the north.
    * I've included only the 'Active' licenses.
2. Exclusion
    * I've excluded the celluar bands.
    * I've excluded all frequencies above 1.3GHz.

Timing:
It takes about 20 minutes to download all the necessary FCC ULS files.  This rate appears to be throttled on the FCC side.
The database joins generally the most amount of time.  More RAM and faster hard drives help.

Code:
I've tried to use Base R functionality in most places.
  The few libraries that are used are listed at the top of the R script.
  The code begins with the main() function at the bottom of the R script.

Enjoy,

Wayne Smith, Ph.D.
N6LHV
Southern California Monitoring Association


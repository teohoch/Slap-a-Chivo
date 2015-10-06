__author__ = 'teohoch'
from astroquery.splatalogue import Splatalogue
from astropy import units as u
import time

#AStroquery has a bug that doesn't allow for setting this keys to anything, so everytime you want to query, it will exclude atmospheric, probable, and potential lines. To fix this, the keys in data must be removed
del Splatalogue.data["no_atmospheric"]
del Splatalogue.data["no_potential"]
del Splatalogue.data["no_probable"]

for i in range(47, 100000):
    step = i*10
    t0 = time.time()
    lines = Splatalogue.query_lines_async(0 * u.GHz,
                                step * u.GHz,
                                exclude=('nothing', 'no'),
                                version='v2.0',
                                only_NRAO_recommended=None,
                                export=True,
                                export_limit=999999999,
                                noHFS=False,
                                displayHFS=False,
                                show_unres_qn=False,
                                show_upper_degeneracy=False,
                                show_molecule_tag=False,
                                show_qn_code=False,
                                show_lovas_labref=True,
                                show_lovas_obsref=True,
                                show_orderedfreq_only=False,
                                show_nrao_recommended=True
                                      )
    t1 = time.time()
    print "Frequency from 0 to " + str(step) +"GHz, with " + str(len(lines.content.split("\n") )) + " lines in " + str(t1-t0)
    del lines




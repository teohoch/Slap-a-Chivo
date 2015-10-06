__author__ = 'teohoch'
from astroquery.splatalogue import Splatalogue
from astropy import units as u
import time

#AStroquery has a bug that doesn't allow for setting this keys to anything, so everytime you want to query, it will exclude atmospheric, probable, and potential lines. To fix this, the keys in data must be removed
del Splatalogue.data["no_atmospheric"]
del Splatalogue.data["no_potential"]
del Splatalogue.data["no_probable"]

'''
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
'''
print("Descargando 1° Segmento")
lines = Splatalogue.query_lines_async(
    0 * u.GHz,
    470 * u.GHz,
    export=True,
    export_limit=999999999,
    line_strengths=('ls1','ls2', 'ls3', 'ls4', 'ls5'),
    displayHFS=True,
    show_unres_qn=True,
    show_upper_degeneracy=True,
    show_molecule_tag=True,
    show_qn_code=True,
    show_lovas_labref=True,
    show_lovas_obsref=True,
    show_orderedfreq_only=None,
    show_nrao_recommended=True
    )
print("Descargando 2° Segmento")
lines2 = Splatalogue.query_lines_async(
    470 * u.GHz,
    1000 * u.GHz,
    export=True,
    export_limit=999999999,
    line_strengths=('ls1','ls2', 'ls3', 'ls4', 'ls5'),
    displayHFS=True,
    show_unres_qn=True,
    show_upper_degeneracy=True,
    show_molecule_tag=True,
    show_qn_code=True,
    show_lovas_labref=True,
    show_lovas_obsref=True,
    show_orderedfreq_only=None,
    show_nrao_recommended=True
    )
print("Descargando 3° Segmento")
lines3 = Splatalogue.query_lines_async(
    1000 * u.GHz,
    1500 * u.GHz,
    export=True,
    export_limit=999999999,
    line_strengths=('ls1','ls2', 'ls3', 'ls4', 'ls5'),
    displayHFS=True,
    show_unres_qn=True,
    show_upper_degeneracy=True,
    show_molecule_tag=True,
    show_qn_code=True,
    show_lovas_labref=True,
    show_lovas_obsref=True,
    show_orderedfreq_only=None,
    show_nrao_recommended=True
    )
print("Descargando 4° Segmento")
lines4 = Splatalogue.query_lines_async(
    1500 * u.GHz,
    100000 * u.THz,
    export=True,
    export_limit=999999999,
    line_strengths=('ls1','ls2', 'ls3', 'ls4', 'ls5'),
    displayHFS=True,
    show_unres_qn=True,
    show_upper_degeneracy=True,
    show_molecule_tag=True,
    show_qn_code=True,
    show_lovas_labref=True,
    show_lovas_obsref=True,
    show_orderedfreq_only=None,
    show_nrao_recommended=True
    )

with open("complete", "w") as f:
    f.write(lines)
    f.write(lines2)
    f.write(lines3)
    f.write(lines4)


__author__ = 'teohoch'
from astroquery.splatalogue import Splatalogue
from astropy import units as u
import time
import threading


class SlaGetter():
    del Splatalogue.data["no_atmospheric"]
    del Splatalogue.data["no_potential"]
    del Splatalogue.data["no_probable"]



    def getLines(self,lower, upper, location):
        print("Downloading From "+ str(lower) + " to " + str(upper))
        lines = Splatalogue.query_lines_async(
            lower,
            upper,
            export=True,
            export_limit=999999999,
            line_strengths=('ls1', 'ls2', 'ls3', 'ls4', 'ls5'),
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
        print("Downloadg From " + str(lower) + " to " + str(upper) + " Completed | ") + lines.content.split("\n")[1]
        Container[location] = lines.content

        return lines
#AStroquery has a bug that doesn't allow for setting this keys to anything, so everytime you want to query, it will exclude atmospheric, probable, and potential lines. To fix this, the keys in data must be removed


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
Container = ["","","",""]
g = SlaGetter()
t1 = threading.Thread(target=g.getLines,args=(0 * u.MHz,470000 * u.MHz,0))
t2 = threading.Thread(target=g.getLines,args=(470000 * u.MHz,1000000 * u.MHz,1))
t3 = threading.Thread(target=g.getLines,args=(1000000 * u.MHz,1500000 * u.MHz,2))
t4 = threading.Thread(target=g.getLines,args=(1500000 * u.MHz,1000000000000* u.MHz,3))

t1.start()
t2.start()
t3.start()
t4.start()

t1.join()
t2.join()
t3.join()
t4.join()

with open("complete_parallel", "w") as f:
    for i in Container:
        f.write(i)



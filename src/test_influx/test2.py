#!/usr/bin/env python 



import numpy as np
import matplotlib.pyplot as plt
 
# data to be plotted
x = np.arange(1, 11)
y = x * x
 
# plotting
plt.title("Line graph")
plt.xlabel("X axis")
plt.ylabel("Y axis")
plt.plot(x, y, color ="red")
plt.show()

x=[-4,-106,-13,-81,-82,1,-127,-32,-87,-102,3,-118,-30,-19,-53,73,-44,16,36,-42,56,-38,-17,27,-78,22,-40,-48,47,-79,0,-31,-37,41,-89,-16,-13,-68,19,-89,-35,-11,-98,10,-68,-56,7,-99,-5,-66,-67,20,-103,-23,-45,-80,12,-100,-41,-29,-92,22,-55,-34,18,-65,47,-11,-20,33,-101,-28,-69,-86,4,-157,-152,251,1058,1834,930,-18,-25,-25,33,-85,-73,-14,-125,-32,-95,-103,-14,-141,-61,-80,-113,-9,-122,-50,-34,-119,0,-118,-69,-53,-142,-32,-127,-83,-33,-125,-12,-67,-53,45,-63,30,12,-29,69,-64,1,18,-65,49,-58,-7
#print(x)
plt.plot([1, 2, 3, 4])
plt.ylabel('some numbers')
plt.show()


import matplotlib.pyplot as plt

# parameters for nice axis labels
plt.rcParams['figure.figsize'] = (30, 18)

plt.rcParams['font.family']     = 'sans-serif' 
plt.rcParams['font.sans-serif'] = 'Arial'
plt.rcParams['font.size']       = 20.0
plt.rcParams['font.weight']     = 'bold'

plt.rcParams['axes.labelsize']   = 60
plt.rcParams['axes.labelweight'] = 'bold'
plt.rcParams['axes.linewidth']   = 1.2  # edge line width of plot

plt.rcParams['lines.linewidth']  = 5.0  # normal line width 
plt.rcParams['lines.markersize']  = 20  # normal marker size

plt.rcParams['xtick.major.size']    = 26
plt.rcParams['xtick.minor.size']    = 13
plt.rcParams['xtick.labelsize']     = 60
plt.rcParams['xtick.major.width']   = 3
plt.rcParams['xtick.minor.width']   = 3
#plt.rcParams['xtick.minor.visible'] = True
plt.rcParams['xtick.direction']     = 'in'

plt.rcParams['ytick.major.size']    = 26
plt.rcParams['ytick.minor.size']    = 13
plt.rcParams['ytick.labelsize']     = 60
plt.rcParams['ytick.major.width']   = 3
plt.rcParams['ytick.minor.width']   = 3
#plt.rcParams['ytick.minor.visible'] = True
plt.rcParams['ytick.direction']     = 'in'
#plt.rcParams['ytick.labelright']     = True # label also draw on the right side of the plot

#plt.rcParams['mathtext.fontset'] = 'custom' #UserWarning: findfont: Font family ['cursive'] not found. Falling back to DejaVu Sans
plt.rcParams['mathtext.fontset'] = r'stix'  #["dejavusans", "dejavuserif", "cm", "stixsans", "stix"]
plt.rcParams['mathtext.rm'] = r'sans\-serif'        # mathtext.rm==\mathrm{} Roman (upright)
plt.rcParams['mathtext.it'] = r'sans\-serif:italic' # mathtext.it==\mathit{} or default italic

plt.rcParams['legend.frameon']  = False
plt.rcParams['legend.loc']      = 'best' 
plt.rcParams['legend.fontsize'] = 35













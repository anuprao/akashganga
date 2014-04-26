###############################################################################
#
# RenderFarm : written by Anup Jayapal Rao 
#		e-mail id:	anup.kadam <at> gmail.com, anup_kadam <at> yahoo.com, ar210 <at> sussex.ac.uk
#
#	Written for the purpose of the Distributed systems assignment at the University Of Sussex, 2008.
#
###############################################################################

##
# JobMonitor 
# 
# This program is responsible for displaying the status of all the servers and 
# clients involved in the render process. 
##

# System dependent libraries
import threading
import time
import socket
import os,os.path
import sys, logging
from subprocess import *
import hashlib

# GUI related libraries
import pygtk
pygtk.require('2.0')
import gobject
import pango
import gtk
import math
import time
from gtk import gdk
import cairo   
    
# CORBA related libraries
import CORBA
import CosNaming

# IDL generated library
import RenderFarm
import RenderFarm__POA

###############################################################################
# Class : GUI_JobMonitor
# This class is responsible for creating and handling the GUI for the 
# JobMonitor.
###############################################################################
(
    COLUMN_HOSTNAME,
    COLUMN_STATUS,
    COLUMN_PROGRESS
) = range(3)

gtk.gdk.threads_init()
	
class GUI_JobMonitor( ):
	
	##
	# Constructor. Creates the widgets for the GUI of the JobMonitor.
	# @param	oRenderServer	RenderServer object that is controlling the renderfarm
	# @param	oFrameServer	FrameServer object that is collecting the rendered frames
	# @param	oJobMonitor		JobMonitor object that is represented by the GUI	
	def __init__( self, oRenderServer, oFrameServer, oJobMonitor):
		self.oRenderServer = oRenderServer
		self.oFrameServer = oFrameServer		
		self.oJobMonitor = servantJobMonitor	
		self.dictClientInfo = {}
				
		self.win = gtk.Window()
		self.win.set_title('JobMonitor on ' + socket.gethostname())
		self.win.connect('delete-event', gtk.main_quit)
		self.win.move(100, 100)
		#self.win.resize(800, 480)
		
		self.win.set_border_width(10)
		
		oVBox = gtk.VBox(False, 10)
		
		imageBanner = gtk.Image()
		imageBanner.set_from_file('JM.png')
		oVBox.pack_start(imageBanner,False,False)
		
		oHBox1 = gtk.HBox(False, 10)
		
		self.lbl_RenderServer = gtk.Label("Render Server: Not connected")
		self.lbl_RenderServer.set_justify(gtk.JUSTIFY_LEFT)
		oHBox1.pack_start(self.lbl_RenderServer,False,True)	
		
		self.lbl_FrameServer = gtk.Label("Frames Server: Not connected")
		self.lbl_FrameServer.set_justify(gtk.JUSTIFY_LEFT)
		oHBox1.pack_start(self.lbl_FrameServer,True,True)	
				
		self.btnServerStart = gtk.Button('Start JobMonitor')
		self.btnServerStart.connect("clicked", self.onStartServer, "Start JobMonitor" )
  		oHBox1.pack_end(self.btnServerStart,False,True)  	
  		
		oVBox.pack_start(oHBox1,False,False)
		
		oHBox2 = gtk.HBox(False, 10)
		
		label = gtk.Label("Frames Processed")
		label.set_justify(gtk.JUSTIFY_LEFT)
		oHBox2.pack_start(label,False,True)		
		
  		self.progressFramesProcessed = gtk.ProgressBar()	
  		oHBox2.pack_start(self.progressFramesProcessed,True,True)		
		
		oVBox.pack_start(oHBox2,False,False)

		# Following code snippet referred from official PyGTK-Demo examples
		oScrolledWindow = gtk.ScrolledWindow()
		oScrolledWindow.set_shadow_type(gtk.SHADOW_ETCHED_IN)
		oScrolledWindow.set_policy(gtk.POLICY_NEVER, gtk.POLICY_AUTOMATIC)

		self.oModel = gtk.ListStore( gobject.TYPE_STRING, gobject.TYPE_STRING,gobject.TYPE_UINT)
				
		oTreeview = gtk.TreeView(self.oModel)
		oScrolledWindow.add(oTreeview)
		
		renderer = gtk.CellRendererToggle()
		renderer.connect('toggled', self.fixed_toggled, self.oModel)
		column = gtk.TreeViewColumn('Fixed', renderer, active=COLUMN_HOSTNAME)

		# set this column to a fixed sizing(of 50 pixels)
		column.set_sizing(gtk.TREE_VIEW_COLUMN_FIXED)
		column.set_fixed_width(200)        

		# columns for Hostname
		column = gtk.TreeViewColumn('Hostname', gtk.CellRendererText(), text=COLUMN_HOSTNAME)
		column.set_sort_column_id(COLUMN_HOSTNAME)
		oTreeview.append_column(column)        
		
		# columns for Progress
		column = gtk.TreeViewColumn('Progress', gtk.CellRendererProgress(), text=COLUMN_STATUS, value=2)
		column.set_sort_column_id(COLUMN_PROGRESS)
		oTreeview.append_column(column)   		
				
		oVBox.pack_start(oScrolledWindow,True,True)

		self.win.add(oVBox)
		
		self.win.show_all()	
		
	##
	# GetDescription 
	# @param	nFrameStatus	Status of the Frame being processed
	def GetDescription(self,nFrameStatus):
		if RenderFarm.FRAME_CLIENT_IDLE == nFrameStatus:
			return 0, 'Idle'
		if RenderFarm.FRAME_WAITING == nFrameStatus:
			return 10, 'Waiting'
		if RenderFarm.FRAME_DOWNLOADING == nFrameStatus:
			return 15, 'Downloading'
		if RenderFarm.FRAME_RENDERING == nFrameStatus:
			return 20, 'Rendering'
		if RenderFarm.FRAME_FORWARDING == nFrameStatus:
			return 90, 'Forwarding'
		if RenderFarm.FRAME_COLLECTED == nFrameStatus:
			return 75, 'Collected'		
		if RenderFarm.FRAME_SCENE_COMPLETE == nFrameStatus:
			return 100, 'Complete'	
		return 0, 'Hanged?'	
			
	##
	# UpdateClientInfo 
	# @param	oInfoCollection		Collection containing information about Client
	def UpdateClientInfo(self,oInfoCollection):
		#print oInfoCollection
		strClientName = oInfoCollection[0]
		
		iterEntry = None
		if self.dictClientInfo.has_key(strClientName):
			iterEntry = self.dictClientInfo[strClientName] 
		else:
			iterNewEntry = self.oModel.append()
			self.dictClientInfo[strClientName] = iterNewEntry
			iterEntry = iterNewEntry
			
		Percentage,Text = self.GetDescription( oInfoCollection[2] )
		
		gtk.gdk.threads_enter()
		self.oModel.set_value(iterEntry,0,oInfoCollection[0])
		self.oModel.set_value(iterEntry,1,oInfoCollection[1]+" "+Text)
		self.oModel.set_value(iterEntry,2,Percentage)	
		gtk.gdk.threads_leave()	

	# Following code snippet referred from official PyGTK-Demo examples
	def fixed_toggled(self, cell, path, model):
		# get toggled iter
		iter = self.model.get_iter((int(path),))
		fixed = self.model.get_value(iter, COLUMN_HOSTNAME)

		# do something with the value
		fixed = not fixed

		# set new value
		self.oModel.set(iter, COLUMN_HOSTNAME, fixed)
			
	##
	# UpdateProgress. This function is used as a callback to update progress of the
	# JobMonitor on the GUI.
	# @param	nProgress		Percentage of completion
	# @param	nNextFrame		Next frame index being issued
	# @param	nEndFrameIndex	Last frame index of the animation scene	
	def UpdateProgress(self,nProgress, nNextFrame, nEndFrameIndex):
		strProgressText = str(nNextFrame) + " of "+ str(nEndFrameIndex) 
		
		gtk.gdk.threads_enter()
		self.progressFramesProcessed.set_fraction(nProgress)
		self.progressFramesProcessed.set_text(strProgressText)
		gtk.gdk.threads_leave()
					
	##
	# onStartServer. This method starts the JobMonitor via the GUI.
	# @param	widget		Widget triggering this function
	# @param	data		Trigger data
	def onStartServer(self,widget, data=None):
		self.btnServerStart.set_sensitive(False)
		self.oRenderServer.AddJobMonitor(self.oJobMonitor.GetName())
		self.oFrameServer.AddJobMonitor(self.oJobMonitor.GetName())
		self.lbl_RenderServer.set_label("Render Server: "+self.oRenderServer.GetHostName())
		self.lbl_FrameServer.set_label("Frame Server: "+self.oFrameServer.GetHostName())
		self.oJobMonitor.bTestMode = self.oRenderServer.IsInTestMode()
		self.oJobMonitor.fxnProgressCallback = self.UpdateProgress
		self.oJobMonitor.fxnUpdateClientInfo = self.UpdateClientInfo
		self.oJobMonitor.ShowVitalInformation()		
		
###############################################################################
# Class : JobMonitor
# This class is responsible monitoring the status of all the servers and the clients
# in the renderfam. It receives updates from all the connected applications as the process
# progresses. It also behaves as the servant for the ORB.
###############################################################################

class JobMonitor (RenderFarm__POA.iJobMonitor):
	##
	# Constructor. Initialises the JobMonitor object.
	# @param	strName			Name for the JobMonitor
	# @param	oRenderServer	Render Server object that is controlling the renderfarm
	# @param	oFrameServer	Frame server object in the renderfarm		
	def __init__(self, strName, oRenderServer,oFrameServer):
		self.fxnProgressCallback = None
		self.fxnUpdateClientInfo = None
		self.strName  = strName
		self.oRenderServer = oRenderServer
		self.oFrameServer = oFrameServer
		self.nEndFrameIndex = -1
		self.nCompletedFrames = 0
		self.bTestMode = False

	##
	# GetName. This method returns the user specified name for the JobMonitor.  
	def GetName(self):
		return self.strName
		
	##
	# GetHostName. This method returns the hostname of the machine running the JobMonitor.  	
	def GetHostName(self):
		return socket.gethostname()		
	
	##
	# ShowVitalInformation. This functions prints associated servers participating in RenderFarm. 	
	def ShowVitalInformation(self):
		print "Job Monitor running on", self.GetName()
		print "Render Server running on", self.oRenderServer.GetHostName()
		print "Frame Server running on", self.oFrameServer.GetHostName()
		bJobPending, JobFile, nFilesize, self.strExt, nStartFrameIndex, self.nEndFrameIndex = oRenderServer.GetJobDetails(0)
			
	##
	# UpdateStatus. This method receives status updates from all the members of the renderfarm about the ongoing
	# activities. It updates the GUI accordingly. 
	# @param	nJobIndex		Index of the Job
	# @param	strClientName	Name of the render client
	# @param	nFrameIndex		Index of the Frame	
	# @param	nFrameStatus	Status of the Frame	
	def UpdateStatus(self, nJobIndex, strClientName, nFrameIndex, nFrameStatus):
		print nJobIndex, strClientName, nFrameIndex, nFrameStatus
		
		if True == self.bTestMode:
			time.sleep(2)		
				
		if  RenderFarm.FRAME_COLLECTED == nFrameStatus:
			self.nCompletedFrames = self.nCompletedFrames + 1
		
		if None != self.fxnProgressCallback:
			nProgress = float(self.nCompletedFrames) / float(self.nEndFrameIndex)
			self.fxnProgressCallback(nProgress, self.nCompletedFrames, self.nEndFrameIndex)
			
		if None != self.fxnUpdateClientInfo:
			strComment = ''
			if 0 < nFrameIndex:
				strComment = 'Frame['+str(nFrameIndex)+'] '
				
			self.fxnUpdateClientInfo((strClientName,strComment,nFrameStatus))
			
		return True

###############################################################################
# Main application thread
# This thread initialises the ORB, binds the JobMonitor object to the naming
# context and runs the GUI loop.
###############################################################################

# Initialisation parameters for OmniOrbPy
application_arguments = sys.argv[:]
#application_arguments.append("-ORBtraceLevel")
#application_arguments.append("25")
application_arguments.append("-ORBmaxGIOPVersion")
application_arguments.append("1.2")
application_arguments.append("-ORBgiopMaxMsgSize")
application_arguments.append("2097152")
application_arguments.append("-ORBstrictIIOP")
application_arguments.append("1")
application_arguments.append("-ORBInitRef")
application_arguments.append("NameService=corbaname::aldgate.tn.informatics.scitech.sussex.ac.uk:8000")
application_arguments.append("-ORBserverTransportRule") 
application_arguments.append("*   bidir,unix,tcp,ssl")
application_arguments.append("-ORBclientTransportRule") 
application_arguments.append("*   bidir,unix,tcp,ssl")
application_arguments.append("-ORBofferBiDirectionalGIOP")
application_arguments.append("1")
application_arguments.append("-ORBacceptBiDirectionalGIOP")
application_arguments.append("1")
# Must be disabled for client and enabled for server
application_arguments.append("-ORBendPoint")
application_arguments.append("giop:tcp::8002")

# Initialise the ORB and the POA
oORB = CORBA.ORB_init(application_arguments,CORBA.ORB_ID)
oPOA = oORB.resolve_initial_references("RootPOA")
oPOA._get_the_POAManager().activate()

# Find the name service
oNameRoot = oORB.resolve_initial_references("NameService")
oNameRoot = oNameRoot._narrow(CosNaming.NamingContext)
if None == oNameRoot:
	print "Did not locate a valid naming service. Please check setup."
	sys.exit(1)
	
# Bind a RenderServer object	
oRenderServer = oORB.string_to_object("corbaname:rir:#RenderServer.obj")
if None == oRenderServer:
	print "Did not locate RenderServer. Please start RenderServer before starting JobMonitor."
	sys.exit(2)
	
# Bind a FrameServer object
strFrameServerName = oRenderServer.GetNameOfAssociatedFrameServer()
if '' == strFrameServerName:
	print "Did not locate FrameServer. Please start FrameServer before starting JobMonitor."
	sys.exit(3)		
	
oFrameServer = oORB.string_to_object("corbaname:rir:#"+strFrameServerName+".obj")
if None == oFrameServer:
	print "Did not locate FrameServer. Please start FrameServer before starting JobMonitor."
	sys.exit(3)	
	
# Bind a JobMonitor object
servantJobMonitor = JobMonitor("JobMonitor",oRenderServer,oFrameServer)
oPOA.activate_object(servantJobMonitor)
objrefJobMonitor = servantJobMonitor._this()		
oName = [CosNaming.NameComponent(servantJobMonitor.GetName(),"obj")]
oNameRoot.rebind(oName,objrefJobMonitor)

#Instantiate a GUI for the JobMonitor
InstGUI = GUI_JobMonitor(oRenderServer,oFrameServer,servantJobMonitor)

#GTK loop
gtk.gdk.threads_enter()
gtk.main()	
gtk.gdk.threads_leave()

# This part of GUI loop was referred from TK based tictactoe example provided with PyOmniOrb	
print "Shut down JobMonitor"
oORB.shutdown(0)

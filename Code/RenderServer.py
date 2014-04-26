###############################################################################
#
# RenderFarm : written by Anup Jayapal Rao 
#		e-mail id:	anup.kadam <at> gmail.com, anup_kadam <at> yahoo.com, ar210 <at> sussex.ac.uk
#
#	Written for the purpose of the Distributed systems assignment at the University Of Sussex, 2008.
#
###############################################################################

##
# RenderServer 
# 
# This program creates the server which is responsible for controlling the states
# of connected clients. It shares the 3D scene file with connected clients using TFTP
# and indicates forwading of rendered frames to the FrameServer.
##

# System dependent libraries
import threading
import time
import socket
import os,os.path
import sys, logging
from optparse import OptionParser
import tftpy
from subprocess import *

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

# GLOBALS
oORB = None
tftpy.setLogLevel(logging.INFO)

###############################################################################
# Class : GUI_RenderServer
# This class is responsible for creating and handling the GUI for the 
# RenderServer.
###############################################################################

gtk.gdk.threads_init()
	
class GUI_RenderServer( ):
	
	##
	# Constructor. Creates the widgets shown in the GUI for the RenderServer.
	# @param	servantRenderServer	RenderServer object that is represented by the GUI
	def __init__( self, servantRenderServer):
		self.oRenderServer = servantRenderServer	
		self.win = gtk.Window()
		self.win.set_title('RenderServer on ' + socket.gethostname())
		self.win.connect('delete-event', gtk.main_quit)
		self.win.move(100, 100)
		#self.win.resize(800, 480)
		
		self.win.set_border_width(10)
		
		oVBox = gtk.VBox(False, 10)
		
		imageBanner = gtk.Image()
		imageBanner.set_from_file('RS.png')
		oVBox.pack_start(imageBanner,False,False)
		
		oHBox1 = gtk.HBox(False, 10)
		
		blendFilefilter = gtk.FileFilter()
		blendFilefilter.set_name('Blender3D Files')
		blendFilefilter.add_pattern('*.blend')
		
		self.filechooserbutton = gtk.FileChooserButton('Select .Blend file')
		self.filechooserbutton.add_filter(blendFilefilter)
		self.filechooserbutton.connect('selection-changed', self.onSelectFile)
  		self.filechooserbutton.set_current_folder(os.getcwd())
  		oHBox1.pack_start(self.filechooserbutton,True,True)
  		
  		self.testbutton = gtk.CheckButton('Test mode')
  		self.testbutton.set_sensitive(False)
  		oHBox1.pack_start(self.testbutton,False,True)
  		
		self.btnServerStart = gtk.Button('Start RenderServer')
		self.btnServerStart.set_sensitive(False)
		self.btnServerStart.connect("clicked", self.onStartServer, "Start RenderServer" )
  		oHBox1.pack_end(self.btnServerStart,False,True)  	
  		
		oVBox.pack_start(oHBox1,False,False)
		
		oHBox2 = gtk.HBox(False, 10)
		
		label = gtk.Label("Frame allocation")
		label.set_justify(gtk.JUSTIFY_LEFT)
		oHBox2.pack_start(label,False,True)		
		
  		self.progressFrameAllocation = gtk.ProgressBar()	
  		oHBox2.pack_start(self.progressFrameAllocation,True,True)		
		
		oVBox.pack_start(oHBox2,False,False)
		
		self.win.add(oVBox)
		
		self.win.show_all()	
		
	##
	# onSelectFile. This function invokes a filechooserbutton dialog to select
	# the 3D scene file containing the animation. 
	# @param	widget		Widget triggering this function
	# @param	data		Trigger data
	def onSelectFile(self, widget, data=None):
		# Following code snippet referred from http://www.mail-archive.com/pygtk@daa.com.au/msg14611.html
		filename = self.filechooserbutton.get_filename()
		if None != filename:
			self.filechooserbutton.set_sensitive(False)
			self.btnServerStart.set_sensitive(True)
			self.testbutton.set_sensitive(True)
			self.filename = filename
	
	##
	# UpdateProgress. This function is used as a callback to update progress of the
	# renderfarm on the GUI.
	# @param	nProgress		Percentage of completion
	# @param	nNextFrame		Next frame index being issued
	# @param	nEndFrameIndex	Last frame index of the animation scene		
	def UpdateProgress(self,nProgress, nNextFrame, nEndFrameIndex):
		strProgressText = str(nNextFrame) + " of "+ str(nEndFrameIndex) 
		
		gtk.gdk.threads_enter()
		self.progressFrameAllocation.set_fraction(nProgress)
		self.progressFrameAllocation.set_text(strProgressText)
		gtk.gdk.threads_leave()
			
	##
	# onStartServer. This method starts the RenderServer via the GUI. 
	# @param	widget		Widget triggering this function
	# @param	data		Trigger data
	def onStartServer(self,widget, data=None):
		self.btnServerStart.set_sensitive(False)	
		self.testbutton.set_sensitive(False)	
		bTestMode = self.testbutton.get_active()
		self.oRenderServer.StartRenderServer(self.filename,self.UpdateProgress,bTestMode)
		
###############################################################################
# Class : FTPServerThread
# This class is responsible for the TFTP thread which serves the folder with
# the 3D Scene file. It can only allow download to a client. Upload from a 
# client is not provided due to library limitations.
###############################################################################

FTPServerPort = 8009

class FTPServerThread ( threading.Thread ):
	
	##
	# Constructor. Initialises a thread object to function serve FTP requests. 
	# @param	oRenderServer		RenderServer object that is controlling the renderfarm	
	# @param	abspathJobFolder	The absolute path to the folder containing the 3D scene
	def __init__( self, oRenderServer, abspathJobFolder):
		global FTPServerPort		
		threading.Thread.__init__(self)
		self.oRenderServer = oRenderServer
		self.FTPServerPort = FTPServerPort

		ServerHostname =  oRenderServer.GetHostName()
		self.ServerIPaddress = socket.gethostbyname(ServerHostname)		
		
		self.abspathJobFolder = abspathJobFolder

	##
	# GetFTPserverIPaddress. This method returns the IP address of the machine running
	# the FTP server.
	def GetFTPserverIPaddress(self):
		return self.ServerIPaddress	
		
	##
	# GetFTPserverPort. This method returns the Port of the machine running
	# the FTP server.
	def GetFTPserverPort(self):
		return self.FTPServerPort

	##
	# run. This method is used to run the thread.
	def run ( self ):	
		# Start tftp server
		# tftp code referenced from tftpy examples
		print "TFTP Server is negotiating FTP from "+ self.abspathJobFolder
		server = tftpy.TftpServer(self.abspathJobFolder)
		try:
			server.listen(self.ServerIPaddress, self.FTPServerPort)
		except KeyboardInterrupt:
			pass
		
###############################################################################
# Class : RenderServer
# This class implements the RenderServer which also behaves as a servant for
# the ORB. It guides the render clients through the entire process of rendering
# an animation.
###############################################################################

class RenderServer (RenderFarm__POA.iRenderServer):
	
	##
	# Constructor. Initialises the RenderServer object.
	# @param	strName		Name for the RenderServer		
	def __init__(self,strName):
		self.strName  = strName
		self.fxnProgressCallback = None
		self.JobFolder = None #os.path.join(os.getcwd(),'JobFolder')
		self.JobFile = '' #'DSTrial01.blend'					
		self.oJobMonitor  = None
		self.strFrameServerName = ''
		self.strSceneName = None
		self.nStartFrameIndex = 1
		self.nEndFrameIndex = 1 
		self.strExt = 'PNG'
		self.nNextFrame  = self.nStartFrameIndex
		self.nFilesize = 0
		self.bTestMode = False
		self.setInitialBucket = None
		self.setCompleted = None	
	
	##
	# StartRenderServer. This method starts the render server. 
	# @param	filename				BLEND file containing the 3D scene animation	
	# @param	fxnProgressCallback		Callback function for GUI which reports progress
	# @param	bTestMode				Flag that indicates that test mode is in progress
	def StartRenderServer(self,filename, fxnProgressCallback,bTestMode):
		self.JobFolder = os.path.dirname(filename)
		self.JobFile = os.path.basename(filename)		
		self.fxnProgressCallback = fxnProgressCallback
		self.nFilesize = os.path.getsize(filename)
		self.strSceneName = 'Scene'
		self.nStartFrameIndex = 1
		self.nEndFrameIndex = 50 #10 #250
		self.strExt = 'PNG'		
		self.bTestMode = bTestMode
		
		self.setInitialBucket = set(range(self.nStartFrameIndex ,self.nEndFrameIndex+1))
		self.setCompleted = set()

		self.InstFTPServerThread = FTPServerThread(self, self.JobFolder)
		self.InstFTPServerThread.start()		
		
	##
	# GetName. This method returns the user specified name for the RenderServer. 		
	def GetName(self):
		return self.strName
		
	##
	# GetHostName. This method returns the hostname of the machine running the RenderServer. 		
	def GetHostName(self):
		return socket.gethostname()
		
	##
	# GetHostIPaddress. This method returns the IPaddress of the machine running the RenderServer. 
	def GetHostIPaddress(self):
		return self.InstFTPServerThread.GetFTPserverIPaddress()
	
	##
	# GetFTPServerPort. This method returns the Port of the machine running the FTP server. 	
	def GetFTPServerPort(self):
		return self.InstFTPServerThread.GetFTPserverPort()
		
	##
	# AddFrameServer. This method associates the specified FrameServer to the RenderServer. 
	# @param	strFrameServerName	Name of the frame server
	def AddFrameServer(self, strFrameServerName):
		self.strFrameServerName = strFrameServerName
			
	##
	# GetNameOfAssociatedFrameServer. This method returns the name of the associated FrameServer. 	
	def GetNameOfAssociatedFrameServer(self):
		return self.strFrameServerName 
			
	##
	# AddJobMonitor. This method adds a Job Monitor to the RenderServer.
	# @param	strJobMonitorName	Name of the Job Monitor
	def AddJobMonitor(self, strJobMonitorName):
		
		oJobMonitor = oORB.string_to_object("corbaname:rir:#"+strJobMonitorName+".obj")
		if None == oJobMonitor:
			print "Did not locate JobMonitor with name:",strJobMonitorName
			return False
	
		self.oJobMonitor = oJobMonitor
		return True
			
	##
	# GetJobDetails. This method gets the details of the job being executed by the RenderServer. 	
	# @param	nJobIndex	Index of the Job	
	def GetJobDetails(self, nJobIndex):
		if '' == self.JobFile:
			return False,'',-1,'',-1,-1
		return True,self.JobFile, self.nFilesize, self.strExt, self.nStartFrameIndex, self.nEndFrameIndex
	
	##
	# GetFrameFromBucket. This method gets a frame index from the bucket of unrendered frames. 
	def GetFrameFromBucket(self):
		# 		nFrameIndex = RenderFarm.RENDERTASK_COMPLETE # when dealing with jobs		
		nFrameIndex = RenderFarm.RENDERJOB_COMPLETE
		
		if len(self.setInitialBucket) > 0 :
			nFrameIndex = self.setInitialBucket.pop()
			self.setInitialBucket.add(nFrameIndex)
			#print self.setInitialBucket
		
		return nFrameIndex
		
	##
	# SetFrameAsComplete. This method flags a frame index as complete. 
	# @param	nFrameIndex	Index of the Frame
	def SetFrameAsComplete(self,nFrameIndex):
		self.setCompleted.add(nFrameIndex)
		self.setInitialBucket = self.setInitialBucket - self.setCompleted
		
	##
	# GetFrameToRender. This method returns a frameindex to the RenderClient for the scene being rendered. 
	# @param	strClientName	Name of the render client
	# @param	strSceneName	Name of the scene being rendered
	def GetFrameToRender(self, strClientName,strSceneName):
	
		# Simple allocation 
		#nFrameIndex = RenderFarm.RENDERJOB_COMPLETE # when dealing with jobs
		#if self.nEndFrameIndex+1 > self.nNextFrame:
		#	nFrameIndex = self.nNextFrame 		
		#	self.nNextFrame  = self.nNextFrame  + 1
		#	print "Allocating "+strClientName+" Scene:"+self.strSceneName+" Frame:"+str(nFrameIndex)
		#	if None != self.fxnProgressCallback:
		#		nProgress = float(nFrameIndex) / float(self.nEndFrameIndex)
		#		self.fxnProgressCallback(nProgress, nFrameIndex, self.nEndFrameIndex)
		
		# Set based allocation with retries
		nFrameIndex = self.GetFrameFromBucket()
		if nFrameIndex > 0:
			print "Allocating "+strClientName+" Scene:"+self.strSceneName+" Frame:"+str(nFrameIndex)
			if None != self.fxnProgressCallback:
					nProgress = float(nFrameIndex) / float(self.nEndFrameIndex)
					self.fxnProgressCallback(nProgress, nFrameIndex, self.nEndFrameIndex)
			
		return nFrameIndex,self.strSceneName
		
	##
	# GetNextState. Obtains the next state of the RenderClient as guided by the RenderServer. 
	# @param	strClientName	Name of the render client
	# @param	nClientStatus	Status of client	
	def GetNextState(self, strClientName, nClientStatus):
		
		if True == self.bTestMode:
			time.sleep(2)
		
		if RenderFarm.TC_UNINITIALISED == nClientStatus:
			return RenderFarm.TC_WAITING
			
		if RenderFarm.TC_PREPARED == nClientStatus:
			return RenderFarm.TC_PREPARED
		
		return RenderFarm.TC_UNINITIALISED
		
	##
	# IsInTestMode. This method enforces the system in to a test mode adding predefined delays
	# to the process.
	def IsInTestMode(self):
		return self.bTestMode

###############################################################################
# Main application thread
# This thread initialises the ORB, binds the RenderServer object to the naming
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
application_arguments.append("NameService=corbaname::"+socket.gethostname()+".tn.informatics.scitech.sussex.ac.uk:8000")
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
application_arguments.append("giop:tcp::8001")

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
servantRenderServer = RenderServer("RenderServer")
oPOA.activate_object(servantRenderServer)
objrefRenderServer = servantRenderServer._this()	
oName = [CosNaming.NameComponent(servantRenderServer.GetName(),"obj")]
oNameRoot.rebind(oName,objrefRenderServer)

#Instantiate a GUI for the RenderServer
InstGUI = GUI_RenderServer(servantRenderServer)

#GTK loop
gtk.gdk.threads_enter()
gtk.main()	
gtk.gdk.threads_leave()

# This part of GUI loop was referred from TK based tictactoe example provided with PyOmniOrb	
print "Shut down Renderserver"
oORB.shutdown(0)

